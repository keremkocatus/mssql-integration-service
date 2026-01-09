using System.Data;
using System.Data.Common;
using System.Diagnostics;
using System.Text;
using Microsoft.Data.SqlClient;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;
using MssqlIntegrationService.Infrastructure.Data;

namespace MssqlIntegrationService.Infrastructure.Services;

public class DataSyncService : IDataSyncService
{
    private readonly ISchemaService _schemaService;

    public DataSyncService(ISchemaService schemaService)
    {
        _schemaService = schemaService;
    }

    /// <summary>
    /// Memory-efficient data synchronization using streaming.
    /// Data is read from source and streamed directly to temp table without loading entire dataset into RAM.
    /// </summary>
    public async Task<Result<SyncResult>> SyncDataAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string sourceQuery,
        string targetTable,
        List<string> keyColumns,
        SyncOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new SyncOptions();
        var stopwatch = Stopwatch.StartNew();
        var result = new SyncResult
        {
            SourceQuery = sourceQuery,
            TargetTable = targetTable,
            KeyColumns = keyColumns
        };

        // Generate unique temp table name
        var tempTableName = $"#Temp_Sync_{Guid.NewGuid():N}";

        try
        {
            // ===== STEP 1: Open source connection and get streaming reader =====
            await using var sourceConnection = new SqlConnection(sourceConnectionString);
            await sourceConnection.OpenAsync(cancellationToken);

            await using var sourceCommand = new SqlCommand(sourceQuery, sourceConnection)
            {
                CommandTimeout = options.Timeout
            };

            // Use SequentialAccess for better memory efficiency
            await using var reader = await sourceCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

            // Get schema
            var schemaTable = await reader.GetSchemaTableAsync(cancellationToken);
            var columns = new List<DataColumn>();

            if (schemaTable != null)
            {
                foreach (DataRow row in schemaTable.Rows)
                {
                    var columnName = row["ColumnName"].ToString()!;
                    var dataType = (Type)row["DataType"];
                    columns.Add(new DataColumn(columnName, dataType));
                }
            }

            if (columns.Count == 0)
            {
                return Result<SyncResult>.Failure("No columns found in source query result");
            }

            // Validate key columns exist
            foreach (var keyCol in keyColumns)
            {
                var colExists = columns.Any(c => c.ColumnName.Equals(keyCol, StringComparison.OrdinalIgnoreCase));
                if (!colExists)
                {
                    return Result<SyncResult>.Failure($"Key column '{keyCol}' not found in source query result");
                }
            }

            // ===== STEP 2-6: Operations on target database =====
            await using var targetConnection = new SqlConnection(targetConnectionString);
            await targetConnection.OpenAsync(cancellationToken);

            SqlTransaction? transaction = null;
            if (options.UseTransaction)
            {
                transaction = targetConnection.BeginTransaction();
            }

            try
            {
                // ===== STEP 2: Create temp table =====
                var createTempTableSql = GenerateCreateTempTableSql(tempTableName, columns, options.ColumnMappings);
                await using (var createCmd = new SqlCommand(createTempTableSql, targetConnection, transaction))
                {
                    createCmd.CommandTimeout = options.Timeout;
                    await createCmd.ExecuteNonQueryAsync(cancellationToken);
                }
                result.Warnings.Add($"Created temp table: {tempTableName}");

                // ===== STEP 2.5: Copy indexes from target table to temp table =====
                var indexesCreated = await CopyIndexesToTempTableAsync(
                    targetConnectionString,
                    targetConnection, 
                    transaction, 
                    targetTable, 
                    tempTableName, 
                    columns.Select(c => options.ColumnMappings?.GetValueOrDefault(c.ColumnName) ?? c.ColumnName).ToList(),
                    options.Timeout, 
                    cancellationToken);
                if (indexesCreated > 0)
                {
                    result.Warnings.Add($"Created {indexesCreated} index(es) on temp table");
                }

                // ===== STEP 3: Stream data directly from source to temp table =====
                // Use RowCountingDataReader to track rows while streaming
                using var countingReader = new RowCountingDataReader(reader);

                using (var bulkCopy = new SqlBulkCopy(targetConnection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.BatchSize = options.BatchSize;
                    bulkCopy.BulkCopyTimeout = options.Timeout;
                    bulkCopy.EnableStreaming = true;

                    // Column mappings
                    foreach (var col in columns)
                    {
                        var targetCol = options.ColumnMappings?.GetValueOrDefault(col.ColumnName) ?? col.ColumnName;
                        bulkCopy.ColumnMappings.Add(col.ColumnName, targetCol);
                    }

                    // Stream directly from source to temp table - NO DataTable in memory!
                    await bulkCopy.WriteToServerAsync(countingReader, cancellationToken);
                    result.TotalRowsRead = (int)countingReader.RowCount;
                }
                result.Warnings.Add($"Streamed {result.TotalRowsRead} rows into temp table");

                if (result.TotalRowsRead == 0)
                {
                    result.Warnings.Add("No rows read from source query");
                    // Clean up temp table
                    await using (var dropCmd = new SqlCommand($"DROP TABLE {tempTableName}", targetConnection, transaction))
                    {
                        await dropCmd.ExecuteNonQueryAsync(cancellationToken);
                    }
                    if (transaction != null)
                    {
                        await transaction.CommitAsync(cancellationToken);
                    }
                    stopwatch.Stop();
                    result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;
                    return Result<SyncResult>.Success(result);
                }

                // ===== STEP 4: Delete matching rows from target =====
                var safeTargetTable = SqlValidator.SafeTableName(targetTable);
                string deleteSql;
                if (options.DeleteAllBeforeInsert)
                {
                    deleteSql = $"DELETE FROM {safeTargetTable}";
                }
                else
                {
                    var joinConditions = keyColumns.Select(k =>
                    {
                        var targetCol = options.ColumnMappings?.GetValueOrDefault(k) ?? k;
                        var safeCol = SqlValidator.SafeIdentifier(targetCol);
                        return $"t.{safeCol} = s.{safeCol}";
                    });
                    
                    deleteSql = $@"
                        DELETE t 
                        FROM {safeTargetTable} t
                        INNER JOIN {tempTableName} s ON {string.Join(" AND ", joinConditions)}";
                }

                await using (var deleteCmd = new SqlCommand(deleteSql, targetConnection, transaction))
                {
                    deleteCmd.CommandTimeout = options.Timeout;
                    result.RowsDeleted = await deleteCmd.ExecuteNonQueryAsync(cancellationToken);
                }
                result.Warnings.Add($"Deleted {result.RowsDeleted} rows from target table");

                // ===== STEP 5: Insert from temp to target =====
                var targetColumns = columns
                    .Select(c => options.ColumnMappings?.GetValueOrDefault(c.ColumnName) ?? c.ColumnName)
                    .ToList();

                var safeColumns = targetColumns.Select(SqlValidator.SafeIdentifier).ToList();
                var insertSql = $@"
                    INSERT INTO {safeTargetTable} ({string.Join(", ", safeColumns)})
                    SELECT {string.Join(", ", safeColumns)} FROM {tempTableName}";

                await using (var insertCmd = new SqlCommand(insertSql, targetConnection, transaction))
                {
                    insertCmd.CommandTimeout = options.Timeout;
                    result.RowsInserted = await insertCmd.ExecuteNonQueryAsync(cancellationToken);
                }
                result.Warnings.Add($"Inserted {result.RowsInserted} rows into target table");

                // ===== STEP 6: Drop temp table =====
                await using (var dropCmd = new SqlCommand($"DROP TABLE {tempTableName}", targetConnection, transaction))
                {
                    await dropCmd.ExecuteNonQueryAsync(cancellationToken);
                }
                result.Warnings.Add($"Dropped temp table: {tempTableName}");

                // Commit transaction
                if (transaction != null)
                {
                    await transaction.CommitAsync(cancellationToken);
                }

                stopwatch.Stop();
                result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;

                return Result<SyncResult>.Success(result);
            }
            catch
            {
                if (transaction != null)
                {
                    await transaction.RollbackAsync(cancellationToken);
                }
                throw;
            }
        }
        catch (SqlException ex)
        {
            stopwatch.Stop();
            return Result<SyncResult>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return Result<SyncResult>.Failure(ex.Message);
        }
    }

    private static string GenerateCreateTempTableSql(string tempTableName, List<DataColumn> columns, Dictionary<string, string>? columnMappings)
    {
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE {tempTableName} (");

        var columnDefs = new List<string>();
        foreach (var col in columns)
        {
            var targetColName = columnMappings?.GetValueOrDefault(col.ColumnName) ?? col.ColumnName;
            var sqlType = GetSqlTypeWithMetadata(col);
            columnDefs.Add($"    [{targetColName}] {sqlType} NULL");
        }

        sb.AppendLine(string.Join(",\n", columnDefs));
        sb.AppendLine(")");

        return sb.ToString();
    }

    /// <summary>
    /// Gets SQL type with metadata from DataColumn (MaxLength for strings).
    /// </summary>
    private static string GetSqlTypeWithMetadata(DataColumn col)
    {
        var type = Nullable.GetUnderlyingType(col.DataType) ?? col.DataType;
        
        return type.Name switch
        {
            "String" when col.MaxLength > 0 && col.MaxLength < 4000 => $"NVARCHAR({col.MaxLength})",
            "String" => "NVARCHAR(MAX)",
            "Int32" => "INT",
            "Int64" => "BIGINT",
            "Int16" => "SMALLINT",
            "Byte" => "TINYINT",
            "Boolean" => "BIT",
            "DateTime" => "DATETIME", // Default to DATETIME for compatibility
            "DateTimeOffset" => "DATETIMEOFFSET",
            "DateOnly" => "DATE",
            "TimeOnly" => "TIME",
            "TimeSpan" => "TIME",
            "Decimal" => "DECIMAL(38,18)", // Max precision for temp table to avoid truncation
            "Double" => "FLOAT",
            "Single" => "REAL",
            "Guid" => "UNIQUEIDENTIFIER",
            "Byte[]" => "VARBINARY(MAX)",
            _ => "NVARCHAR(MAX)"
        };
    }

    /// <summary>
    /// Copies indexes from target table to temp table using ISchemaService.
    /// Priority: 1. Primary Key (as non-clustered), 2. Unique indexes, 3. Non-unique indexes
    /// Only copies indexes where all columns exist in temp table.
    /// </summary>
    private async Task<int> CopyIndexesToTempTableAsync(
        string targetConnectionString,
        SqlConnection connection,
        SqlTransaction? transaction,
        string targetTable,
        string tempTableName,
        List<string> tempTableColumns,
        int timeout,
        CancellationToken cancellationToken)
    {
        // Get indexes from schema service
        var indexesResult = await _schemaService.GetIndexesAsync(targetConnectionString, targetTable, cancellationToken);
        
        if (!indexesResult.IsSuccess || indexesResult.Data == null || indexesResult.Data.Count == 0)
        {
            return 0; // No indexes to copy
        }

        var indexes = indexesResult.Data;
        var indexesCreated = 0;
        var tempColumnSet = new HashSet<string>(tempTableColumns, StringComparer.OrdinalIgnoreCase);

        foreach (var index in indexes)
        {
            // Check if all index columns exist in temp table
            if (!index.Columns.All(c => tempColumnSet.Contains(c)))
            {
                continue; // Skip indexes with columns not in temp table
            }

            // Build column list with sort order
            var columnDefs = new List<string>();
            for (int i = 0; i < index.Columns.Count; i++)
            {
                var safeCol = SqlValidator.SafeIdentifier(index.Columns[i]);
                var order = i < index.ColumnOrders.Count ? index.ColumnOrders[i] : "ASC";
                columnDefs.Add($"{safeCol} {order}");
            }

            // Generate unique index name for temp table
            var tempIndexName = $"IX_Temp_{Guid.NewGuid():N}";

            // Build CREATE INDEX statement
            // Note: For temp tables, we create non-clustered indexes only
            var uniqueKeyword = index.IsUnique ? "UNIQUE " : "";
            var createIndexSql = $@"
                CREATE {uniqueKeyword}NONCLUSTERED INDEX [{tempIndexName}]
                ON {tempTableName} ({string.Join(", ", columnDefs)})";

            try
            {
                await using var createCmd = new SqlCommand(createIndexSql, connection, transaction);
                createCmd.CommandTimeout = timeout;
                await createCmd.ExecuteNonQueryAsync(cancellationToken);
                indexesCreated++;
            }
            catch
            {
                // Ignore index creation failures (e.g., duplicate key issues)
                // The sync will still work, just slower
            }
        }

        return indexesCreated;
    }
}
