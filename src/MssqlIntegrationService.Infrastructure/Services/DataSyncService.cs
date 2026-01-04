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
    /// Copies indexes from target table to temp table.
    /// Priority: 1. Primary Key (as non-clustered), 2. Unique indexes, 3. Non-unique indexes
    /// Only copies indexes where all columns exist in temp table.
    /// </summary>
    private static async Task<int> CopyIndexesToTempTableAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        string targetTable,
        string tempTableName,
        List<string> tempTableColumns,
        int timeout,
        CancellationToken cancellationToken)
    {
        // Parse table name (handle schema.table format)
        var tableParts = targetTable.Split('.');
        var schemaName = tableParts.Length > 1 ? tableParts[0].Trim('[', ']') : "dbo";
        var tableName = tableParts.Length > 1 ? tableParts[1].Trim('[', ']') : tableParts[0].Trim('[', ']');

        // Query to get indexes with their columns, ordered by priority
        // Priority: PK first, then unique indexes, then non-unique
        var indexQuery = @"
            SELECT 
                i.name AS IndexName,
                i.is_primary_key AS IsPrimaryKey,
                i.is_unique AS IsUnique,
                i.type_desc AS IndexType,
                STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS Columns,
                STRING_AGG(
                    CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END, 
                    ','
                ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS ColumnOrders
            FROM sys.indexes i
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
            INNER JOIN sys.tables t ON i.object_id = t.object_id
            INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
            WHERE s.name = @schemaName 
                AND t.name = @tableName
                AND i.type IN (1, 2) -- Clustered and Non-clustered
                AND ic.is_included_column = 0 -- Only key columns, not included columns
            GROUP BY i.name, i.is_primary_key, i.is_unique, i.type_desc, i.index_id
            ORDER BY 
                i.is_primary_key DESC,  -- Primary key first
                i.is_unique DESC,       -- Then unique indexes
                i.index_id ASC          -- Then by index order";

        var indexes = new List<(string Name, bool IsPK, bool IsUnique, string[] Columns, string[] Orders)>();

        await using (var cmd = new SqlCommand(indexQuery, connection, transaction))
        {
            cmd.Parameters.AddWithValue("@schemaName", schemaName);
            cmd.Parameters.AddWithValue("@tableName", tableName);
            cmd.CommandTimeout = timeout;

            await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                var indexName = reader.GetString(0);
                var isPK = reader.GetBoolean(1);
                var isUnique = reader.GetBoolean(2);
                var columns = reader.GetString(4).Split(',');
                var orders = reader.GetString(5).Split(',');

                indexes.Add((indexName, isPK, isUnique, columns, orders));
            }
        }

        // Create indexes on temp table
        var indexesCreated = 0;
        var tempColumnSet = new HashSet<string>(tempTableColumns, StringComparer.OrdinalIgnoreCase);

        foreach (var (indexName, isPK, isUnique, columns, orders) in indexes)
        {
            // Check if all index columns exist in temp table
            if (!columns.All(c => tempColumnSet.Contains(c)))
            {
                continue; // Skip indexes with columns not in temp table
            }

            // Build column list with sort order
            var columnDefs = new List<string>();
            for (int i = 0; i < columns.Length; i++)
            {
                var safeCol = SqlValidator.SafeIdentifier(columns[i]);
                var order = i < orders.Length ? orders[i] : "ASC";
                columnDefs.Add($"{safeCol} {order}");
            }

            // Generate unique index name for temp table
            var tempIndexName = $"IX_Temp_{Guid.NewGuid():N}";

            // Build CREATE INDEX statement
            // Note: For temp tables, we create non-clustered indexes only
            var uniqueKeyword = isUnique ? "UNIQUE " : "";
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
