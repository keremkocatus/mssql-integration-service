using System.Data;
using System.Diagnostics;
using System.Text;
using Microsoft.Data.SqlClient;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;

namespace MssqlIntegrationService.Infrastructure.Services;

public class DataSyncService : IDataSyncService
{
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
            // ===== STEP 1: Read data from source =====
            var dataTable = new DataTable();
            List<DataColumn> columns;

            await using (var sourceConnection = new SqlConnection(sourceConnectionString))
            {
                await sourceConnection.OpenAsync(cancellationToken);

                await using var sourceCommand = new SqlCommand(sourceQuery, sourceConnection)
                {
                    CommandTimeout = options.Timeout
                };

                await using var reader = await sourceCommand.ExecuteReaderAsync(cancellationToken);

                // Get schema
                var schemaTable = await reader.GetSchemaTableAsync(cancellationToken);
                columns = new List<DataColumn>();

                if (schemaTable != null)
                {
                    foreach (DataRow row in schemaTable.Rows)
                    {
                        var columnName = row["ColumnName"].ToString()!;
                        var dataType = (Type)row["DataType"];
                        
                        // Handle nullable types
                        if (dataType.IsValueType)
                        {
                            dataTable.Columns.Add(new DataColumn(columnName, dataType) { AllowDBNull = true });
                        }
                        else
                        {
                            dataTable.Columns.Add(new DataColumn(columnName, dataType));
                        }
                        columns.Add(new DataColumn(columnName, dataType));
                    }
                }

                // Read data
                while (await reader.ReadAsync(cancellationToken))
                {
                    var row = dataTable.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[i] = reader.GetValue(i);
                    }
                    dataTable.Rows.Add(row);
                    result.TotalRowsRead++;
                }
            }

            if (result.TotalRowsRead == 0)
            {
                result.Warnings.Add("No rows read from source query");
                stopwatch.Stop();
                result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;
                return Result<SyncResult>.Success(result);
            }

            // Validate key columns exist
            foreach (var keyCol in keyColumns)
            {
                var targetKeyCol = options.ColumnMappings?.GetValueOrDefault(keyCol) ?? keyCol;
                if (!dataTable.Columns.Contains(keyCol))
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

                // ===== STEP 3: Bulk insert into temp table =====
                using (var bulkCopy = new SqlBulkCopy(targetConnection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = tempTableName;
                    bulkCopy.BatchSize = options.BatchSize;
                    bulkCopy.BulkCopyTimeout = options.Timeout;

                    // Column mappings
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        var targetCol = options.ColumnMappings?.GetValueOrDefault(col.ColumnName) ?? col.ColumnName;
                        bulkCopy.ColumnMappings.Add(col.ColumnName, targetCol);
                    }

                    await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);
                }
                result.Warnings.Add($"Inserted {result.TotalRowsRead} rows into temp table");

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
                var targetColumns = dataTable.Columns.Cast<DataColumn>()
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
            var sqlType = GetSqlType(col.DataType);
            columnDefs.Add($"    [{targetColName}] {sqlType} NULL");
        }

        sb.AppendLine(string.Join(",\n", columnDefs));
        sb.AppendLine(")");

        return sb.ToString();
    }

    private static string GetSqlType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        
        return underlyingType.Name switch
        {
            "String" => "NVARCHAR(MAX)",
            "Int32" => "INT",
            "Int64" => "BIGINT",
            "Int16" => "SMALLINT",
            "Byte" => "TINYINT",
            "Boolean" => "BIT",
            "DateTime" => "DATETIME2",
            "DateTimeOffset" => "DATETIMEOFFSET",
            "DateOnly" => "DATE",
            "TimeOnly" => "TIME",
            "TimeSpan" => "TIME",
            "Decimal" => "DECIMAL(18,4)",
            "Double" => "FLOAT",
            "Single" => "REAL",
            "Guid" => "UNIQUEIDENTIFIER",
            "Byte[]" => "VARBINARY(MAX)",
            _ => "NVARCHAR(MAX)"
        };
    }
}
