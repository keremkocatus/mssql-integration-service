using System.Data;
using System.Data.Common;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;
using MssqlIntegrationService.Infrastructure.Data;

namespace MssqlIntegrationService.Infrastructure.Services;

public class DataTransferService : IDataTransferService
{
    /// <summary>
    /// Memory-efficient data transfer using streaming.
    /// Data is read directly from source and written to target without loading entire dataset into RAM.
    /// Uses SqlBulkCopy's native IDataReader support for optimal memory usage.
    /// </summary>
    public async Task<Result<TransferResult>> TransferDataAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string sourceQuery,
        string targetTable,
        TransferOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new TransferOptions();
        var stopwatch = Stopwatch.StartNew();
        var result = new TransferResult
        {
            SourceQuery = sourceQuery,
            TargetTable = targetTable
        };

        try
        {
            // Open source connection and get reader (streaming mode)
            await using var sourceConnection = new SqlConnection(sourceConnectionString);
            await sourceConnection.OpenAsync(cancellationToken);

            await using var sourceCommand = new SqlCommand(sourceQuery, sourceConnection)
            {
                CommandTimeout = options.Timeout
            };

            // Use SequentialAccess for better memory efficiency with large columns
            await using var reader = await sourceCommand.ExecuteReaderAsync(CommandBehavior.SequentialAccess, cancellationToken);

            // Get schema information for table creation if needed
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

            // Open target connection
            await using var targetConnection = new SqlConnection(targetConnectionString);
            await targetConnection.OpenAsync(cancellationToken);

            SqlTransaction? transaction = null;
            if (options.UseTransaction)
            {
                transaction = targetConnection.BeginTransaction();
            }

            try
            {
                // Truncate if requested
                if (options.TruncateTargetTable)
                {
                    var safeTableName = SqlValidator.SafeTableName(targetTable);
                    await using var truncateCommand = new SqlCommand($"TRUNCATE TABLE {safeTableName}", targetConnection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync(cancellationToken);
                    result.Warnings.Add($"Table {targetTable} was truncated before insert");
                }

                // Create table if requested
                if (options.CreateTableIfNotExists)
                {
                    var safeTableForCreate = SqlValidator.SafeTableName(targetTable);
                    var createTableSql = GenerateCreateTableSql(safeTableForCreate, columns, schemaTable);
                    await using var checkCommand = new SqlCommand(
                        $"IF OBJECT_ID('{safeTableForCreate}', 'U') IS NULL BEGIN {createTableSql} END",
                        targetConnection, transaction);
                    await checkCommand.ExecuteNonQueryAsync(cancellationToken);
                }

                // Create counting wrapper to track rows while streaming
                using var countingReader = new RowCountingDataReader(reader);

                // Bulk copy directly from reader - NO DataTable in memory!
                // SqlBulkCopy will read in batches internally based on BatchSize
                var destTableName = SqlValidator.SafeTableName(targetTable);
                using var bulkCopy = new SqlBulkCopy(targetConnection, SqlBulkCopyOptions.Default, transaction)
                {
                    DestinationTableName = destTableName,
                    BatchSize = options.BatchSize,
                    BulkCopyTimeout = options.Timeout,
                    EnableStreaming = true // Enable streaming for better memory efficiency
                };

                // Column mappings
                if (options.ColumnMappings != null && options.ColumnMappings.Count > 0)
                {
                    foreach (var mapping in options.ColumnMappings)
                    {
                        bulkCopy.ColumnMappings.Add(mapping.Key, mapping.Value);
                    }
                }
                else
                {
                    foreach (var col in columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                }

                // Stream data directly from source to target
                await bulkCopy.WriteToServerAsync(countingReader, cancellationToken);
                
                result.TotalRowsRead = (int)countingReader.RowCount;
                result.TotalRowsWritten = (int)countingReader.RowCount;

                if (transaction != null)
                {
                    await transaction.CommitAsync(cancellationToken);
                }
            }
            catch
            {
                if (transaction != null)
                {
                    await transaction.RollbackAsync(cancellationToken);
                }
                throw;
            }

            stopwatch.Stop();
            result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;

            return Result<TransferResult>.Success(result);
        }
        catch (SqlException ex)
        {
            stopwatch.Stop();
            return Result<TransferResult>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return Result<TransferResult>.Failure(ex.Message);
        }
    }

    /// <summary>
    /// Memory-efficient bulk insert using streaming.
    /// Uses ObjectDataReader to stream data directly without loading entire dataset into a DataTable.
    /// </summary>
    public async Task<Result<TransferResult>> BulkInsertAsync(
        string connectionString,
        string tableName,
        IEnumerable<IDictionary<string, object?>> data,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new BulkInsertOptions();
        var stopwatch = Stopwatch.StartNew();
        var result = new TransferResult
        {
            TargetTable = tableName
        };

        try
        {
            // Peek at first item to get column names without materializing entire collection
            using var enumerator = data.GetEnumerator();
            if (!enumerator.MoveNext())
            {
                // Empty data
                return Result<TransferResult>.Success(result);
            }

            var firstRow = enumerator.Current;
            var columns = firstRow.Keys.ToList();

            // Create a streaming enumerable that yields first row + remaining rows
            IEnumerable<IDictionary<string, object?>> StreamData()
            {
                yield return firstRow;
                while (enumerator.MoveNext())
                {
                    yield return enumerator.Current;
                }
            }

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            SqlTransaction? transaction = null;
            if (options.UseTransaction)
            {
                transaction = connection.BeginTransaction();
            }

            try
            {
                // Use ObjectDataReader for memory-efficient streaming
                using var objectReader = new ObjectDataReader<IDictionary<string, object?>>(StreamData(), columns);

                var safeTableName = SqlValidator.SafeTableName(tableName);
                using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
                {
                    DestinationTableName = safeTableName,
                    BatchSize = options.BatchSize,
                    BulkCopyTimeout = options.Timeout,
                    EnableStreaming = true
                };

                // Column mappings
                foreach (var col in columns)
                {
                    bulkCopy.ColumnMappings.Add(col, col);
                }

                await bulkCopy.WriteToServerAsync(objectReader, cancellationToken);
                
                result.TotalRowsRead = (int)objectReader.RowCount;
                result.TotalRowsWritten = (int)objectReader.RowCount;

                if (transaction != null)
                {
                    await transaction.CommitAsync(cancellationToken);
                }
            }
            catch
            {
                if (transaction != null)
                {
                    await transaction.RollbackAsync(cancellationToken);
                }
                throw;
            }

            stopwatch.Stop();
            result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;

            return Result<TransferResult>.Success(result);
        }
        catch (SqlException ex)
        {
            stopwatch.Stop();
            return Result<TransferResult>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return Result<TransferResult>.Failure(ex.Message);
        }
    }

    private static string GenerateCreateTableSql(string tableName, List<DataColumn> columns, DataTable? schemaTable)
    {
        var columnDefs = new List<string>();

        for (int i = 0; i < columns.Count; i++)
        {
            var col = columns[i];
            var sqlType = GetSqlType(col.DataType);

            if (schemaTable != null)
            {
                var schemaRow = schemaTable.Rows[i];
                var maxLength = schemaRow["ColumnSize"] as int? ?? 0;
                var isNullable = schemaRow["AllowDBNull"] as bool? ?? true;

                if (sqlType == "NVARCHAR" || sqlType == "VARCHAR")
                {
                    sqlType = maxLength > 0 && maxLength < 8000 ? $"{sqlType}({maxLength})" : $"{sqlType}(MAX)";
                }

                columnDefs.Add($"[{col.ColumnName}] {sqlType} {(isNullable ? "NULL" : "NOT NULL")}");
            }
            else
            {
                columnDefs.Add($"[{col.ColumnName}] {sqlType} NULL");
            }
        }

        return $"CREATE TABLE {tableName} ({string.Join(", ", columnDefs)})";
    }

    private static string GetSqlType(Type type)
    {
        return type.Name switch
        {
            "String" => "NVARCHAR(MAX)",
            "Int32" => "INT",
            "Int64" => "BIGINT",
            "Int16" => "SMALLINT",
            "Byte" => "TINYINT",
            "Boolean" => "BIT",
            "DateTime" => "DATETIME2",
            "DateTimeOffset" => "DATETIMEOFFSET",
            "Decimal" => "DECIMAL(18,4)",
            "Double" => "FLOAT",
            "Single" => "REAL",
            "Guid" => "UNIQUEIDENTIFIER",
            "Byte[]" => "VARBINARY(MAX)",
            _ => "NVARCHAR(MAX)"
        };
    }
}
