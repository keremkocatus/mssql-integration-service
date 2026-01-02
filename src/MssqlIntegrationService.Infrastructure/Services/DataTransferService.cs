using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Infrastructure.Services;

public class DataTransferService : IDataTransferService
{
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
            // Read data from source
            await using var sourceConnection = new SqlConnection(sourceConnectionString);
            await sourceConnection.OpenAsync(cancellationToken);

            await using var sourceCommand = new SqlCommand(sourceQuery, sourceConnection)
            {
                CommandTimeout = options.Timeout
            };

            await using var reader = await sourceCommand.ExecuteReaderAsync(cancellationToken);

            // Get schema information
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

            // Create DataTable for bulk copy
            var dataTable = new DataTable();
            foreach (var col in columns)
            {
                dataTable.Columns.Add(new DataColumn(col.ColumnName, col.DataType));
            }

            // Read all rows
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

            await reader.CloseAsync();
            await sourceConnection.CloseAsync();

            // Write to target
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
                    await using var truncateCommand = new SqlCommand($"TRUNCATE TABLE {targetTable}", targetConnection, transaction);
                    await truncateCommand.ExecuteNonQueryAsync(cancellationToken);
                    result.Warnings.Add($"Table {targetTable} was truncated before insert");
                }

                // Create table if requested
                if (options.CreateTableIfNotExists)
                {
                    var createTableSql = GenerateCreateTableSql(targetTable, columns, schemaTable);
                    await using var checkCommand = new SqlCommand(
                        $"IF OBJECT_ID('{targetTable}', 'U') IS NULL BEGIN {createTableSql} END",
                        targetConnection, transaction);
                    await checkCommand.ExecuteNonQueryAsync(cancellationToken);
                }

                // Bulk copy
                using var bulkCopy = new SqlBulkCopy(targetConnection, SqlBulkCopyOptions.Default, transaction)
                {
                    DestinationTableName = targetTable,
                    BatchSize = options.BatchSize,
                    BulkCopyTimeout = options.Timeout
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
                    foreach (DataColumn col in dataTable.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }
                }

                await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);
                result.TotalRowsWritten = result.TotalRowsRead;

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
            var dataList = data.ToList();
            if (dataList.Count == 0)
            {
                return Result<TransferResult>.Success(result);
            }

            // Create DataTable from data
            var dataTable = new DataTable();
            var firstRow = dataList.First();

            foreach (var key in firstRow.Keys)
            {
                var value = firstRow[key];
                var type = value?.GetType() ?? typeof(string);
                dataTable.Columns.Add(new DataColumn(key, Nullable.GetUnderlyingType(type) ?? type));
            }

            foreach (var item in dataList)
            {
                var row = dataTable.NewRow();
                foreach (var key in item.Keys)
                {
                    row[key] = item[key] ?? DBNull.Value;
                }
                dataTable.Rows.Add(row);
                result.TotalRowsRead++;
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
                using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
                {
                    DestinationTableName = tableName,
                    BatchSize = options.BatchSize,
                    BulkCopyTimeout = options.Timeout
                };

                foreach (DataColumn col in dataTable.Columns)
                {
                    bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                }

                await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);
                result.TotalRowsWritten = result.TotalRowsRead;

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
