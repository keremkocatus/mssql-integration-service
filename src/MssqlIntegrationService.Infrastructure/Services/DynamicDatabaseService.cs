using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Infrastructure.Services;

public class DynamicDatabaseService : IDynamicDatabaseService
{
    public async Task<Result<QueryResult>> ExecuteQueryAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        int? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(query, connection);
            if (timeout.HasValue)
                command.CommandTimeout = timeout.Value;

            AddParameters(command, parameters);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var rows = new List<Dictionary<string, object?>>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.GetValue(i);
                    row[reader.GetName(i)] = value == DBNull.Value ? null : value;
                }
                rows.Add(row);
            }

            stopwatch.Stop();

            return Result<QueryResult>.Success(new QueryResult
            {
                Rows = rows,
                RowCount = rows.Count,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }
        catch (SqlException ex)
        {
            stopwatch.Stop();
            return Result<QueryResult>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return Result<QueryResult>.Failure(ex.Message);
        }
    }

    public async Task<Result<int>> ExecuteNonQueryAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        int? timeout = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(query, connection);
            if (timeout.HasValue)
                command.CommandTimeout = timeout.Value;

            AddParameters(command, parameters);

            var affectedRows = await command.ExecuteNonQueryAsync(cancellationToken);

            return Result<int>.Success(affectedRows);
        }
        catch (SqlException ex)
        {
            return Result<int>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            return Result<int>.Failure(ex.Message);
        }
    }

    public async Task<Result<T?>> ExecuteScalarAsync<T>(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        int? timeout = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(query, connection);
            if (timeout.HasValue)
                command.CommandTimeout = timeout.Value;

            AddParameters(command, parameters);

            var result = await command.ExecuteScalarAsync(cancellationToken);

            if (result == null || result == DBNull.Value)
            {
                return Result<T?>.Success(default);
            }

            return Result<T?>.Success((T)Convert.ChangeType(result, typeof(T)));
        }
        catch (SqlException ex)
        {
            return Result<T?>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            return Result<T?>.Failure(ex.Message);
        }
    }

    public async Task<Result<QueryResult>> ExecuteStoredProcedureAsync(
        string connectionString,
        string procedureName,
        IDictionary<string, object?>? parameters = null,
        int? timeout = null,
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();

        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(procedureName, connection)
            {
                CommandType = CommandType.StoredProcedure
            };
            if (timeout.HasValue)
                command.CommandTimeout = timeout.Value;

            AddParameters(command, parameters);

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);

            var rows = new List<Dictionary<string, object?>>();

            while (await reader.ReadAsync(cancellationToken))
            {
                var row = new Dictionary<string, object?>();
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.GetValue(i);
                    row[reader.GetName(i)] = value == DBNull.Value ? null : value;
                }
                rows.Add(row);
            }

            stopwatch.Stop();

            return Result<QueryResult>.Success(new QueryResult
            {
                Rows = rows,
                RowCount = rows.Count,
                ExecutionTimeMs = stopwatch.ElapsedMilliseconds
            });
        }
        catch (SqlException ex)
        {
            stopwatch.Stop();
            return Result<QueryResult>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return Result<QueryResult>.Failure(ex.Message);
        }
    }

    public async Task<Result<bool>> TestConnectionAsync(
        string connectionString,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);
            return Result<bool>.Success(true);
        }
        catch (SqlException ex)
        {
            return Result<bool>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            return Result<bool>.Failure(ex.Message);
        }
    }

    public async Task<Result<DatabaseInfo>> GetDatabaseInfoAsync(
        string connectionString,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            var info = new DatabaseInfo
            {
                ServerName = connection.DataSource,
                DatabaseName = connection.Database,
                ServerVersion = connection.ServerVersion
            };

            // Get edition
            await using (var cmd = new SqlCommand("SELECT SERVERPROPERTY('Edition')", connection))
            {
                var edition = await cmd.ExecuteScalarAsync(cancellationToken);
                info.Edition = edition?.ToString();
            }

            // Get tables
            const string tableQuery = @"
                SELECT 
                    s.name AS SchemaName,
                    t.name AS TableName,
                    p.rows AS RowCount
                FROM sys.tables t
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.partitions p ON t.object_id = p.object_id AND p.index_id IN (0, 1)
                ORDER BY s.name, t.name";

            await using (var cmd = new SqlCommand(tableQuery, connection))
            {
                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    info.Tables.Add(new TableInfo
                    {
                        SchemaName = reader.GetString(0),
                        TableName = reader.GetString(1),
                        RowCount = reader.GetInt64(2)
                    });
                }
            }

            // Get columns for each table
            const string columnQuery = @"
                SELECT 
                    c.name AS ColumnName,
                    t.name AS DataType,
                    c.max_length AS MaxLength,
                    c.is_nullable AS IsNullable,
                    ISNULL(i.is_primary_key, 0) AS IsPrimaryKey,
                    c.is_identity AS IsIdentity
                FROM sys.columns c
                INNER JOIN sys.types t ON c.user_type_id = t.user_type_id
                LEFT JOIN sys.index_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
                LEFT JOIN sys.indexes i ON ic.object_id = i.object_id AND ic.index_id = i.index_id AND i.is_primary_key = 1
                WHERE c.object_id = OBJECT_ID(@tableName)
                ORDER BY c.column_id";

            foreach (var table in info.Tables)
            {
                await using var cmd = new SqlCommand(columnQuery, connection);
                cmd.Parameters.AddWithValue("@tableName", $"{table.SchemaName}.{table.TableName}");

                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    table.Columns.Add(new ColumnInfo
                    {
                        ColumnName = reader.GetString(0),
                        DataType = reader.GetString(1),
                        MaxLength = reader.GetInt16(2),
                        IsNullable = reader.GetBoolean(3),
                        IsPrimaryKey = reader.GetInt32(4) == 1,
                        IsIdentity = reader.GetBoolean(5)
                    });
                }
            }

            return Result<DatabaseInfo>.Success(info);
        }
        catch (SqlException ex)
        {
            return Result<DatabaseInfo>.Failure(ex.Message, ex.Number);
        }
        catch (Exception ex)
        {
            return Result<DatabaseInfo>.Failure(ex.Message);
        }
    }

    private static void AddParameters(SqlCommand command, IDictionary<string, object?>? parameters)
    {
        if (parameters == null) return;

        foreach (var param in parameters)
        {
            var paramName = param.Key.StartsWith('@') ? param.Key : $"@{param.Key}";
            command.Parameters.AddWithValue(paramName, param.Value ?? DBNull.Value);
        }
    }
}
