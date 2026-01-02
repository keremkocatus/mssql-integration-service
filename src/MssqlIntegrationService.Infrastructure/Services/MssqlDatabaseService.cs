using System.Data;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Infrastructure.Data;

namespace MssqlIntegrationService.Infrastructure.Services;

public class MssqlDatabaseService : IDatabaseService
{
    private readonly ISqlConnectionFactory _connectionFactory;

    public MssqlDatabaseService(ISqlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }

    public async Task<Result<QueryResult>> ExecuteQueryAsync(
        string query, 
        IDictionary<string, object?>? parameters = null, 
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            await using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(query, connection);
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
        string query, 
        IDictionary<string, object?>? parameters = null, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(query, connection);
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
        string query, 
        IDictionary<string, object?>? parameters = null, 
        CancellationToken cancellationToken = default)
    {
        try
        {
            await using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(query, connection);
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
        string procedureName, 
        IDictionary<string, object?>? parameters = null, 
        CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        
        try
        {
            await using var connection = _connectionFactory.CreateConnection();
            await connection.OpenAsync(cancellationToken);

            await using var command = new SqlCommand(procedureName, connection)
            {
                CommandType = CommandType.StoredProcedure
            };
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
