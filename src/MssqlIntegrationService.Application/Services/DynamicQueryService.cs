using System.Diagnostics;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Application.Services;

public class DynamicQueryService : IDynamicQueryService
{
    private readonly IDynamicDatabaseService _databaseService;

    public DynamicQueryService(IDynamicDatabaseService databaseService)
    {
        _databaseService = databaseService;
    }

    public async Task<QueryResponse> ExecuteQueryAsync(DynamicQueryRequest request, CancellationToken cancellationToken = default)
    {
        var result = await _databaseService.ExecuteQueryAsync(
            request.ConnectionString,
            request.Query,
            request.Parameters,
            request.Timeout,
            cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            return new QueryResponse
            {
                Success = true,
                Data = result.Data.Rows,
                RowCount = result.Data.RowCount,
                ExecutionTimeMs = result.Data.ExecutionTimeMs
            };
        }

        return new QueryResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage,
            ErrorCode = result.ErrorCode
        };
    }

    public async Task<QueryResponse> ExecuteNonQueryAsync(DynamicQueryRequest request, CancellationToken cancellationToken = default)
    {
        var result = await _databaseService.ExecuteNonQueryAsync(
            request.ConnectionString,
            request.Query,
            request.Parameters,
            request.Timeout,
            cancellationToken);

        if (result.IsSuccess)
        {
            return new QueryResponse
            {
                Success = true,
                AffectedRows = result.Data
            };
        }

        return new QueryResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage,
            ErrorCode = result.ErrorCode
        };
    }

    public async Task<QueryResponse> ExecuteStoredProcedureAsync(DynamicStoredProcedureRequest request, CancellationToken cancellationToken = default)
    {
        var result = await _databaseService.ExecuteStoredProcedureAsync(
            request.ConnectionString,
            request.ProcedureName,
            request.Parameters,
            request.Timeout,
            cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            return new QueryResponse
            {
                Success = true,
                Data = result.Data.Rows,
                RowCount = result.Data.RowCount,
                ExecutionTimeMs = result.Data.ExecutionTimeMs
            };
        }

        return new QueryResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage,
            ErrorCode = result.ErrorCode
        };
    }

    public async Task<TestConnectionResponse> TestConnectionAsync(TestConnectionRequest request, CancellationToken cancellationToken = default)
    {
        var stopwatch = Stopwatch.StartNew();
        var result = await _databaseService.TestConnectionAsync(request.ConnectionString, cancellationToken);
        stopwatch.Stop();

        return new TestConnectionResponse
        {
            Success = result.IsSuccess,
            Message = result.IsSuccess ? "Connection successful" : result.ErrorMessage,
            ResponseTimeMs = stopwatch.ElapsedMilliseconds
        };
    }

    public async Task<DatabaseInfoResponse> GetDatabaseInfoAsync(DatabaseInfoRequest request, CancellationToken cancellationToken = default)
    {
        var result = await _databaseService.GetDatabaseInfoAsync(request.ConnectionString, cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            var response = new DatabaseInfoResponse
            {
                Success = true,
                ServerName = result.Data.ServerName,
                DatabaseName = result.Data.DatabaseName,
                ServerVersion = result.Data.ServerVersion,
                Edition = result.Data.Edition
            };

            if (request.IncludeTables)
            {
                response.Tables = result.Data.Tables.Select(t => new TableInfoDto
                {
                    SchemaName = t.SchemaName,
                    TableName = t.TableName,
                    RowCount = t.RowCount,
                    Columns = request.IncludeColumns
                        ? t.Columns.Select(c => new ColumnInfoDto
                        {
                            ColumnName = c.ColumnName,
                            DataType = c.DataType,
                            MaxLength = c.MaxLength,
                            IsNullable = c.IsNullable,
                            IsPrimaryKey = c.IsPrimaryKey,
                            IsIdentity = c.IsIdentity
                        }).ToList()
                        : null
                }).ToList();
            }

            return response;
        }

        return new DatabaseInfoResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage
        };
    }
}
