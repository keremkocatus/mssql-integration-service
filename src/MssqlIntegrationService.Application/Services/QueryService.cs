using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Application.Services;

public class QueryService : IQueryService
{
    private readonly IDatabaseService _databaseService;

    public QueryService(IDatabaseService databaseService)
    {
        _databaseService = databaseService;
    }

    public async Task<QueryResponse> ExecuteQueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        var result = await _databaseService.ExecuteQueryAsync(
            request.Query, 
            request.Parameters, 
            cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            return new QueryResponse
            {
                Success = true,
                Data = result.Data.Rows,
                RowCount = result.Data.RowCount,
                AffectedRows = result.Data.AffectedRows,
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

    public async Task<QueryResponse> ExecuteNonQueryAsync(QueryRequest request, CancellationToken cancellationToken = default)
    {
        var result = await _databaseService.ExecuteNonQueryAsync(
            request.Query, 
            request.Parameters, 
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

    public async Task<QueryResponse> ExecuteStoredProcedureAsync(StoredProcedureRequest request, CancellationToken cancellationToken = default)
    {
        var result = await _databaseService.ExecuteStoredProcedureAsync(
            request.ProcedureName, 
            request.Parameters, 
            cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            return new QueryResponse
            {
                Success = true,
                Data = result.Data.Rows,
                RowCount = result.Data.RowCount,
                AffectedRows = result.Data.AffectedRows,
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
}
