using MssqlIntegrationService.Application.DTOs;

namespace MssqlIntegrationService.Application.Interfaces;

public interface IQueryService
{
    Task<QueryResponse> ExecuteQueryAsync(QueryRequest request, CancellationToken cancellationToken = default);
    
    Task<QueryResponse> ExecuteNonQueryAsync(QueryRequest request, CancellationToken cancellationToken = default);
    
    Task<QueryResponse> ExecuteStoredProcedureAsync(StoredProcedureRequest request, CancellationToken cancellationToken = default);
}
