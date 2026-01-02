using MssqlIntegrationService.Application.DTOs;

namespace MssqlIntegrationService.Application.Interfaces;

public interface IDynamicQueryService
{
    Task<QueryResponse> ExecuteQueryAsync(DynamicQueryRequest request, CancellationToken cancellationToken = default);
    Task<QueryResponse> ExecuteNonQueryAsync(DynamicQueryRequest request, CancellationToken cancellationToken = default);
    Task<QueryResponse> ExecuteStoredProcedureAsync(DynamicStoredProcedureRequest request, CancellationToken cancellationToken = default);
    Task<TestConnectionResponse> TestConnectionAsync(TestConnectionRequest request, CancellationToken cancellationToken = default);
    Task<DatabaseInfoResponse> GetDatabaseInfoAsync(DatabaseInfoRequest request, CancellationToken cancellationToken = default);
}
