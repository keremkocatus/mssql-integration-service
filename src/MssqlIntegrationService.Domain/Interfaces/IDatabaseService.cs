using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Domain.Interfaces;

public interface IDatabaseService
{
    Task<Result<QueryResult>> ExecuteQueryAsync(string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default);
    
    Task<Result<int>> ExecuteNonQueryAsync(string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default);
    
    Task<Result<T?>> ExecuteScalarAsync<T>(string query, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default);
    
    Task<Result<QueryResult>> ExecuteStoredProcedureAsync(string procedureName, IDictionary<string, object?>? parameters = null, CancellationToken cancellationToken = default);
}
