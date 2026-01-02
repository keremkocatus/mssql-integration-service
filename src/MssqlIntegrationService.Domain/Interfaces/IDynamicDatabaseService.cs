using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Domain.Interfaces;

public interface IDynamicDatabaseService
{
    Task<Result<QueryResult>> ExecuteQueryAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        int? timeout = null,
        CancellationToken cancellationToken = default);

    Task<Result<int>> ExecuteNonQueryAsync(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        int? timeout = null,
        CancellationToken cancellationToken = default);

    Task<Result<T?>> ExecuteScalarAsync<T>(
        string connectionString,
        string query,
        IDictionary<string, object?>? parameters = null,
        int? timeout = null,
        CancellationToken cancellationToken = default);

    Task<Result<QueryResult>> ExecuteStoredProcedureAsync(
        string connectionString,
        string procedureName,
        IDictionary<string, object?>? parameters = null,
        int? timeout = null,
        CancellationToken cancellationToken = default);

    Task<Result<bool>> TestConnectionAsync(
        string connectionString,
        CancellationToken cancellationToken = default);

    Task<Result<DatabaseInfo>> GetDatabaseInfoAsync(
        string connectionString,
        CancellationToken cancellationToken = default);
}
