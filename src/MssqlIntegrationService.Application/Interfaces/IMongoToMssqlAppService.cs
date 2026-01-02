using MssqlIntegrationService.Application.DTOs;

namespace MssqlIntegrationService.Application.Interfaces;

/// <summary>
/// Application service interface for MongoDB to MSSQL operations
/// </summary>
public interface IMongoToMssqlAppService
{
    /// <summary>
    /// Transfer data from MongoDB to MSSQL
    /// </summary>
    Task<MongoToMssqlResponse> TransferAsync(MongoToMssqlRequest request, CancellationToken cancellationToken = default);
}
