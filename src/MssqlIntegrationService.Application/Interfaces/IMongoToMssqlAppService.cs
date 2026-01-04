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

    /// <summary>
    /// Transfer data from MongoDB to MSSQL as raw JSON strings.
    /// Creates a single-column table (TableName_JSON) for later parsing with OPENJSON.
    /// </summary>
    Task<MongoToMssqlResponse> TransferAsJsonAsync(MongoToMssqlRequest request, CancellationToken cancellationToken = default);
}
