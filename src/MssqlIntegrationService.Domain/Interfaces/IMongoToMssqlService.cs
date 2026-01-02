using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Domain.Interfaces;

/// <summary>
/// Interface for MongoDB to MSSQL data transfer operations
/// </summary>
public interface IMongoToMssqlService
{
    /// <summary>
    /// Transfer data from MongoDB collection to MSSQL table
    /// </summary>
    /// <param name="mongoConnectionString">MongoDB connection string</param>
    /// <param name="mongoDatabaseName">MongoDB database name</param>
    /// <param name="mongoCollection">MongoDB collection name</param>
    /// <param name="mongoFilter">MongoDB filter (JSON format, empty for all documents)</param>
    /// <param name="mssqlConnectionString">MSSQL connection string</param>
    /// <param name="targetTable">Target MSSQL table name</param>
    /// <param name="options">Transfer options</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task<Result<MongoToMssqlResult>> TransferAsync(
        string mongoConnectionString,
        string mongoDatabaseName,
        string mongoCollection,
        string? mongoFilter,
        string mssqlConnectionString,
        string targetTable,
        MongoToMssqlOptions? options = null,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Transfer data from MongoDB aggregation pipeline to MSSQL table
    /// </summary>
    /// <param name="mongoConnectionString">MongoDB connection string</param>
    /// <param name="mongoDatabaseName">MongoDB database name</param>
    /// <param name="mongoCollection">MongoDB collection name</param>
    /// <param name="aggregationPipeline">MongoDB aggregation pipeline (JSON array format)</param>
    /// <param name="mssqlConnectionString">MSSQL connection string</param>
    /// <param name="targetTable">Target MSSQL table name</param>
    /// <param name="options">Transfer options</param>
    /// <param name="cancellationToken">Cancellation token</param>
    Task<Result<MongoToMssqlResult>> TransferWithAggregationAsync(
        string mongoConnectionString,
        string mongoDatabaseName,
        string mongoCollection,
        string aggregationPipeline,
        string mssqlConnectionString,
        string targetTable,
        MongoToMssqlOptions? options = null,
        CancellationToken cancellationToken = default);
}
