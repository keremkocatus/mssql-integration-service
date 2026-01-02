using Microsoft.AspNetCore.Mvc;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;

namespace MssqlIntegrationService.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class MongoToMssqlController : ControllerBase
{
    private readonly IMongoToMssqlAppService _mongoToMssqlService;
    private readonly ILogger<MongoToMssqlController> _logger;

    public MongoToMssqlController(IMongoToMssqlAppService mongoToMssqlService, ILogger<MongoToMssqlController> logger)
    {
        _mongoToMssqlService = mongoToMssqlService;
        _logger = logger;
    }

    /// <summary>
    /// Transfer data from MongoDB collection to MSSQL table
    /// </summary>
    /// <remarks>
    /// Supports:
    /// - Simple filter-based transfer
    /// - Aggregation pipeline for complex transformations
    /// - Field mapping (MongoDB field → MSSQL column)
    /// - Nested document flattening
    /// - Array handling (Serialize, Skip, FirstElement)
    /// - Auto table creation
    /// 
    /// Example request with filter:
    /// ```json
    /// {
    ///   "source": {
    ///     "connectionString": "mongodb://localhost:27017",
    ///     "databaseName": "mydb",
    ///     "collectionName": "users",
    ///     "filter": "{ \"status\": \"active\" }"
    ///   },
    ///   "target": {
    ///     "connectionString": "Server=...;Database=...;",
    ///     "tableName": "dbo.Users"
    ///   },
    ///   "options": {
    ///     "createTableIfNotExists": true,
    ///     "flattenNestedDocuments": true
    ///   }
    /// }
    /// ```
    /// 
    /// Example with aggregation pipeline:
    /// ```json
    /// {
    ///   "source": {
    ///     "connectionString": "mongodb://localhost:27017",
    ///     "databaseName": "mydb",
    ///     "collectionName": "orders",
    ///     "aggregationPipeline": "[{ \"$match\": { \"status\": \"completed\" } }, { \"$project\": { \"orderId\": 1, \"total\": 1 } }]"
    ///   },
    ///   "target": {
    ///     "connectionString": "Server=...;Database=...;",
    ///     "tableName": "dbo.CompletedOrders"
    ///   }
    /// }
    /// ```
    /// </remarks>
    [HttpPost("transfer")]
    [ProducesResponseType(typeof(MongoToMssqlResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(MongoToMssqlResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> Transfer([FromBody] MongoToMssqlRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Starting MongoDB to MSSQL transfer: {Collection} → {Table}",
            request.Source.CollectionName,
            request.Target.TableName);

        var response = await _mongoToMssqlService.TransferAsync(request, cancellationToken);

        if (!response.Success)
        {
            _logger.LogWarning("MongoDB to MSSQL transfer failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        _logger.LogInformation(
            "Transfer completed: {DocsRead} documents read, {RowsWritten} rows written in {TimeMs}ms",
            response.TotalDocumentsRead,
            response.TotalRowsWritten,
            response.ExecutionTimeMs);

        return Ok(response);
    }
}
