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

    /// <summary>
    /// Transfer MongoDB documents as raw JSON strings to a single-column MSSQL table.
    /// </summary>
    /// <remarks>
    /// Creates a table with suffix _JSON containing:
    /// - Id (BIGINT IDENTITY) - Auto-increment primary key
    /// - JsonData (NVARCHAR(MAX)) - Raw JSON document
    /// - CreatedAt (DATETIME2) - Insert timestamp
    /// 
    /// Use OPENJSON in MSSQL to parse the JSON later:
    /// ```sql
    /// SELECT j.*
    /// FROM Users_JSON
    /// CROSS APPLY OPENJSON(JsonData)
    /// WITH (
    ///     name NVARCHAR(100) '$.name',
    ///     email NVARCHAR(200) '$.email',
    ///     age INT '$.age'
    /// ) AS j
    /// ```
    /// 
    /// Example request:
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
    ///     "tableName": "Users"
    ///   }
    /// }
    /// ```
    /// This will create table "Users_JSON" with raw JSON documents.
    /// </remarks>
    [HttpPost("transfer-as-json")]
    [ProducesResponseType(typeof(MongoToMssqlResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(MongoToMssqlResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> TransferAsJson([FromBody] MongoToMssqlRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Starting MongoDB to MSSQL JSON transfer: {Collection} → {Table}_JSON",
            request.Source.CollectionName,
            request.Target.TableName);

        var response = await _mongoToMssqlService.TransferAsJsonAsync(request, cancellationToken);

        if (!response.Success)
        {
            _logger.LogWarning("MongoDB to MSSQL JSON transfer failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        _logger.LogInformation(
            "JSON transfer completed: {DocsRead} documents read, {RowsWritten} rows written in {TimeMs}ms",
            response.TotalDocumentsRead,
            response.TotalRowsWritten,
            response.ExecutionTimeMs);

        return Ok(response);
    }
}
