using Microsoft.AspNetCore.Mvc;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;

namespace MssqlIntegrationService.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class DataTransferController : ControllerBase
{
    private readonly IDataTransferAppService _dataTransferService;
    private readonly ILogger<DataTransferController> _logger;

    public DataTransferController(IDataTransferAppService dataTransferService, ILogger<DataTransferController> logger)
    {
        _dataTransferService = dataTransferService;
        _logger = logger;
    }

    /// <summary>
    /// Transfer data from one database to another
    /// </summary>
    /// <remarks>
    /// Example request:
    /// 
    ///     POST /api/datatransfer/transfer
    ///     {
    ///         "source": {
    ///             "connectionString": "Server=source-server;Database=SourceDB;...",
    ///             "query": "SELECT * FROM Users WHERE IsActive = 1"
    ///         },
    ///         "target": {
    ///             "connectionString": "Server=target-server;Database=TargetDB;...",
    ///             "tableName": "Users"
    ///         },
    ///         "options": {
    ///             "batchSize": 1000,
    ///             "truncateTargetTable": false,
    ///             "createTableIfNotExists": true,
    ///             "useTransaction": true
    ///         }
    ///     }
    /// </remarks>
    [HttpPost("transfer")]
    [ProducesResponseType(typeof(DataTransferResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(DataTransferResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> TransferData([FromBody] DataTransferRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Starting data transfer from source query to {TargetTable}",
            request.Target.TableName);

        var response = await _dataTransferService.TransferDataAsync(request, cancellationToken);

        if (!response.Success)
        {
            _logger.LogWarning("Data transfer failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        _logger.LogInformation(
            "Data transfer completed. Rows read: {RowsRead}, Rows written: {RowsWritten}, Time: {Time}ms",
            response.TotalRowsRead,
            response.TotalRowsWritten,
            response.ExecutionTimeMs);

        return Ok(response);
    }

    /// <summary>
    /// Bulk insert data into a table
    /// </summary>
    /// <remarks>
    /// Example request:
    /// 
    ///     POST /api/datatransfer/bulk-insert
    ///     {
    ///         "connectionString": "Server=myserver;Database=MyDB;...",
    ///         "tableName": "Users",
    ///         "data": [
    ///             { "Name": "John", "Email": "john@example.com" },
    ///             { "Name": "Jane", "Email": "jane@example.com" }
    ///         ],
    ///         "options": {
    ///             "batchSize": 1000,
    ///             "useTransaction": true
    ///         }
    ///     }
    /// </remarks>
    [HttpPost("bulk-insert")]
    [ProducesResponseType(typeof(BulkInsertResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(BulkInsertResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> BulkInsert([FromBody] BulkInsertRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation(
            "Starting bulk insert to {TableName} with {RowCount} rows",
            request.TableName,
            request.Data.Count);

        var response = await _dataTransferService.BulkInsertAsync(request, cancellationToken);

        if (!response.Success)
        {
            _logger.LogWarning("Bulk insert failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        _logger.LogInformation(
            "Bulk insert completed. Rows inserted: {RowsInserted}, Time: {Time}ms",
            response.TotalRowsInserted,
            response.ExecutionTimeMs);

        return Ok(response);
    }
}
