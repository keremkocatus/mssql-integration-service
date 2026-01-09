using Microsoft.AspNetCore.Mvc;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;

namespace MssqlIntegrationService.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class DataSyncController : ControllerBase
{
    private readonly IDataSyncAppService _dataSyncService;
    private readonly ILogger<DataSyncController> _logger;

    public DataSyncController(IDataSyncAppService dataSyncService, ILogger<DataSyncController> logger)
    {
        _dataSyncService = dataSyncService;
        _logger = logger;
    }

    /// <summary>
    /// Sync data from source database to target database using delete-insert pattern
    /// </summary>
    /// <remarks>
    /// This operation performs the following steps:
    /// 1. Read data from source database using the provided query
    /// 2. Create a temporary table in target database
    /// 3. Bulk insert source data into temp table
    /// 4. Delete matching rows from target table (based on key columns)
    /// 5. Insert data from temp table to target table
    /// 6. Drop temp table
    /// 
    /// All operations are wrapped in a transaction for atomicity.
    /// 
    /// Example request:
    /// 
    ///     POST /api/datasync/sync
    ///     {
    ///         "source": {
    ///             "connectionString": "Server=source-server;Database=SourceDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
    ///             "query": "SELECT Id, Name, Email, UpdatedAt FROM Users WHERE IsActive = 1"
    ///         },
    ///         "target": {
    ///             "connectionString": "Server=target-server;Database=TargetDB;User Id=sa;Password=xxx;TrustServerCertificate=true;",
    ///             "tableName": "Users",
    ///             "keyColumns": ["Id"]
    ///         },
    ///         "options": {
    ///             "batchSize": 1000,
    ///             "useTransaction": true,
    ///             "deleteAllBeforeInsert": false
    ///         }
    ///     }
    /// 
    /// Key columns are used to match records between temp table and target table for deletion.
    /// If deleteAllBeforeInsert is true, all rows in target table will be deleted before insert.
    /// </remarks>
    [HttpPost("sync")]
    [ProducesResponseType(typeof(DataSyncResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(DataSyncResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> SyncData([FromBody] DataSyncRequest request, CancellationToken cancellationToken)
    {
        var keyColumnsInfo = request.Target.KeyColumns != null && request.Target.KeyColumns.Count > 0
            ? string.Join(", ", request.Target.KeyColumns)
            : "(auto-detect from schema)";

        _logger.LogInformation(
            "Starting data sync from source to {TargetTable} with keys: {KeyColumns}",
            request.Target.TableName,
            keyColumnsInfo);

        var response = await _dataSyncService.SyncDataAsync(request, cancellationToken);

        if (!response.Success)
        {
            _logger.LogWarning("Data sync failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        _logger.LogInformation(
            "Data sync completed. Read: {Read}, Deleted: {Deleted}, Inserted: {Inserted}, Time: {Time}ms",
            response.TotalRowsRead,
            response.RowsDeleted,
            response.RowsInserted,
            response.ExecutionTimeMs);

        return Ok(response);
    }
}
