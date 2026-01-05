using Microsoft.AspNetCore.Mvc;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;

namespace MssqlIntegrationService.API.Controllers;

/// <summary>
/// Controller for managing background ETL jobs.
/// Jobs are processed asynchronously - create a job and poll for status.
/// </summary>
[ApiController]
[Route("api/[controller]")]
public class JobsController : ControllerBase
{
    private readonly IJobAppService _jobService;
    private readonly ILogger<JobsController> _logger;

    public JobsController(IJobAppService jobService, ILogger<JobsController> logger)
    {
        _jobService = jobService;
        _logger = logger;
    }

    /// <summary>
    /// Create a DataTransfer job (async - returns immediately with job ID)
    /// </summary>
    /// <remarks>
    /// Creates a background job for MSSQL to MSSQL data transfer.
    /// Poll the returned StatusUrl to check job progress.
    /// 
    /// Example:
    /// ```json
    /// {
    ///   "source": {
    ///     "connectionString": "Server=source;Database=SourceDB;...",
    ///     "query": "SELECT * FROM Users WHERE IsActive = 1"
    ///   },
    ///   "target": {
    ///     "connectionString": "Server=target;Database=TargetDB;...",
    ///     "tableName": "Users"
    ///   },
    ///   "options": {
    ///     "batchSize": 1000,
    ///     "createTableIfNotExists": true
    ///   }
    /// }
    /// ```
    /// </remarks>
    [HttpPost("data-transfer")]
    [ProducesResponseType(typeof(JobCreatedResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CreateDataTransferJob([FromBody] DataTransferJobRequest request, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Creating DataTransfer job for table {Table}", request.Target.TableName);
            var response = await _jobService.CreateDataTransferJobAsync(request, cancellationToken);
            return AcceptedAtAction(nameof(GetJobStatus), new { jobId = response.JobId }, response);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    /// <summary>
    /// Create a DataSync job (async - returns immediately with job ID)
    /// </summary>
    /// <remarks>
    /// Creates a background job for data synchronization using delete-insert pattern.
    /// Poll the returned StatusUrl to check job progress.
    /// 
    /// Example:
    /// ```json
    /// {
    ///   "source": {
    ///     "connectionString": "Server=source;Database=SourceDB;...",
    ///     "query": "SELECT Id, Name, Email FROM Users"
    ///   },
    ///   "target": {
    ///     "connectionString": "Server=target;Database=TargetDB;...",
    ///     "tableName": "Users",
    ///     "keyColumns": ["Id"]
    ///   }
    /// }
    /// ```
    /// </remarks>
    [HttpPost("data-sync")]
    [ProducesResponseType(typeof(JobCreatedResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CreateDataSyncJob([FromBody] DataSyncJobRequest request, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Creating DataSync job for table {Table}", request.Target.TableName);
            var response = await _jobService.CreateDataSyncJobAsync(request, cancellationToken);
            return AcceptedAtAction(nameof(GetJobStatus), new { jobId = response.JobId }, response);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    /// <summary>
    /// Create a MongoDB to MSSQL transfer job (async - returns immediately with job ID)
    /// </summary>
    /// <remarks>
    /// Creates a background job for MongoDB to MSSQL data transfer.
    /// Set `asJson: true` to transfer documents as raw JSON strings.
    /// Poll the returned StatusUrl to check job progress.
    /// 
    /// Example:
    /// ```json
    /// {
    ///   "source": {
    ///     "connectionString": "mongodb://localhost:27017",
    ///     "databaseName": "mydb",
    ///     "collectionName": "users",
    ///     "filter": "{ \"status\": \"active\" }"
    ///   },
    ///   "target": {
    ///     "connectionString": "Server=mssql;Database=MyDB;...",
    ///     "tableName": "Users"
    ///   },
    ///   "asJson": false
    /// }
    /// ```
    /// </remarks>
    [HttpPost("mongo-to-mssql")]
    [ProducesResponseType(typeof(JobCreatedResponse), StatusCodes.Status202Accepted)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> CreateMongoToMssqlJob([FromBody] MongoToMssqlJobRequest request, CancellationToken cancellationToken)
    {
        try
        {
            _logger.LogInformation("Creating MongoToMssql job for collection {Collection} → {Table}", 
                request.Source.CollectionName, request.Target.TableName);
            var response = await _jobService.CreateMongoToMssqlJobAsync(request, cancellationToken);
            return AcceptedAtAction(nameof(GetJobStatus), new { jobId = response.JobId }, response);
        }
        catch (ArgumentException ex)
        {
            return BadRequest(new { error = ex.Message });
        }
    }

    /// <summary>
    /// Get job status by ID (for polling)
    /// </summary>
    /// <remarks>
    /// Returns the current status of a job. Poll this endpoint to track progress.
    /// 
    /// Status values:
    /// - **Pending**: Job is queued, waiting to be processed
    /// - **Running**: Job is currently being processed
    /// - **Completed**: Job finished successfully (check `result` field)
    /// - **Failed**: Job failed (check `errorMessage` field)
    /// - **Cancelled**: Job was cancelled
    /// 
    /// When `isFinished` is true, stop polling.
    /// </remarks>
    [HttpGet("{jobId}")]
    [ProducesResponseType(typeof(JobStatusResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> GetJobStatus(string jobId, CancellationToken cancellationToken)
    {
        var status = await _jobService.GetJobStatusAsync(jobId, cancellationToken);
        if (status == null)
        {
            return NotFound(new { error = $"Job '{jobId}' not found" });
        }
        return Ok(status);
    }

    /// <summary>
    /// Get recent jobs
    /// </summary>
    /// <param name="limit">Maximum number of jobs to return (default: 50)</param>
    /// <param name="cancellationToken"></param>
    [HttpGet]
    [ProducesResponseType(typeof(JobListResponse), StatusCodes.Status200OK)]
    public async Task<IActionResult> GetRecentJobs([FromQuery] int limit = 50, CancellationToken cancellationToken = default)
    {
        var jobs = await _jobService.GetRecentJobsAsync(limit, cancellationToken);
        return Ok(jobs);
    }

    /// <summary>
    /// Cancel a pending job
    /// </summary>
    /// <remarks>
    /// Only pending jobs can be cancelled. Running jobs cannot be stopped.
    /// </remarks>
    [HttpPost("{jobId}/cancel")]
    [ProducesResponseType(StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    public async Task<IActionResult> CancelJob(string jobId, CancellationToken cancellationToken)
    {
        var result = await _jobService.CancelJobAsync(jobId, cancellationToken);
        if (!result)
        {
            return BadRequest(new { error = "Job cannot be cancelled (not found or not in Pending status)" });
        }
        return Ok(new { message = "Job cancelled successfully" });
    }
}
