using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Application.DTOs;

#region Job Response DTOs

/// <summary>
/// Response when a job is created
/// </summary>
public class JobCreatedResponse
{
    /// <summary>
    /// Unique job ID for polling
    /// </summary>
    public string JobId { get; set; } = string.Empty;

    /// <summary>
    /// Current status of the job
    /// </summary>
    public string Status { get; set; } = "Pending";

    /// <summary>
    /// URL to poll for job status
    /// </summary>
    public string StatusUrl { get; set; } = string.Empty;

    /// <summary>
    /// Message
    /// </summary>
    public string Message { get; set; } = "Job created successfully. Use StatusUrl to poll for status.";
}

/// <summary>
/// Response for job status query
/// </summary>
public class JobStatusResponse
{
    /// <summary>
    /// Job ID
    /// </summary>
    public string JobId { get; set; } = string.Empty;

    /// <summary>
    /// Job type
    /// </summary>
    public string Type { get; set; } = string.Empty;

    /// <summary>
    /// Current status (Pending, Running, Completed, Failed, Cancelled)
    /// </summary>
    public string Status { get; set; } = string.Empty;

    /// <summary>
    /// Progress percentage (0-100)
    /// </summary>
    public int Progress { get; set; }

    /// <summary>
    /// Progress message
    /// </summary>
    public string? ProgressMessage { get; set; }

    /// <summary>
    /// Error message if failed
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Result data if completed (JSON)
    /// </summary>
    public object? Result { get; set; }

    /// <summary>
    /// When the job was created
    /// </summary>
    public DateTime CreatedAt { get; set; }

    /// <summary>
    /// When the job started processing
    /// </summary>
    public DateTime? StartedAt { get; set; }

    /// <summary>
    /// When the job completed
    /// </summary>
    public DateTime? CompletedAt { get; set; }

    /// <summary>
    /// Total duration in milliseconds (if completed)
    /// </summary>
    public long? DurationMs { get; set; }

    /// <summary>
    /// Whether the job is finished (completed, failed, or cancelled)
    /// </summary>
    public bool IsFinished { get; set; }
}

/// <summary>
/// Response for job list
/// </summary>
public class JobListResponse
{
    public List<JobStatusResponse> Jobs { get; set; } = new();
    public int TotalCount { get; set; }
}

#endregion

#region Job Request DTOs

/// <summary>
/// Request to create a DataTransfer job
/// </summary>
public class DataTransferJobRequest
{
    /// <summary>
    /// Source database configuration
    /// </summary>
    public required SourceConfig Source { get; set; }

    /// <summary>
    /// Target database configuration
    /// </summary>
    public required TargetConfig Target { get; set; }

    /// <summary>
    /// Transfer options
    /// </summary>
    public TransferOptionsDto? Options { get; set; }
}

/// <summary>
/// Request to create a DataSync job
/// </summary>
public class DataSyncJobRequest
{
    /// <summary>
    /// Source database configuration
    /// </summary>
    public required SourceConfig Source { get; set; }

    /// <summary>
    /// Target sync configuration
    /// </summary>
    public required SyncTargetConfig Target { get; set; }

    /// <summary>
    /// Sync options
    /// </summary>
    public SyncOptionsDto? Options { get; set; }
}

/// <summary>
/// Request to create a MongoToMssql job
/// </summary>
public class MongoToMssqlJobRequest
{
    /// <summary>
    /// MongoDB source configuration
    /// </summary>
    public required MongoSourceConfig Source { get; set; }

    /// <summary>
    /// MSSQL target configuration
    /// </summary>
    public required MssqlTargetConfig Target { get; set; }

    /// <summary>
    /// Transfer options
    /// </summary>
    public MongoToMssqlOptionsDto? Options { get; set; }

    /// <summary>
    /// If true, transfers as raw JSON to a single-column table
    /// </summary>
    public bool AsJson { get; set; }
}

// Note: SyncTargetConfig and SyncOptionsDto are defined in DataSyncRequest.cs

#endregion

#region Mapping Extensions

public static class JobMappingExtensions
{
    public static JobStatusResponse ToResponse(this Job job)
    {
        var response = new JobStatusResponse
        {
            JobId = job.Id,
            Type = job.Type.ToString(),
            Status = job.Status.ToString(),
            Progress = job.Progress,
            ProgressMessage = job.ProgressMessage,
            ErrorMessage = job.ErrorMessage,
            CreatedAt = job.CreatedAt,
            StartedAt = job.StartedAt,
            CompletedAt = job.CompletedAt,
            IsFinished = job.Status is JobStatus.Completed or JobStatus.Failed or JobStatus.Cancelled
        };

        if (job.CompletedAt.HasValue && job.StartedAt.HasValue)
        {
            response.DurationMs = (long)(job.CompletedAt.Value - job.StartedAt.Value).TotalMilliseconds;
        }

        if (!string.IsNullOrEmpty(job.ResultPayload))
        {
            try
            {
                response.Result = System.Text.Json.JsonSerializer.Deserialize<object>(job.ResultPayload);
            }
            catch
            {
                response.Result = job.ResultPayload;
            }
        }

        return response;
    }
}

#endregion
