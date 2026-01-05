namespace MssqlIntegrationService.Domain.Entities;

/// <summary>
/// Represents a background job for long-running ETL operations
/// </summary>
public class Job
{
    /// <summary>
    /// Unique identifier for the job
    /// </summary>
    public string Id { get; set; } = Guid.NewGuid().ToString("N");

    /// <summary>
    /// Type of job (DataTransfer, DataSync, MongoToMssql, etc.)
    /// </summary>
    public JobType Type { get; set; }

    /// <summary>
    /// Current status of the job
    /// </summary>
    public JobStatus Status { get; set; } = JobStatus.Pending;

    /// <summary>
    /// Serialized request payload (JSON)
    /// </summary>
    public string RequestPayload { get; set; } = string.Empty;

    /// <summary>
    /// Serialized result payload (JSON) - populated when job completes
    /// </summary>
    public string? ResultPayload { get; set; }

    /// <summary>
    /// Error message if job failed
    /// </summary>
    public string? ErrorMessage { get; set; }

    /// <summary>
    /// Progress percentage (0-100)
    /// </summary>
    public int Progress { get; set; }

    /// <summary>
    /// Progress message (e.g., "Reading from source...", "Writing to target...")
    /// </summary>
    public string? ProgressMessage { get; set; }

    /// <summary>
    /// When the job was created
    /// </summary>
    public DateTime CreatedAt { get; set; } = DateTime.UtcNow;

    /// <summary>
    /// When the job started processing
    /// </summary>
    public DateTime? StartedAt { get; set; }

    /// <summary>
    /// When the job completed (success or failure)
    /// </summary>
    public DateTime? CompletedAt { get; set; }

    /// <summary>
    /// Optional: User or client identifier who created the job
    /// </summary>
    public string? CreatedBy { get; set; }

    /// <summary>
    /// Optional: Correlation ID for tracking
    /// </summary>
    public string? CorrelationId { get; set; }
}

/// <summary>
/// Job status enumeration
/// </summary>
public enum JobStatus
{
    /// <summary>Job is queued and waiting to be processed</summary>
    Pending = 0,
    
    /// <summary>Job is currently being processed</summary>
    Running = 1,
    
    /// <summary>Job completed successfully</summary>
    Completed = 2,
    
    /// <summary>Job failed with an error</summary>
    Failed = 3,
    
    /// <summary>Job was cancelled</summary>
    Cancelled = 4
}

/// <summary>
/// Job type enumeration
/// </summary>
public enum JobType
{
    /// <summary>MSSQL to MSSQL data transfer</summary>
    DataTransfer = 0,
    
    /// <summary>Bulk insert operation</summary>
    BulkInsert = 1,
    
    /// <summary>Data synchronization (delete-insert pattern)</summary>
    DataSync = 2,
    
    /// <summary>MongoDB to MSSQL transfer</summary>
    MongoToMssql = 3,
    
    /// <summary>MongoDB to MSSQL as raw JSON</summary>
    MongoToMssqlJson = 4
}
