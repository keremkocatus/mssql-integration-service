using MssqlIntegrationService.Application.DTOs;

namespace MssqlIntegrationService.Application.Interfaces;

/// <summary>
/// Application service interface for Job operations
/// </summary>
public interface IJobAppService
{
    /// <summary>
    /// Creates a DataTransfer job
    /// </summary>
    Task<JobCreatedResponse> CreateDataTransferJobAsync(DataTransferJobRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a DataSync job
    /// </summary>
    Task<JobCreatedResponse> CreateDataSyncJobAsync(DataSyncJobRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Creates a MongoToMssql job
    /// </summary>
    Task<JobCreatedResponse> CreateMongoToMssqlJobAsync(MongoToMssqlJobRequest request, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets job status by ID
    /// </summary>
    Task<JobStatusResponse?> GetJobStatusAsync(string jobId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets recent jobs
    /// </summary>
    Task<JobListResponse> GetRecentJobsAsync(int limit = 50, CancellationToken cancellationToken = default);

    /// <summary>
    /// Cancels a pending job
    /// </summary>
    Task<bool> CancelJobAsync(string jobId, CancellationToken cancellationToken = default);
}
