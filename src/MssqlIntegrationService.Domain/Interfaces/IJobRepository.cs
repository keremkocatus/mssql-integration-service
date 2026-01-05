using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Domain.Interfaces;

/// <summary>
/// Repository interface for Job persistence
/// </summary>
public interface IJobRepository
{
    /// <summary>
    /// Creates a new job
    /// </summary>
    Task<Job> CreateAsync(Job job, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets a job by ID
    /// </summary>
    Task<Job?> GetByIdAsync(string jobId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates a job
    /// </summary>
    Task<Job> UpdateAsync(Job job, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates job status
    /// </summary>
    Task UpdateStatusAsync(string jobId, JobStatus status, string? errorMessage = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Updates job progress
    /// </summary>
    Task UpdateProgressAsync(string jobId, int progress, string? message = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks job as started
    /// </summary>
    Task MarkAsStartedAsync(string jobId, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks job as completed with result
    /// </summary>
    Task MarkAsCompletedAsync(string jobId, string? resultPayload = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Marks job as failed with error
    /// </summary>
    Task MarkAsFailedAsync(string jobId, string errorMessage, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets jobs by status
    /// </summary>
    Task<List<Job>> GetByStatusAsync(JobStatus status, int limit = 100, CancellationToken cancellationToken = default);

    /// <summary>
    /// Gets recent jobs (for listing)
    /// </summary>
    Task<List<Job>> GetRecentAsync(int limit = 50, CancellationToken cancellationToken = default);

    /// <summary>
    /// Deletes old completed/failed jobs (cleanup)
    /// </summary>
    Task<int> DeleteOldJobsAsync(DateTime olderThan, CancellationToken cancellationToken = default);
}
