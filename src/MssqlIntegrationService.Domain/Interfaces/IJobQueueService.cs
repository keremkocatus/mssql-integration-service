namespace MssqlIntegrationService.Domain.Interfaces;

/// <summary>
/// Interface for job queue operations (fire-and-forget)
/// </summary>
public interface IJobQueueService
{
    /// <summary>
    /// Enqueues a job ID for background processing
    /// </summary>
    ValueTask EnqueueAsync(string jobId, CancellationToken cancellationToken = default);
}
