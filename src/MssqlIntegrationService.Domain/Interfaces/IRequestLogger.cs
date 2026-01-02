using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Domain.Interfaces;

/// <summary>
/// Interface for request logging implementations
/// </summary>
public interface IRequestLogger
{
    /// <summary>
    /// Logger type name
    /// </summary>
    string LoggerType { get; }
    
    /// <summary>
    /// Check if logger is enabled
    /// </summary>
    bool IsEnabled { get; }
    
    /// <summary>
    /// Log a request
    /// </summary>
    Task LogAsync(RequestLog log, CancellationToken cancellationToken = default);
}
