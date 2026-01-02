namespace MssqlIntegrationService.Domain.Entities;

/// <summary>
/// HTTP Request/Response log entity
/// </summary>
public class RequestLog
{
    public string Id { get; set; } = Guid.NewGuid().ToString();
    public DateTime Timestamp { get; set; } = DateTime.UtcNow;
    
    // Request Info
    public string HttpMethod { get; set; } = string.Empty;
    public string Path { get; set; } = string.Empty;
    public string QueryString { get; set; } = string.Empty;
    public string? RequestBody { get; set; }
    public Dictionary<string, string> RequestHeaders { get; set; } = new();
    
    // Response Info
    public int StatusCode { get; set; }
    public string? ResponseBody { get; set; }
    public long ResponseTimeMs { get; set; }
    
    // Client Info
    public string? ClientIp { get; set; }
    public string? UserAgent { get; set; }
    
    // Error Info (if any)
    public string? ErrorMessage { get; set; }
    public string? StackTrace { get; set; }
}
