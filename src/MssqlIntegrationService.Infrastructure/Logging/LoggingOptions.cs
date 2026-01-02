namespace MssqlIntegrationService.Infrastructure.Logging;

/// <summary>
/// Main logging configuration
/// </summary>
public class LoggingOptions
{
    public const string SectionName = "RequestLogging";
    
    /// <summary>
    /// Enable/disable all request logging
    /// </summary>
    public bool Enabled { get; set; } = false;
    
    /// <summary>
    /// Log request body (may contain sensitive data)
    /// </summary>
    public bool LogRequestBody { get; set; } = true;
    
    /// <summary>
    /// Log response body
    /// </summary>
    public bool LogResponseBody { get; set; } = false;
    
    /// <summary>
    /// Log request headers
    /// </summary>
    public bool LogHeaders { get; set; } = false;
    
    /// <summary>
    /// Max body length to log (prevents huge payloads)
    /// </summary>
    public int MaxBodyLength { get; set; } = 10000;
    
    /// <summary>
    /// Paths to exclude from logging (e.g., "/api/health")
    /// </summary>
    public List<string> ExcludePaths { get; set; } = new() { "/api/health", "/swagger" };
    
    /// <summary>
    /// Console logging options
    /// </summary>
    public ConsoleLoggingOptions Console { get; set; } = new();
    
    /// <summary>
    /// File logging options
    /// </summary>
    public FileLoggingOptions File { get; set; } = new();
    
    /// <summary>
    /// MongoDB logging options
    /// </summary>
    public MongoLoggingOptions MongoDB { get; set; } = new();
}

/// <summary>
/// Console/Terminal logging configuration
/// </summary>
public class ConsoleLoggingOptions
{
    /// <summary>
    /// Enable console logging
    /// </summary>
    public bool Enabled { get; set; } = true;
    
    /// <summary>
    /// Use colored output
    /// </summary>
    public bool UseColors { get; set; } = true;
    
    /// <summary>
    /// Output format: "Simple" or "Detailed"
    /// </summary>
    public string Format { get; set; } = "Simple";
}

/// <summary>
/// File logging configuration
/// </summary>
public class FileLoggingOptions
{
    /// <summary>
    /// Enable file logging
    /// </summary>
    public bool Enabled { get; set; } = false;
    
    /// <summary>
    /// Log file directory (relative or absolute)
    /// </summary>
    public string Directory { get; set; } = "logs";
    
    /// <summary>
    /// File format: "txt" or "json"
    /// </summary>
    public string Format { get; set; } = "json";
    
    /// <summary>
    /// File name pattern. {date} will be replaced with current date
    /// </summary>
    public string FileNamePattern { get; set; } = "request-log-{date}";
    
    /// <summary>
    /// Rolling interval: "Daily", "Hourly"
    /// </summary>
    public string RollingInterval { get; set; } = "Daily";
    
    /// <summary>
    /// Max file size in MB before rolling (0 = no limit)
    /// </summary>
    public int MaxFileSizeMB { get; set; } = 100;
    
    /// <summary>
    /// Number of days to retain log files (0 = keep forever)
    /// </summary>
    public int RetentionDays { get; set; } = 30;
}

/// <summary>
/// MongoDB logging configuration
/// </summary>
public class MongoLoggingOptions
{
    /// <summary>
    /// Enable MongoDB logging
    /// </summary>
    public bool Enabled { get; set; } = false;
    
    /// <summary>
    /// MongoDB connection string
    /// </summary>
    public string ConnectionString { get; set; } = string.Empty;
    
    /// <summary>
    /// Database name
    /// </summary>
    public string DatabaseName { get; set; } = "MssqlIntegrationService";
    
    /// <summary>
    /// Collection name
    /// </summary>
    public string CollectionName { get; set; } = "RequestLogs";
    
    /// <summary>
    /// TTL (Time To Live) in days. Documents older than this will be auto-deleted (0 = keep forever)
    /// </summary>
    public int TtlDays { get; set; } = 30;
}
