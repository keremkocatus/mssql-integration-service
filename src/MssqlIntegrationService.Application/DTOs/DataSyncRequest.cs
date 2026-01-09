namespace MssqlIntegrationService.Application.DTOs;

public class DataSyncRequest
{
    /// <summary>
    /// Source database configuration
    /// </summary>
    public required SyncSourceConfig Source { get; set; }
    
    /// <summary>
    /// Target database configuration
    /// </summary>
    public required SyncTargetConfig Target { get; set; }
    
    /// <summary>
    /// Sync options
    /// </summary>
    public SyncOptionsDto? Options { get; set; }
}

public class SyncSourceConfig
{
    /// <summary>
    /// Connection string for source database
    /// </summary>
    public required string ConnectionString { get; set; }
    
    /// <summary>
    /// SELECT query to fetch data from source
    /// </summary>
    public required string Query { get; set; }
}

public class SyncTargetConfig
{
    /// <summary>
    /// Connection string for target database
    /// </summary>
    public required string ConnectionString { get; set; }
    
    /// <summary>
    /// Target table name (can include schema, e.g., "dbo.Users")
    /// </summary>
    public required string TableName { get; set; }
    
    /// <summary>
    /// Key columns used for matching records to delete.
    /// These columns will be used in JOIN condition.
    /// If not provided, Primary Key or Unique Index columns will be auto-detected from target table schema.
    /// </summary>
    public List<string>? KeyColumns { get; set; }
}

public class SyncOptionsDto
{
    /// <summary>
    /// Batch size for bulk operations (default: 1000)
    /// </summary>
    public int BatchSize { get; set; } = 1000;
    
    /// <summary>
    /// Timeout in seconds (default: 300)
    /// </summary>
    public int Timeout { get; set; } = 300;
    
    /// <summary>
    /// Use transaction for atomic operation (default: true)
    /// </summary>
    public bool UseTransaction { get; set; } = true;
    
    /// <summary>
    /// If true, deletes ALL rows from target before insert (ignores KeyColumns for delete)
    /// </summary>
    public bool DeleteAllBeforeInsert { get; set; } = false;
    
    /// <summary>
    /// Column mappings from source to target (optional)
    /// Key: Source column name, Value: Target column name
    /// </summary>
    public Dictionary<string, string>? ColumnMappings { get; set; }
}

public class DataSyncResponse
{
    public bool Success { get; set; }
    public int TotalRowsRead { get; set; }
    public int RowsDeleted { get; set; }
    public int RowsInserted { get; set; }
    public long ExecutionTimeMs { get; set; }
    public string? SourceQuery { get; set; }
    public string? TargetTable { get; set; }
    public List<string>? KeyColumns { get; set; }
    public List<string>? Warnings { get; set; }
    public string? ErrorMessage { get; set; }
}
