namespace MssqlIntegrationService.Application.DTOs;

/// <summary>
/// Request for MongoDB to MSSQL transfer
/// </summary>
public class MongoToMssqlRequest
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
}

/// <summary>
/// MongoDB source configuration
/// </summary>
public class MongoSourceConfig
{
    /// <summary>
    /// MongoDB connection string
    /// </summary>
    public required string ConnectionString { get; set; }
    
    /// <summary>
    /// MongoDB database name
    /// </summary>
    public required string DatabaseName { get; set; }
    
    /// <summary>
    /// MongoDB collection name
    /// </summary>
    public required string CollectionName { get; set; }
    
    /// <summary>
    /// MongoDB filter in JSON format (optional)
    /// Example: { "status": "active", "age": { "$gt": 18 } }
    /// </summary>
    public string? Filter { get; set; }
    
    /// <summary>
    /// MongoDB aggregation pipeline in JSON array format (optional)
    /// If provided, Filter is ignored
    /// Example: [{ "$match": { "status": "active" } }, { "$project": { "name": 1 } }]
    /// </summary>
    public string? AggregationPipeline { get; set; }
}

/// <summary>
/// MSSQL target configuration
/// </summary>
public class MssqlTargetConfig
{
    /// <summary>
    /// MSSQL connection string
    /// </summary>
    public required string ConnectionString { get; set; }
    
    /// <summary>
    /// Target table name (can include schema, e.g., "dbo.Users")
    /// </summary>
    public required string TableName { get; set; }
}

/// <summary>
/// Transfer options DTO
/// </summary>
public class MongoToMssqlOptionsDto
{
    /// <summary>
    /// Batch size for bulk insert (default: 1000)
    /// </summary>
    public int BatchSize { get; set; } = 1000;
    
    /// <summary>
    /// Timeout in seconds (default: 300)
    /// </summary>
    public int Timeout { get; set; } = 300;
    
    /// <summary>
    /// Truncate target table before insert (default: false)
    /// </summary>
    public bool TruncateTargetTable { get; set; } = false;
    
    /// <summary>
    /// Create table if not exists (default: false)
    /// </summary>
    public bool CreateTableIfNotExists { get; set; } = false;
    
    /// <summary>
    /// Use transaction for atomic operation (default: true)
    /// </summary>
    public bool UseTransaction { get; set; } = true;
    
    /// <summary>
    /// Field mappings from MongoDB to MSSQL
    /// Key: MongoDB field name, Value: MSSQL column name
    /// </summary>
    public Dictionary<string, string>? FieldMappings { get; set; }
    
    /// <summary>
    /// Fields to include (if empty, include all)
    /// </summary>
    public List<string>? IncludeFields { get; set; }
    
    /// <summary>
    /// Fields to exclude from transfer
    /// </summary>
    public List<string>? ExcludeFields { get; set; }
    
    /// <summary>
    /// Flatten nested documents (default: true)
    /// </summary>
    public bool FlattenNestedDocuments { get; set; } = true;
    
    /// <summary>
    /// Separator for flattened field names (default: "_")
    /// </summary>
    public string FlattenSeparator { get; set; } = "_";
    
    /// <summary>
    /// Handle arrays: "Serialize" (JSON string), "Skip", "FirstElement"
    /// </summary>
    public string ArrayHandling { get; set; } = "Serialize";
}

/// <summary>
/// Response for MongoDB to MSSQL transfer
/// </summary>
public class MongoToMssqlResponse
{
    public bool Success { get; set; }
    public string? ErrorMessage { get; set; }
    
    public string? SourceCollection { get; set; }
    public string? TargetTable { get; set; }
    
    public long TotalDocumentsRead { get; set; }
    public long TotalRowsWritten { get; set; }
    public int FailedDocuments { get; set; }
    
    public long ExecutionTimeMs { get; set; }
    public List<string> Warnings { get; set; } = new();
}
