namespace MssqlIntegrationService.Domain.Entities;

/// <summary>
/// Result of MongoDB to MSSQL transfer operation
/// </summary>
public class MongoToMssqlResult
{
    /// <summary>
    /// MongoDB source details
    /// </summary>
    public string SourceCollection { get; set; } = string.Empty;
    
    /// <summary>
    /// Target MSSQL table
    /// </summary>
    public string TargetTable { get; set; } = string.Empty;
    
    /// <summary>
    /// Total documents read from MongoDB
    /// </summary>
    public long TotalDocumentsRead { get; set; }
    
    /// <summary>
    /// Total rows written to MSSQL
    /// </summary>
    public long TotalRowsWritten { get; set; }
    
    /// <summary>
    /// Documents that failed to transfer (schema mismatch, etc.)
    /// </summary>
    public int FailedDocuments { get; set; }
    
    /// <summary>
    /// Execution time in milliseconds
    /// </summary>
    public long ExecutionTimeMs { get; set; }
    
    /// <summary>
    /// Warnings during transfer
    /// </summary>
    public List<string> Warnings { get; set; } = new();
}

/// <summary>
/// Options for MongoDB to MSSQL transfer
/// </summary>
public class MongoToMssqlOptions
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
    /// Field mappings from MongoDB to MSSQL (optional)
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
    /// e.g., { "address": { "city": "X" } } → address_city
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

    /// <summary>
    /// Number of documents to sample for schema inference (default: 100).
    /// Higher values give more accurate schema but use more memory during initialization.
    /// </summary>
    public int SchemaSampleSize { get; set; } = 100;
}
