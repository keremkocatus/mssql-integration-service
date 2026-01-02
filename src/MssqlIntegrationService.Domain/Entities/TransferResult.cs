namespace MssqlIntegrationService.Domain.Entities;

public class TransferResult
{
    public int TotalRowsRead { get; set; }
    public int TotalRowsWritten { get; set; }
    public long ExecutionTimeMs { get; set; }
    public string? SourceQuery { get; set; }
    public string? TargetTable { get; set; }
    public List<string> Warnings { get; set; } = [];
}

public class TransferOptions
{
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool TruncateTargetTable { get; set; } = false;
    public bool CreateTableIfNotExists { get; set; } = false;
    public Dictionary<string, string>? ColumnMappings { get; set; }
    public bool UseTransaction { get; set; } = true;
}

public class BulkInsertOptions
{
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool UseTransaction { get; set; } = true;
}
