namespace MssqlIntegrationService.Domain.Entities;

public class SyncResult
{
    public int TotalRowsRead { get; set; }
    public int RowsDeleted { get; set; }
    public int RowsInserted { get; set; }
    public long ExecutionTimeMs { get; set; }
    public string? SourceQuery { get; set; }
    public string? TargetTable { get; set; }
    public List<string> KeyColumns { get; set; } = [];
    public List<string> Warnings { get; set; } = [];
}

public class SyncOptions
{
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool UseTransaction { get; set; } = true;
    public bool DeleteAllBeforeInsert { get; set; } = false;
    public Dictionary<string, string>? ColumnMappings { get; set; }
}
