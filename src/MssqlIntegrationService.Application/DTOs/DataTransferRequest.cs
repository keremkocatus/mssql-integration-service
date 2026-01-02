namespace MssqlIntegrationService.Application.DTOs;

public class DataTransferRequest
{
    public required SourceConfig Source { get; set; }
    public required TargetConfig Target { get; set; }
    public TransferOptionsDto? Options { get; set; }
}

public class SourceConfig
{
    public required string ConnectionString { get; set; }
    public required string Query { get; set; }
}

public class TargetConfig
{
    public required string ConnectionString { get; set; }
    public required string TableName { get; set; }
}

public class TransferOptionsDto
{
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool TruncateTargetTable { get; set; } = false;
    public bool CreateTableIfNotExists { get; set; } = false;
    public Dictionary<string, string>? ColumnMappings { get; set; }
    public bool UseTransaction { get; set; } = true;
}

public class DataTransferResponse
{
    public bool Success { get; set; }
    public int TotalRowsRead { get; set; }
    public int TotalRowsWritten { get; set; }
    public long ExecutionTimeMs { get; set; }
    public string? SourceQuery { get; set; }
    public string? TargetTable { get; set; }
    public List<string>? Warnings { get; set; }
    public string? ErrorMessage { get; set; }
}
