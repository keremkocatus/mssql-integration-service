namespace MssqlIntegrationService.Application.DTOs;

public class BulkInsertRequest
{
    public required string ConnectionString { get; set; }
    public required string TableName { get; set; }
    public required List<Dictionary<string, object?>> Data { get; set; }
    public BulkInsertOptionsDto? Options { get; set; }
}

public class BulkInsertOptionsDto
{
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool UseTransaction { get; set; } = true;
}

public class BulkInsertResponse
{
    public bool Success { get; set; }
    public int TotalRowsInserted { get; set; }
    public long ExecutionTimeMs { get; set; }
    public string? ErrorMessage { get; set; }
}
