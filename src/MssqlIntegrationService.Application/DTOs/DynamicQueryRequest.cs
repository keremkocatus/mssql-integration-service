namespace MssqlIntegrationService.Application.DTOs;

public class DynamicQueryRequest
{
    public required string ConnectionString { get; set; }
    public required string Query { get; set; }
    public Dictionary<string, object?>? Parameters { get; set; }
    public int? Timeout { get; set; }
}

public class DynamicStoredProcedureRequest
{
    public required string ConnectionString { get; set; }
    public required string ProcedureName { get; set; }
    public Dictionary<string, object?>? Parameters { get; set; }
    public int? Timeout { get; set; }
}

public class TestConnectionRequest
{
    public required string ConnectionString { get; set; }
}

public class TestConnectionResponse
{
    public bool Success { get; set; }
    public string? Message { get; set; }
    public long ResponseTimeMs { get; set; }
}
