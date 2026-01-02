using System.Text.Json.Serialization;

namespace MssqlIntegrationService.Application.DTOs;

public class QueryResponse
{
    public bool Success { get; set; }
    
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public IEnumerable<IDictionary<string, object?>>? Data { get; set; }
    
    public int RowCount { get; set; }
    
    public int AffectedRows { get; set; }
    
    public long ExecutionTimeMs { get; set; }
    
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? ErrorMessage { get; set; }
    
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? ErrorCode { get; set; }
}
