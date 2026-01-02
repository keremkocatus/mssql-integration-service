using System.Text.Json.Serialization;

namespace MssqlIntegrationService.Application.DTOs;

public class QueryRequest
{
    public required string Query { get; set; }
    
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, object?>? Parameters { get; set; }
    
    public int? Timeout { get; set; }
}
