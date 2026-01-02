using System.Text.Json.Serialization;

namespace MssqlIntegrationService.Application.DTOs;

public class StoredProcedureRequest
{
    public required string ProcedureName { get; set; }
    
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public Dictionary<string, object?>? Parameters { get; set; }
    
    public int? Timeout { get; set; }
}
