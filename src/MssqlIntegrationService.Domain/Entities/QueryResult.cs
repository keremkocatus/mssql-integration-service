namespace MssqlIntegrationService.Domain.Entities;

public class QueryResult
{
    public IEnumerable<IDictionary<string, object?>> Rows { get; set; } = [];
    public int RowCount { get; set; }
    public int AffectedRows { get; set; }
    public long ExecutionTimeMs { get; set; }
}
