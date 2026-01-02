namespace MssqlIntegrationService.Application.DTOs;

public class DatabaseInfoRequest
{
    public required string ConnectionString { get; set; }
    public bool IncludeTables { get; set; } = true;
    public bool IncludeColumns { get; set; } = false;
}

public class DatabaseInfoResponse
{
    public bool Success { get; set; }
    public string? ServerName { get; set; }
    public string? DatabaseName { get; set; }
    public string? ServerVersion { get; set; }
    public string? Edition { get; set; }
    public List<TableInfoDto>? Tables { get; set; }
    public string? ErrorMessage { get; set; }
}

public class TableInfoDto
{
    public string? SchemaName { get; set; }
    public string? TableName { get; set; }
    public long RowCount { get; set; }
    public List<ColumnInfoDto>? Columns { get; set; }
}

public class ColumnInfoDto
{
    public string? ColumnName { get; set; }
    public string? DataType { get; set; }
    public int? MaxLength { get; set; }
    public bool IsNullable { get; set; }
    public bool IsPrimaryKey { get; set; }
    public bool IsIdentity { get; set; }
}
