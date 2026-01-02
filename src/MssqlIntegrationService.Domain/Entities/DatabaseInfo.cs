namespace MssqlIntegrationService.Domain.Entities;

public class DatabaseInfo
{
    public string? ServerName { get; set; }
    public string? DatabaseName { get; set; }
    public string? ServerVersion { get; set; }
    public string? Edition { get; set; }
    public List<TableInfo> Tables { get; set; } = [];
}

public class TableInfo
{
    public string? SchemaName { get; set; }
    public string? TableName { get; set; }
    public long RowCount { get; set; }
    public List<ColumnInfo> Columns { get; set; } = [];
}

public class ColumnInfo
{
    public string? ColumnName { get; set; }
    public string? DataType { get; set; }
    public int? MaxLength { get; set; }
    public bool IsNullable { get; set; }
    public bool IsPrimaryKey { get; set; }
    public bool IsIdentity { get; set; }
}
