using MssqlIntegrationService.Domain.Common;

namespace MssqlIntegrationService.Domain.Interfaces;

/// <summary>
/// Service for retrieving database schema information
/// </summary>
public interface ISchemaService
{
    /// <summary>
    /// Retrieves key columns from a table schema.
    /// Priority: 1. Primary Key columns, 2. First Unique Index columns
    /// </summary>
    /// <param name="connectionString">Database connection string</param>
    /// <param name="tableName">Table name (can include schema, e.g., "dbo.Users")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>List of key column names or error if no key found</returns>
    Task<Result<List<string>>> GetKeyColumnsAsync(
        string connectionString,
        string tableName,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Retrieves all indexes from a table including their columns and sort orders.
    /// </summary>
    /// <param name="connectionString">Database connection string</param>
    /// <param name="tableName">Table name (can include schema, e.g., "dbo.Users")</param>
    /// <param name="cancellationToken">Cancellation token</param>
    /// <returns>List of index information</returns>
    Task<Result<List<IndexInfo>>> GetIndexesAsync(
        string connectionString,
        string tableName,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Represents index information from database schema
/// </summary>
public class IndexInfo
{
    public required string Name { get; set; }
    public bool IsPrimaryKey { get; set; }
    public bool IsUnique { get; set; }
    public required List<string> Columns { get; set; }
    public required List<string> ColumnOrders { get; set; }
}
