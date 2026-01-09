using Microsoft.Data.SqlClient;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Infrastructure.Services;

/// <summary>
/// Service for retrieving database schema information
/// </summary>
public class SchemaService : ISchemaService
{
    /// <summary>
    /// Retrieves key columns from a table schema.
    /// Priority: 1. Primary Key columns, 2. First Unique Index columns
    /// </summary>
    public async Task<Result<List<string>>> GetKeyColumnsAsync(
        string connectionString,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Parse table name (handle schema.table format)
            var tableParts = tableName.Split('.');
            var schemaName = tableParts.Length > 1 ? tableParts[0].Trim('[', ']') : "dbo";
            var tableNameOnly = tableParts.Length > 1 ? tableParts[1].Trim('[', ']') : tableParts[0].Trim('[', ']');

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Query to get Primary Key or first Unique Index columns
            // Priority: Primary Key first, then Unique Index (ordered by index_id to get the first one)
            var keyColumnQuery = @"
                WITH KeyIndexes AS (
                    SELECT TOP 1
                        i.index_id,
                        i.is_primary_key,
                        i.is_unique,
                        i.name AS IndexName
                    FROM sys.indexes i
                    INNER JOIN sys.tables t ON i.object_id = t.object_id
                    INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                    WHERE s.name = @schemaName 
                        AND t.name = @tableName
                        AND (i.is_primary_key = 1 OR i.is_unique = 1)
                        AND i.type IN (1, 2) -- Clustered and Non-clustered
                    ORDER BY 
                        i.is_primary_key DESC,  -- Primary key first
                        i.index_id ASC          -- Then first unique index
                )
                SELECT 
                    c.name AS ColumnName,
                    ki.is_primary_key AS IsPrimaryKey,
                    ki.IndexName
                FROM KeyIndexes ki
                INNER JOIN sys.indexes i ON ki.index_id = i.index_id
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                WHERE s.name = @schemaName 
                    AND t.name = @tableName
                    AND ic.is_included_column = 0 -- Only key columns
                ORDER BY ic.key_ordinal";

            var keyColumns = new List<string>();

            await using (var cmd = new SqlCommand(keyColumnQuery, connection))
            {
                cmd.Parameters.AddWithValue("@schemaName", schemaName);
                cmd.Parameters.AddWithValue("@tableName", tableNameOnly);
                cmd.CommandTimeout = 30;

                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    keyColumns.Add(reader.GetString(0));
                }
            }

            if (keyColumns.Count == 0)
            {
                return Result<List<string>>.Failure(
                    $"No Primary Key or Unique Index found on table '{tableName}'. " +
                    "Please provide KeyColumns manually or add a Primary Key/Unique Index to the target table.");
            }

            return Result<List<string>>.Success(keyColumns);
        }
        catch (SqlException ex)
        {
            return Result<List<string>>.Failure($"Failed to retrieve key columns from schema: {ex.Message}", ex.Number);
        }
        catch (Exception ex)
        {
            return Result<List<string>>.Failure($"Failed to retrieve key columns from schema: {ex.Message}");
        }
    }

    /// <summary>
    /// Retrieves all indexes from a table including their columns and sort orders.
    /// Priority: Primary Key first, then unique indexes, then non-unique indexes.
    /// </summary>
    public async Task<Result<List<IndexInfo>>> GetIndexesAsync(
        string connectionString,
        string tableName,
        CancellationToken cancellationToken = default)
    {
        try
        {
            // Parse table name (handle schema.table format)
            var tableParts = tableName.Split('.');
            var schemaName = tableParts.Length > 1 ? tableParts[0].Trim('[', ']') : "dbo";
            var tableNameOnly = tableParts.Length > 1 ? tableParts[1].Trim('[', ']') : tableParts[0].Trim('[', ']');

            await using var connection = new SqlConnection(connectionString);
            await connection.OpenAsync(cancellationToken);

            // Query to get indexes with their columns, ordered by priority
            var indexQuery = @"
                SELECT 
                    i.name AS IndexName,
                    i.is_primary_key AS IsPrimaryKey,
                    i.is_unique AS IsUnique,
                    STRING_AGG(c.name, ',') WITHIN GROUP (ORDER BY ic.key_ordinal) AS Columns,
                    STRING_AGG(
                        CASE WHEN ic.is_descending_key = 1 THEN 'DESC' ELSE 'ASC' END, 
                        ','
                    ) WITHIN GROUP (ORDER BY ic.key_ordinal) AS ColumnOrders
                FROM sys.indexes i
                INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id
                INNER JOIN sys.tables t ON i.object_id = t.object_id
                INNER JOIN sys.schemas s ON t.schema_id = s.schema_id
                WHERE s.name = @schemaName 
                    AND t.name = @tableName
                    AND i.type IN (1, 2) -- Clustered and Non-clustered
                    AND ic.is_included_column = 0 -- Only key columns, not included columns
                GROUP BY i.name, i.is_primary_key, i.is_unique, i.index_id
                ORDER BY 
                    i.is_primary_key DESC,  -- Primary key first
                    i.is_unique DESC,       -- Then unique indexes
                    i.index_id ASC";        // Then by index order

            var indexes = new List<IndexInfo>();

            await using (var cmd = new SqlCommand(indexQuery, connection))
            {
                cmd.Parameters.AddWithValue("@schemaName", schemaName);
                cmd.Parameters.AddWithValue("@tableName", tableNameOnly);
                cmd.CommandTimeout = 30;

                await using var reader = await cmd.ExecuteReaderAsync(cancellationToken);
                while (await reader.ReadAsync(cancellationToken))
                {
                    indexes.Add(new IndexInfo
                    {
                        Name = reader.GetString(0),
                        IsPrimaryKey = reader.GetBoolean(1),
                        IsUnique = reader.GetBoolean(2),
                        Columns = reader.GetString(3).Split(',').ToList(),
                        ColumnOrders = reader.GetString(4).Split(',').ToList()
                    });
                }
            }

            return Result<List<IndexInfo>>.Success(indexes);
        }
        catch (SqlException ex)
        {
            return Result<List<IndexInfo>>.Failure($"Failed to retrieve indexes from schema: {ex.Message}", ex.Number);
        }
        catch (Exception ex)
        {
            return Result<List<IndexInfo>>.Failure($"Failed to retrieve indexes from schema: {ex.Message}");
        }
    }
}
