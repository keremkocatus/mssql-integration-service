using System.Data;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Microsoft.Data.SqlClient;
using MongoDB.Bson;
using MongoDB.Driver;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;

namespace MssqlIntegrationService.Infrastructure.Services;

public class MongoToMssqlService : IMongoToMssqlService
{
    public async Task<Result<MongoToMssqlResult>> TransferAsync(
        string mongoConnectionString,
        string mongoDatabaseName,
        string mongoCollection,
        string? mongoFilter,
        string mssqlConnectionString,
        string targetTable,
        MongoToMssqlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new MongoToMssqlOptions();
        var stopwatch = Stopwatch.StartNew();
        var result = new MongoToMssqlResult
        {
            SourceCollection = mongoCollection,
            TargetTable = targetTable
        };

        try
        {
            // Connect to MongoDB
            var mongoClient = new MongoClient(mongoConnectionString);
            var database = mongoClient.GetDatabase(mongoDatabaseName);
            var collection = database.GetCollection<BsonDocument>(mongoCollection);

            // Build filter
            var filter = string.IsNullOrWhiteSpace(mongoFilter)
                ? FilterDefinition<BsonDocument>.Empty
                : BsonDocument.Parse(mongoFilter);

            // Get documents
            var documents = await collection.Find(filter).ToListAsync(cancellationToken);
            result.TotalDocumentsRead = documents.Count;

            if (documents.Count == 0)
            {
                result.Warnings.Add("No documents found matching the filter");
                stopwatch.Stop();
                result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;
                return Result<MongoToMssqlResult>.Success(result);
            }

            // Process and transfer
            await TransferDocumentsToMssql(documents, mssqlConnectionString, targetTable, options, result, cancellationToken);

            stopwatch.Stop();
            result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;
            return Result<MongoToMssqlResult>.Success(result);
        }
        catch (MongoException ex)
        {
            stopwatch.Stop();
            return Result<MongoToMssqlResult>.Failure($"MongoDB Error: {ex.Message}");
        }
        catch (SqlException ex)
        {
            stopwatch.Stop();
            return Result<MongoToMssqlResult>.Failure($"MSSQL Error: {ex.Message}", ex.Number);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return Result<MongoToMssqlResult>.Failure(ex.Message);
        }
    }

    public async Task<Result<MongoToMssqlResult>> TransferWithAggregationAsync(
        string mongoConnectionString,
        string mongoDatabaseName,
        string mongoCollection,
        string aggregationPipeline,
        string mssqlConnectionString,
        string targetTable,
        MongoToMssqlOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        options ??= new MongoToMssqlOptions();
        var stopwatch = Stopwatch.StartNew();
        var result = new MongoToMssqlResult
        {
            SourceCollection = mongoCollection,
            TargetTable = targetTable
        };

        try
        {
            // Connect to MongoDB
            var mongoClient = new MongoClient(mongoConnectionString);
            var database = mongoClient.GetDatabase(mongoDatabaseName);
            var collection = database.GetCollection<BsonDocument>(mongoCollection);

            // Parse aggregation pipeline - deserialize JSON array to BsonDocument array
            var pipelineArray = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonArray>(aggregationPipeline);
            var pipelineStages = pipelineArray.Select(stage => (BsonDocument)stage).ToArray();
            var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(pipelineStages);

            // Execute aggregation
            var documents = await collection.Aggregate(pipeline).ToListAsync(cancellationToken);
            result.TotalDocumentsRead = documents.Count;

            if (documents.Count == 0)
            {
                result.Warnings.Add("No documents returned from aggregation pipeline");
                stopwatch.Stop();
                result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;
                return Result<MongoToMssqlResult>.Success(result);
            }

            // Process and transfer
            await TransferDocumentsToMssql(documents, mssqlConnectionString, targetTable, options, result, cancellationToken);

            stopwatch.Stop();
            result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;
            return Result<MongoToMssqlResult>.Success(result);
        }
        catch (MongoException ex)
        {
            stopwatch.Stop();
            return Result<MongoToMssqlResult>.Failure($"MongoDB Error: {ex.Message}");
        }
        catch (SqlException ex)
        {
            stopwatch.Stop();
            return Result<MongoToMssqlResult>.Failure($"MSSQL Error: {ex.Message}", ex.Number);
        }
        catch (Exception ex)
        {
            stopwatch.Stop();
            return Result<MongoToMssqlResult>.Failure(ex.Message);
        }
    }

    private async Task TransferDocumentsToMssql(
        List<BsonDocument> documents,
        string mssqlConnectionString,
        string targetTable,
        MongoToMssqlOptions options,
        MongoToMssqlResult result,
        CancellationToken cancellationToken)
    {
        // Convert documents to DataTable
        var (dataTable, failedCount) = ConvertToDataTable(documents, options, result);
        result.FailedDocuments = failedCount;

        if (dataTable.Rows.Count == 0)
        {
            result.Warnings.Add("No valid rows to transfer after conversion");
            return;
        }

        // Connect to MSSQL
        await using var connection = new SqlConnection(mssqlConnectionString);
        await connection.OpenAsync(cancellationToken);

        SqlTransaction? transaction = null;
        if (options.UseTransaction)
        {
            transaction = connection.BeginTransaction();
        }

        try
        {
            var safeTableName = SqlValidator.SafeTableName(targetTable);

            // Create table if needed
            if (options.CreateTableIfNotExists)
            {
                await CreateTableIfNotExistsAsync(connection, transaction, targetTable, dataTable, options.Timeout, cancellationToken);
                result.Warnings.Add($"Table '{targetTable}' created or verified");
            }

            // Truncate if needed
            if (options.TruncateTargetTable)
            {
                await using var truncateCmd = new SqlCommand($"TRUNCATE TABLE {safeTableName}", connection, transaction);
                truncateCmd.CommandTimeout = options.Timeout;
                await truncateCmd.ExecuteNonQueryAsync(cancellationToken);
                result.Warnings.Add($"Table '{targetTable}' truncated");
            }

            // Bulk insert
            using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction);
            bulkCopy.DestinationTableName = safeTableName;
            bulkCopy.BatchSize = options.BatchSize;
            bulkCopy.BulkCopyTimeout = options.Timeout;

            // Map columns
            foreach (DataColumn col in dataTable.Columns)
            {
                var targetCol = options.FieldMappings?.GetValueOrDefault(col.ColumnName) ?? col.ColumnName;
                bulkCopy.ColumnMappings.Add(col.ColumnName, targetCol);
            }

            await bulkCopy.WriteToServerAsync(dataTable, cancellationToken);
            result.TotalRowsWritten = dataTable.Rows.Count;

            // Commit transaction
            if (transaction != null)
            {
                await transaction.CommitAsync(cancellationToken);
            }
        }
        catch
        {
            if (transaction != null)
            {
                await transaction.RollbackAsync(cancellationToken);
            }
            throw;
        }
    }

    private (DataTable dataTable, int failedCount) ConvertToDataTable(
        List<BsonDocument> documents,
        MongoToMssqlOptions options,
        MongoToMssqlResult result)
    {
        var dataTable = new DataTable();
        var failedCount = 0;
        var columnTypes = new Dictionary<string, Type>();

        // First pass: determine schema from all documents
        foreach (var doc in documents)
        {
            var flatDoc = options.FlattenNestedDocuments
                ? FlattenDocument(doc, options.FlattenSeparator, options.ArrayHandling)
                : ConvertToDictionary(doc, options.ArrayHandling);

            foreach (var kvp in flatDoc)
            {
                var fieldName = kvp.Key;

                // Skip _id by default, unless explicitly included
                if (fieldName == "_id" && options.IncludeFields?.Contains("_id") != true)
                    continue;

                // Apply include/exclude filters
                if (options.IncludeFields != null && options.IncludeFields.Count > 0 && !options.IncludeFields.Contains(fieldName))
                    continue;
                if (options.ExcludeFields != null && options.ExcludeFields.Contains(fieldName))
                    continue;

                if (!columnTypes.ContainsKey(fieldName) && kvp.Value != null)
                {
                    columnTypes[fieldName] = kvp.Value.GetType();
                }
            }
        }

        // Create columns
        foreach (var kvp in columnTypes)
        {
            var columnName = options.FieldMappings?.GetValueOrDefault(kvp.Key) ?? kvp.Key;
            // Validate column name
            if (!SqlValidator.IsValidColumnName(columnName))
            {
                result.Warnings.Add($"Skipping invalid column name: '{columnName}'");
                continue;
            }
            dataTable.Columns.Add(new DataColumn(kvp.Key, kvp.Value) { AllowDBNull = true });
        }

        // Second pass: populate data
        foreach (var doc in documents)
        {
            try
            {
                var flatDoc = options.FlattenNestedDocuments
                    ? FlattenDocument(doc, options.FlattenSeparator, options.ArrayHandling)
                    : ConvertToDictionary(doc, options.ArrayHandling);

                var row = dataTable.NewRow();
                foreach (DataColumn col in dataTable.Columns)
                {
                    if (flatDoc.TryGetValue(col.ColumnName, out var value))
                    {
                        row[col.ColumnName] = value ?? DBNull.Value;
                    }
                    else
                    {
                        row[col.ColumnName] = DBNull.Value;
                    }
                }
                dataTable.Rows.Add(row);
            }
            catch
            {
                failedCount++;
            }
        }

        return (dataTable, failedCount);
    }

    private Dictionary<string, object?> FlattenDocument(BsonDocument doc, string separator, string arrayHandling, string prefix = "")
    {
        var result = new Dictionary<string, object?>();

        foreach (var element in doc)
        {
            var key = string.IsNullOrEmpty(prefix) ? element.Name : $"{prefix}{separator}{element.Name}";

            if (element.Value.IsBsonDocument)
            {
                var nested = FlattenDocument(element.Value.AsBsonDocument, separator, arrayHandling, key);
                foreach (var kvp in nested)
                {
                    result[kvp.Key] = kvp.Value;
                }
            }
            else if (element.Value.IsBsonArray)
            {
                result[key] = HandleArray(element.Value.AsBsonArray, arrayHandling);
            }
            else
            {
                result[key] = ConvertBsonValue(element.Value);
            }
        }

        return result;
    }

    private Dictionary<string, object?> ConvertToDictionary(BsonDocument doc, string arrayHandling)
    {
        var result = new Dictionary<string, object?>();

        foreach (var element in doc)
        {
            if (element.Value.IsBsonDocument)
            {
                result[element.Name] = element.Value.ToJson();
            }
            else if (element.Value.IsBsonArray)
            {
                result[element.Name] = HandleArray(element.Value.AsBsonArray, arrayHandling);
            }
            else
            {
                result[element.Name] = ConvertBsonValue(element.Value);
            }
        }

        return result;
    }

    private object? HandleArray(BsonArray array, string arrayHandling)
    {
        return arrayHandling.ToLower() switch
        {
            "skip" => null,
            "firstelement" => array.Count > 0 ? ConvertBsonValue(array[0]) : null,
            _ => array.ToJson() // Serialize (default)
        };
    }

    private object? ConvertBsonValue(BsonValue value)
    {
        if (value.IsBsonNull) return null;
        
        return value.BsonType switch
        {
            BsonType.ObjectId => value.AsObjectId.ToString(),
            BsonType.String => value.AsString,
            BsonType.Int32 => value.AsInt32,
            BsonType.Int64 => value.AsInt64,
            BsonType.Double => value.AsDouble,
            BsonType.Boolean => value.AsBoolean,
            BsonType.DateTime => value.ToUniversalTime(),
            BsonType.Decimal128 => value.AsDecimal,
            BsonType.Binary => value.AsBsonBinaryData.Bytes,
            BsonType.Timestamp => value.AsBsonTimestamp.ToUniversalTime(),
            _ => value.ToString()
        };
    }

    private async Task CreateTableIfNotExistsAsync(
        SqlConnection connection,
        SqlTransaction? transaction,
        string tableName,
        DataTable dataTable,
        int timeout,
        CancellationToken cancellationToken)
    {
        var safeTableName = SqlValidator.SafeTableName(tableName);
        
        // Check if table exists
        var checkSql = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName";
        await using var checkCmd = new SqlCommand(checkSql, connection, transaction);
        checkCmd.Parameters.AddWithValue("@tableName", tableName.Contains('.') ? tableName.Split('.')[1] : tableName);
        checkCmd.CommandTimeout = timeout;
        
        var exists = (int)await checkCmd.ExecuteScalarAsync(cancellationToken)! > 0;
        if (exists) return;

        // Create table
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE {safeTableName} (");

        var columnDefs = new List<string>();
        foreach (DataColumn col in dataTable.Columns)
        {
            var safeColName = SqlValidator.SafeIdentifier(col.ColumnName);
            var sqlType = GetSqlType(col.DataType);
            columnDefs.Add($"    {safeColName} {sqlType} NULL");
        }

        sb.AppendLine(string.Join(",\n", columnDefs));
        sb.AppendLine(")");

        await using var createCmd = new SqlCommand(sb.ToString(), connection, transaction);
        createCmd.CommandTimeout = timeout;
        await createCmd.ExecuteNonQueryAsync(cancellationToken);
    }

    private static string GetSqlType(Type type)
    {
        var underlyingType = Nullable.GetUnderlyingType(type) ?? type;
        
        return underlyingType.Name switch
        {
            "String" => "NVARCHAR(MAX)",
            "Int32" => "INT",
            "Int64" => "BIGINT",
            "Int16" => "SMALLINT",
            "Byte" => "TINYINT",
            "Boolean" => "BIT",
            "DateTime" => "DATETIME2",
            "DateTimeOffset" => "DATETIMEOFFSET",
            "Decimal" => "DECIMAL(18,4)",
            "Double" => "FLOAT",
            "Single" => "REAL",
            "Guid" => "UNIQUEIDENTIFIER",
            "Byte[]" => "VARBINARY(MAX)",
            _ => "NVARCHAR(MAX)"
        };
    }
}
