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
using MssqlIntegrationService.Infrastructure.Data;

namespace MssqlIntegrationService.Infrastructure.Services;

public class MongoToMssqlService : IMongoToMssqlService
{
    /// <summary>
    /// Memory-efficient MongoDB to MSSQL transfer using cursor-based streaming.
    /// Documents are processed one at a time without loading entire collection into memory.
    /// </summary>
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

            // Get cursor for streaming (not ToListAsync!)
            using var cursor = await collection.Find(filter).ToCursorAsync(cancellationToken);

            // Check if there are any documents
            if (!await cursor.MoveNextAsync(cancellationToken) || !cursor.Current.Any())
            {
                result.Warnings.Add("No documents found matching the filter");
                stopwatch.Stop();
                result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;
                return Result<MongoToMssqlResult>.Success(result);
            }

            // Stream transfer using cursor
            await TransferWithCursorAsync(cursor, mssqlConnectionString, targetTable, options, result, cancellationToken);

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

    /// <summary>
    /// Memory-efficient MongoDB aggregation to MSSQL transfer using cursor-based streaming.
    /// </summary>
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

            // Parse aggregation pipeline
            var pipelineArray = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonArray>(aggregationPipeline);
            var pipelineStages = pipelineArray.Select(stage => (BsonDocument)stage).ToArray();
            var pipeline = PipelineDefinition<BsonDocument, BsonDocument>.Create(pipelineStages);

            // Aggregate() returns IAsyncCursor directly, no need for ToCursorAsync
            using var cursor = await collection.AggregateAsync(pipeline, cancellationToken: cancellationToken);

            // Check if there are any documents
            if (!await cursor.MoveNextAsync(cancellationToken) || !cursor.Current.Any())
            {
                result.Warnings.Add("No documents returned from aggregation pipeline");
                stopwatch.Stop();
                result.ExecutionTimeMs = stopwatch.ElapsedMilliseconds;
                return Result<MongoToMssqlResult>.Success(result);
            }

            // Stream transfer using cursor
            await TransferWithCursorAsync(cursor, mssqlConnectionString, targetTable, options, result, cancellationToken);

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

    /// <summary>
    /// Memory-efficient transfer using MongoDB cursor streaming.
    /// Schema is inferred from first batch, then data streams directly to SQL Server.
    /// </summary>
    private async Task TransferWithCursorAsync(
        IAsyncCursor<BsonDocument> cursor,
        string mssqlConnectionString,
        string targetTable,
        MongoToMssqlOptions options,
        MongoToMssqlResult result,
        CancellationToken cancellationToken)
    {
        // First, sample documents to determine schema (from current batch which we already loaded)
        var sampleDocs = cursor.Current.Take(options.SchemaSampleSize).ToList();
        var (columns, columnTypes) = InferSchemaFromSamples(sampleDocs, options, result);

        if (columns.Count == 0)
        {
            result.Warnings.Add("No valid columns found after schema inference");
            return;
        }

        // Create async enumerator that yields all documents including the first batch
        async IAsyncEnumerable<BsonDocument> StreamAllDocuments()
        {
            // Yield documents from first batch (already loaded during schema inference)
            foreach (var doc in cursor.Current)
            {
                yield return doc;
            }

            // Stream remaining batches
            while (await cursor.MoveNextAsync(cancellationToken))
            {
                foreach (var doc in cursor.Current)
                {
                    yield return doc;
                }
            }
        }

        // Prepare document converter function
        Dictionary<string, object?> ConvertDocument(BsonDocument doc)
        {
            return options.FlattenNestedDocuments
                ? FlattenDocument(doc, options.FlattenSeparator, options.ArrayHandling)
                : ConvertToDictionary(doc, options.ArrayHandling);
        }

        // Create streaming data reader
        var enumerator = StreamAllDocuments().GetAsyncEnumerator(cancellationToken);
        await using var bsonReader = new BsonDocumentDataReader(
            enumerator,
            columns,
            columnTypes,
            ConvertDocument,
            options.IncludeFields != null ? new HashSet<string>(options.IncludeFields) : null,
            options.ExcludeFields != null ? new HashSet<string>(options.ExcludeFields) : null);

        // Connect to MSSQL and stream data
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
                await CreateTableIfNotExistsAsync(connection, transaction, targetTable, columns, columnTypes, options, cancellationToken);
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

            // Bulk copy with streaming
            using var bulkCopy = new SqlBulkCopy(connection, SqlBulkCopyOptions.Default, transaction)
            {
                DestinationTableName = safeTableName,
                BatchSize = options.BatchSize,
                BulkCopyTimeout = options.Timeout,
                EnableStreaming = true
            };

            // Map columns
            foreach (var col in columns)
            {
                var targetCol = options.FieldMappings?.GetValueOrDefault(col) ?? col;
                bulkCopy.ColumnMappings.Add(col, targetCol);
            }

            // Stream data from MongoDB to SQL Server
            await bulkCopy.WriteToServerAsync(bsonReader, cancellationToken);

            result.TotalDocumentsRead = (int)bsonReader.RowCount;
            result.TotalRowsWritten = (int)bsonReader.RowCount;
            result.FailedDocuments = bsonReader.FailedCount;

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

    /// <summary>
    /// Infer schema from sample documents without loading entire collection.
    /// </summary>
    private (List<string> columns, Dictionary<string, Type> columnTypes) InferSchemaFromSamples(
        List<BsonDocument> sampleDocs,
        MongoToMssqlOptions options,
        MongoToMssqlResult result)
    {
        var columnTypes = new Dictionary<string, Type>();

        foreach (var doc in sampleDocs)
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

                // Validate column name
                var columnName = options.FieldMappings?.GetValueOrDefault(fieldName) ?? fieldName;
                if (!SqlValidator.IsValidColumnName(columnName))
                {
                    result.Warnings.Add($"Skipping invalid column name: '{columnName}'");
                    continue;
                }

                if (!columnTypes.ContainsKey(fieldName) && kvp.Value != null)
                {
                    columnTypes[fieldName] = kvp.Value.GetType();
                }
            }
        }

        return (columnTypes.Keys.ToList(), columnTypes);
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
        List<string> columns,
        Dictionary<string, Type> columnTypes,
        MongoToMssqlOptions options,
        CancellationToken cancellationToken)
    {
        var safeTableName = SqlValidator.SafeTableName(tableName);
        
        // Check if table exists
        var checkSql = $"SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = @tableName";
        await using var checkCmd = new SqlCommand(checkSql, connection, transaction);
        checkCmd.Parameters.AddWithValue("@tableName", tableName.Contains('.') ? tableName.Split('.')[1] : tableName);
        checkCmd.CommandTimeout = options.Timeout;
        
        var exists = (int)await checkCmd.ExecuteScalarAsync(cancellationToken)! > 0;
        if (exists) return;

        // Create table
        var sb = new StringBuilder();
        sb.AppendLine($"CREATE TABLE {safeTableName} (");

        var columnDefs = new List<string>();
        foreach (var col in columns)
        {
            var targetCol = options.FieldMappings?.GetValueOrDefault(col) ?? col;
            var safeColName = SqlValidator.SafeIdentifier(targetCol);
            var colType = columnTypes.TryGetValue(col, out var t) ? t : typeof(string);
            var sqlType = GetSqlType(colType);
            columnDefs.Add($"    {safeColName} {sqlType} NULL");
        }

        sb.AppendLine(string.Join(",\n", columnDefs));
        sb.AppendLine(")");

        await using var createCmd = new SqlCommand(sb.ToString(), connection, transaction);
        createCmd.CommandTimeout = options.Timeout;
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
