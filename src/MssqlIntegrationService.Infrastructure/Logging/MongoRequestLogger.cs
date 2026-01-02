using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Infrastructure.Logging;

/// <summary>
/// MongoDB logger implementation
/// </summary>
public class MongoRequestLogger : IRequestLogger, IDisposable
{
    private readonly MongoLoggingOptions _options;
    private readonly bool _masterEnabled;
    private IMongoCollection<MongoRequestLog>? _collection;
    private bool _initialized;
    private readonly SemaphoreSlim _initLock = new(1, 1);

    public MongoRequestLogger(IOptions<LoggingOptions> options)
    {
        _options = options.Value.MongoDB;
        _masterEnabled = options.Value.Enabled;
    }

    public string LoggerType => "MongoDB";
    public bool IsEnabled => _masterEnabled && _options.Enabled && !string.IsNullOrEmpty(_options.ConnectionString);

    public async Task LogAsync(RequestLog log, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;

        try
        {
            await EnsureInitializedAsync(cancellationToken);
            
            if (_collection == null) return;

            var mongoLog = new MongoRequestLog
            {
                Id = ObjectId.GenerateNewId(),
                RequestId = log.Id,
                Timestamp = log.Timestamp,
                HttpMethod = log.HttpMethod,
                Path = log.Path,
                QueryString = log.QueryString,
                RequestBody = log.RequestBody,
                RequestHeaders = log.RequestHeaders,
                StatusCode = log.StatusCode,
                ResponseBody = log.ResponseBody,
                ResponseTimeMs = log.ResponseTimeMs,
                ClientIp = log.ClientIp,
                UserAgent = log.UserAgent,
                ErrorMessage = log.ErrorMessage,
                StackTrace = log.StackTrace,
                CreatedAt = DateTime.UtcNow
            };

            await _collection.InsertOneAsync(mongoLog, cancellationToken: cancellationToken);
        }
        catch (Exception ex)
        {
            // Log to console as fallback, don't throw
            Console.WriteLine($"[MongoRequestLogger] Failed to log: {ex.Message}");
        }
    }

    private async Task EnsureInitializedAsync(CancellationToken cancellationToken)
    {
        if (_initialized) return;

        await _initLock.WaitAsync(cancellationToken);
        try
        {
            if (_initialized) return;

            var client = new MongoClient(_options.ConnectionString);
            var database = client.GetDatabase(_options.DatabaseName);
            _collection = database.GetCollection<MongoRequestLog>(_options.CollectionName);

            // Create TTL index if configured
            if (_options.TtlDays > 0)
            {
                var indexKeys = Builders<MongoRequestLog>.IndexKeys.Ascending(x => x.CreatedAt);
                var indexOptions = new CreateIndexOptions
                {
                    ExpireAfter = TimeSpan.FromDays(_options.TtlDays),
                    Name = "TTL_CreatedAt"
                };

                try
                {
                    await _collection.Indexes.CreateOneAsync(
                        new CreateIndexModel<MongoRequestLog>(indexKeys, indexOptions),
                        cancellationToken: cancellationToken);
                }
                catch (MongoCommandException)
                {
                    // Index might already exist with different settings, ignore
                }
            }

            // Create indexes for common queries
            var pathIndex = Builders<MongoRequestLog>.IndexKeys.Ascending(x => x.Path);
            var timestampIndex = Builders<MongoRequestLog>.IndexKeys.Descending(x => x.Timestamp);
            var statusIndex = Builders<MongoRequestLog>.IndexKeys.Ascending(x => x.StatusCode);

            try
            {
                await _collection.Indexes.CreateManyAsync(new[]
                {
                    new CreateIndexModel<MongoRequestLog>(pathIndex),
                    new CreateIndexModel<MongoRequestLog>(timestampIndex),
                    new CreateIndexModel<MongoRequestLog>(statusIndex)
                }, cancellationToken);
            }
            catch
            {
                // Indexes might already exist, ignore
            }

            _initialized = true;
        }
        finally
        {
            _initLock.Release();
        }
    }

    public void Dispose()
    {
        _initLock.Dispose();
    }
}

/// <summary>
/// MongoDB-specific log document
/// </summary>
internal class MongoRequestLog
{
    [BsonId]
    public ObjectId Id { get; set; }
    
    [BsonElement("requestId")]
    public string RequestId { get; set; } = string.Empty;
    
    [BsonElement("timestamp")]
    public DateTime Timestamp { get; set; }
    
    [BsonElement("httpMethod")]
    public string HttpMethod { get; set; } = string.Empty;
    
    [BsonElement("path")]
    public string Path { get; set; } = string.Empty;
    
    [BsonElement("queryString")]
    [BsonIgnoreIfNull]
    public string? QueryString { get; set; }
    
    [BsonElement("requestBody")]
    [BsonIgnoreIfNull]
    public string? RequestBody { get; set; }
    
    [BsonElement("requestHeaders")]
    [BsonIgnoreIfNull]
    public Dictionary<string, string>? RequestHeaders { get; set; }
    
    [BsonElement("statusCode")]
    public int StatusCode { get; set; }
    
    [BsonElement("responseBody")]
    [BsonIgnoreIfNull]
    public string? ResponseBody { get; set; }
    
    [BsonElement("responseTimeMs")]
    public long ResponseTimeMs { get; set; }
    
    [BsonElement("clientIp")]
    [BsonIgnoreIfNull]
    public string? ClientIp { get; set; }
    
    [BsonElement("userAgent")]
    [BsonIgnoreIfNull]
    public string? UserAgent { get; set; }
    
    [BsonElement("errorMessage")]
    [BsonIgnoreIfNull]
    public string? ErrorMessage { get; set; }
    
    [BsonElement("stackTrace")]
    [BsonIgnoreIfNull]
    public string? StackTrace { get; set; }
    
    [BsonElement("createdAt")]
    public DateTime CreatedAt { get; set; }
}
