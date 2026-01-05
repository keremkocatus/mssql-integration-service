using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using MongoDB.Driver;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Infrastructure.Data;

/// <summary>
/// MongoDB implementation of IJobRepository
/// </summary>
public class MongoJobRepository : IJobRepository
{
    private readonly IMongoCollection<JobDocument> _collection;

    public MongoJobRepository(IMongoDatabase database, string collectionName = "Jobs")
    {
        _collection = database.GetCollection<JobDocument>(collectionName);
        
        // Create indexes
        var indexKeys = Builders<JobDocument>.IndexKeys;
        var indexes = new List<CreateIndexModel<JobDocument>>
        {
            new(indexKeys.Descending(x => x.CreatedAt)),
            new(indexKeys.Ascending(x => x.Status)),
            new(indexKeys.Ascending(x => x.Status).Descending(x => x.CreatedAt))
        };
        _collection.Indexes.CreateMany(indexes);
    }

    public async Task<Job> CreateAsync(Job job, CancellationToken cancellationToken = default)
    {
        var document = ToDocument(job);
        await _collection.InsertOneAsync(document, cancellationToken: cancellationToken);
        return job;
    }

    public async Task<Job?> GetByIdAsync(string jobId, CancellationToken cancellationToken = default)
    {
        var filter = Builders<JobDocument>.Filter.Eq(x => x.Id, jobId);
        var document = await _collection.Find(filter).FirstOrDefaultAsync(cancellationToken);
        return document != null ? ToEntity(document) : null;
    }

    public async Task<Job> UpdateAsync(Job job, CancellationToken cancellationToken = default)
    {
        var document = ToDocument(job);
        var filter = Builders<JobDocument>.Filter.Eq(x => x.Id, job.Id);
        await _collection.ReplaceOneAsync(filter, document, cancellationToken: cancellationToken);
        return job;
    }

    public async Task UpdateStatusAsync(string jobId, JobStatus status, string? errorMessage = null, CancellationToken cancellationToken = default)
    {
        var filter = Builders<JobDocument>.Filter.Eq(x => x.Id, jobId);
        var update = Builders<JobDocument>.Update
            .Set(x => x.Status, (int)status)
            .Set(x => x.ErrorMessage, errorMessage);

        if (status == JobStatus.Completed || status == JobStatus.Failed || status == JobStatus.Cancelled)
        {
            update = update.Set(x => x.CompletedAt, DateTime.UtcNow);
        }

        await _collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken);
    }

    public async Task UpdateProgressAsync(string jobId, int progress, string? message = null, CancellationToken cancellationToken = default)
    {
        var filter = Builders<JobDocument>.Filter.Eq(x => x.Id, jobId);
        var update = Builders<JobDocument>.Update
            .Set(x => x.Progress, progress)
            .Set(x => x.ProgressMessage, message);

        await _collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken);
    }

    public async Task MarkAsStartedAsync(string jobId, CancellationToken cancellationToken = default)
    {
        var filter = Builders<JobDocument>.Filter.Eq(x => x.Id, jobId);
        var update = Builders<JobDocument>.Update
            .Set(x => x.Status, (int)JobStatus.Running)
            .Set(x => x.StartedAt, DateTime.UtcNow)
            .Set(x => x.Progress, 0)
            .Set(x => x.ProgressMessage, "Processing started");

        await _collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken);
    }

    public async Task MarkAsCompletedAsync(string jobId, string? resultPayload = null, CancellationToken cancellationToken = default)
    {
        var filter = Builders<JobDocument>.Filter.Eq(x => x.Id, jobId);
        var update = Builders<JobDocument>.Update
            .Set(x => x.Status, (int)JobStatus.Completed)
            .Set(x => x.CompletedAt, DateTime.UtcNow)
            .Set(x => x.Progress, 100)
            .Set(x => x.ProgressMessage, "Completed")
            .Set(x => x.ResultPayload, resultPayload);

        await _collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken);
    }

    public async Task MarkAsFailedAsync(string jobId, string errorMessage, CancellationToken cancellationToken = default)
    {
        var filter = Builders<JobDocument>.Filter.Eq(x => x.Id, jobId);
        var update = Builders<JobDocument>.Update
            .Set(x => x.Status, (int)JobStatus.Failed)
            .Set(x => x.CompletedAt, DateTime.UtcNow)
            .Set(x => x.ErrorMessage, errorMessage)
            .Set(x => x.ProgressMessage, "Failed");

        await _collection.UpdateOneAsync(filter, update, cancellationToken: cancellationToken);
    }

    public async Task<List<Job>> GetByStatusAsync(JobStatus status, int limit = 100, CancellationToken cancellationToken = default)
    {
        var filter = Builders<JobDocument>.Filter.Eq(x => x.Status, (int)status);
        var documents = await _collection
            .Find(filter)
            .SortByDescending(x => x.CreatedAt)
            .Limit(limit)
            .ToListAsync(cancellationToken);

        return documents.Select(ToEntity).ToList();
    }

    public async Task<List<Job>> GetRecentAsync(int limit = 50, CancellationToken cancellationToken = default)
    {
        var documents = await _collection
            .Find(FilterDefinition<JobDocument>.Empty)
            .SortByDescending(x => x.CreatedAt)
            .Limit(limit)
            .ToListAsync(cancellationToken);

        return documents.Select(ToEntity).ToList();
    }

    public async Task<int> DeleteOldJobsAsync(DateTime olderThan, CancellationToken cancellationToken = default)
    {
        var filter = Builders<JobDocument>.Filter.And(
            Builders<JobDocument>.Filter.Lt(x => x.CreatedAt, olderThan),
            Builders<JobDocument>.Filter.In(x => x.Status, new[] { (int)JobStatus.Completed, (int)JobStatus.Failed, (int)JobStatus.Cancelled })
        );

        var result = await _collection.DeleteManyAsync(filter, cancellationToken);
        return (int)result.DeletedCount;
    }

    #region Mapping

    private static JobDocument ToDocument(Job entity) => new()
    {
        Id = entity.Id,
        Type = (int)entity.Type,
        Status = (int)entity.Status,
        RequestPayload = entity.RequestPayload,
        ResultPayload = entity.ResultPayload,
        ErrorMessage = entity.ErrorMessage,
        Progress = entity.Progress,
        ProgressMessage = entity.ProgressMessage,
        CreatedAt = entity.CreatedAt,
        StartedAt = entity.StartedAt,
        CompletedAt = entity.CompletedAt,
        CreatedBy = entity.CreatedBy,
        CorrelationId = entity.CorrelationId
    };

    private static Job ToEntity(JobDocument document) => new()
    {
        Id = document.Id,
        Type = (JobType)document.Type,
        Status = (JobStatus)document.Status,
        RequestPayload = document.RequestPayload,
        ResultPayload = document.ResultPayload,
        ErrorMessage = document.ErrorMessage,
        Progress = document.Progress,
        ProgressMessage = document.ProgressMessage,
        CreatedAt = document.CreatedAt,
        StartedAt = document.StartedAt,
        CompletedAt = document.CompletedAt,
        CreatedBy = document.CreatedBy,
        CorrelationId = document.CorrelationId
    };

    #endregion
}

/// <summary>
/// MongoDB document for Job
/// </summary>
internal class JobDocument
{
    [BsonId]
    [BsonRepresentation(BsonType.String)]
    public string Id { get; set; } = string.Empty;

    public int Type { get; set; }
    public int Status { get; set; }
    public string RequestPayload { get; set; } = string.Empty;
    public string? ResultPayload { get; set; }
    public string? ErrorMessage { get; set; }
    public int Progress { get; set; }
    public string? ProgressMessage { get; set; }
    public DateTime CreatedAt { get; set; }
    public DateTime? StartedAt { get; set; }
    public DateTime? CompletedAt { get; set; }
    public string? CreatedBy { get; set; }
    public string? CorrelationId { get; set; }
}
