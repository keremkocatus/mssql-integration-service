using System.Text.Json;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;

namespace MssqlIntegrationService.Application.Services;

/// <summary>
/// Application service for Job operations
/// </summary>
public class JobAppService : IJobAppService
{
    private readonly IJobRepository _jobRepository;
    private readonly IJobQueueService _jobQueueService;

    public JobAppService(IJobRepository jobRepository, IJobQueueService jobQueueService)
    {
        _jobRepository = jobRepository;
        _jobQueueService = jobQueueService;
    }

    public async Task<JobCreatedResponse> CreateDataTransferJobAsync(DataTransferJobRequest request, CancellationToken cancellationToken = default)
    {
        // Validation
        if (!SqlValidator.IsValidTableName(request.Target.TableName))
        {
            throw new ArgumentException($"Invalid target table name: '{request.Target.TableName}'");
        }

        var jobPayload = new
        {
            SourceConnectionString = request.Source.ConnectionString,
            TargetConnectionString = request.Target.ConnectionString,
            SourceQuery = request.Source.Query,
            TargetTable = request.Target.TableName,
            BatchSize = request.Options?.BatchSize ?? 1000,
            Timeout = request.Options?.Timeout ?? 300,
            TruncateTargetTable = request.Options?.TruncateTargetTable ?? false,
            CreateTableIfNotExists = request.Options?.CreateTableIfNotExists ?? false,
            UseTransaction = request.Options?.UseTransaction ?? true
        };

        var job = new Job
        {
            Type = JobType.DataTransfer,
            RequestPayload = JsonSerializer.Serialize(jobPayload)
        };

        await _jobRepository.CreateAsync(job, cancellationToken);
        await _jobQueueService.EnqueueAsync(job.Id, cancellationToken);

        return new JobCreatedResponse
        {
            JobId = job.Id,
            Status = job.Status.ToString(),
            StatusUrl = $"/api/jobs/{job.Id}"
        };
    }

    public async Task<JobCreatedResponse> CreateDataSyncJobAsync(DataSyncJobRequest request, CancellationToken cancellationToken = default)
    {
        // Validation
        if (!SqlValidator.IsValidTableName(request.Target.TableName))
        {
            throw new ArgumentException($"Invalid target table name: '{request.Target.TableName}'");
        }

        if (request.Target.KeyColumns == null || request.Target.KeyColumns.Count == 0)
        {
            throw new ArgumentException("KeyColumns must be specified for data sync");
        }

        var jobPayload = new
        {
            SourceConnectionString = request.Source.ConnectionString,
            TargetConnectionString = request.Target.ConnectionString,
            SourceQuery = request.Source.Query,
            TargetTable = request.Target.TableName,
            KeyColumns = request.Target.KeyColumns,
            BatchSize = request.Options?.BatchSize ?? 1000,
            Timeout = request.Options?.Timeout ?? 300,
            UseTransaction = request.Options?.UseTransaction ?? true,
            DeleteAllBeforeInsert = request.Options?.DeleteAllBeforeInsert ?? false,
            ColumnMappings = request.Options?.ColumnMappings
        };

        var job = new Job
        {
            Type = JobType.DataSync,
            RequestPayload = JsonSerializer.Serialize(jobPayload)
        };

        await _jobRepository.CreateAsync(job, cancellationToken);
        await _jobQueueService.EnqueueAsync(job.Id, cancellationToken);

        return new JobCreatedResponse
        {
            JobId = job.Id,
            Status = job.Status.ToString(),
            StatusUrl = $"/api/jobs/{job.Id}"
        };
    }

    public async Task<JobCreatedResponse> CreateMongoToMssqlJobAsync(MongoToMssqlJobRequest request, CancellationToken cancellationToken = default)
    {
        // Validation
        if (!SqlValidator.IsValidTableName(request.Target.TableName))
        {
            throw new ArgumentException($"Invalid target table name: '{request.Target.TableName}'");
        }

        var jobPayload = new
        {
            MongoConnectionString = request.Source.ConnectionString,
            MongoDatabaseName = request.Source.DatabaseName,
            MongoCollection = request.Source.CollectionName,
            MongoFilter = request.Source.Filter,
            AggregationPipeline = request.Source.AggregationPipeline,
            MssqlConnectionString = request.Target.ConnectionString,
            TargetTable = request.Target.TableName,
            BatchSize = request.Options?.BatchSize ?? 1000,
            Timeout = request.Options?.Timeout ?? 300,
            TruncateTargetTable = request.Options?.TruncateTargetTable ?? false,
            CreateTableIfNotExists = request.Options?.CreateTableIfNotExists ?? false,
            UseTransaction = request.Options?.UseTransaction ?? true,
            FlattenNestedDocuments = request.Options?.FlattenNestedDocuments ?? false,
            FlattenSeparator = request.Options?.FlattenSeparator ?? "_",
            ArrayHandling = request.Options?.ArrayHandling ?? "Serialize",
            IncludeFields = request.Options?.IncludeFields,
            ExcludeFields = request.Options?.ExcludeFields,
            FieldMappings = request.Options?.FieldMappings
        };

        var job = new Job
        {
            Type = request.AsJson ? JobType.MongoToMssqlJson : JobType.MongoToMssql,
            RequestPayload = JsonSerializer.Serialize(jobPayload)
        };

        await _jobRepository.CreateAsync(job, cancellationToken);
        await _jobQueueService.EnqueueAsync(job.Id, cancellationToken);

        return new JobCreatedResponse
        {
            JobId = job.Id,
            Status = job.Status.ToString(),
            StatusUrl = $"/api/jobs/{job.Id}"
        };
    }

    public async Task<JobStatusResponse?> GetJobStatusAsync(string jobId, CancellationToken cancellationToken = default)
    {
        var job = await _jobRepository.GetByIdAsync(jobId, cancellationToken);
        return job?.ToResponse();
    }

    public async Task<JobListResponse> GetRecentJobsAsync(int limit = 50, CancellationToken cancellationToken = default)
    {
        var jobs = await _jobRepository.GetRecentAsync(limit, cancellationToken);
        return new JobListResponse
        {
            Jobs = jobs.Select(j => j.ToResponse()).ToList(),
            TotalCount = jobs.Count
        };
    }

    public async Task<bool> CancelJobAsync(string jobId, CancellationToken cancellationToken = default)
    {
        var job = await _jobRepository.GetByIdAsync(jobId, cancellationToken);
        if (job == null || job.Status != JobStatus.Pending)
        {
            return false;
        }

        await _jobRepository.UpdateStatusAsync(jobId, JobStatus.Cancelled, "Cancelled by user", cancellationToken);
        return true;
    }
}
