using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Infrastructure.Services;

/// <summary>
/// Background service that processes ETL jobs from a queue.
/// Uses Channel for high-performance in-memory queue.
/// Implements IJobQueueService for job enqueuing.
/// </summary>
public class BackgroundJobProcessor : BackgroundService, IJobQueueService
{
    private readonly Channel<string> _jobQueue;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly ILogger<BackgroundJobProcessor> _logger;

    public BackgroundJobProcessor(
        IServiceScopeFactory scopeFactory,
        ILogger<BackgroundJobProcessor> logger)
    {
        _scopeFactory = scopeFactory;
        _logger = logger;
        
        // Bounded channel to prevent memory issues if jobs pile up
        _jobQueue = Channel.CreateBounded<string>(new BoundedChannelOptions(1000)
        {
            FullMode = BoundedChannelFullMode.Wait
        });
    }

    /// <summary>
    /// Enqueues a job ID for processing
    /// </summary>
    public async ValueTask EnqueueAsync(string jobId, CancellationToken cancellationToken = default)
    {
        await _jobQueue.Writer.WriteAsync(jobId, cancellationToken);
        _logger.LogInformation("Job {JobId} enqueued for background processing", jobId);
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("BackgroundJobProcessor started");

        await foreach (var jobId in _jobQueue.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                await ProcessJobAsync(jobId, stoppingToken);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Unhandled error processing job {JobId}", jobId);
            }
        }

        _logger.LogInformation("BackgroundJobProcessor stopped");
    }

    private async Task ProcessJobAsync(string jobId, CancellationToken cancellationToken)
    {
        using var scope = _scopeFactory.CreateScope();
        var jobRepository = scope.ServiceProvider.GetRequiredService<IJobRepository>();

        var job = await jobRepository.GetByIdAsync(jobId, cancellationToken);
        if (job == null)
        {
            _logger.LogWarning("Job {JobId} not found", jobId);
            return;
        }

        if (job.Status != JobStatus.Pending)
        {
            _logger.LogWarning("Job {JobId} is not in Pending status (current: {Status})", jobId, job.Status);
            return;
        }

        _logger.LogInformation("Processing job {JobId} of type {JobType}", jobId, job.Type);
        await jobRepository.MarkAsStartedAsync(jobId, cancellationToken);

        try
        {
            var result = job.Type switch
            {
                JobType.DataTransfer => await ProcessDataTransferAsync(scope.ServiceProvider, job, jobRepository, cancellationToken),
                JobType.BulkInsert => await ProcessBulkInsertAsync(scope.ServiceProvider, job, jobRepository, cancellationToken),
                JobType.DataSync => await ProcessDataSyncAsync(scope.ServiceProvider, job, jobRepository, cancellationToken),
                JobType.MongoToMssql => await ProcessMongoToMssqlAsync(scope.ServiceProvider, job, jobRepository, cancellationToken),
                JobType.MongoToMssqlJson => await ProcessMongoToMssqlJsonAsync(scope.ServiceProvider, job, jobRepository, cancellationToken),
                _ => throw new NotSupportedException($"Job type {job.Type} is not supported")
            };

            await jobRepository.MarkAsCompletedAsync(jobId, result, cancellationToken);
            _logger.LogInformation("Job {JobId} completed successfully", jobId);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Job {JobId} failed", jobId);
            await jobRepository.MarkAsFailedAsync(jobId, ex.Message, cancellationToken);
        }
    }

    private async Task<string> ProcessDataTransferAsync(
        IServiceProvider serviceProvider,
        Job job,
        IJobRepository jobRepository,
        CancellationToken cancellationToken)
    {
        var service = serviceProvider.GetRequiredService<IDataTransferService>();
        var request = JsonSerializer.Deserialize<DataTransferJobRequest>(job.RequestPayload)!;

        await jobRepository.UpdateProgressAsync(job.Id, 10, "Connecting to source database...", cancellationToken);

        var result = await service.TransferDataAsync(
            request.SourceConnectionString,
            request.TargetConnectionString,
            request.SourceQuery,
            request.TargetTable,
            new TransferOptions
            {
                BatchSize = request.BatchSize,
                Timeout = request.Timeout,
                TruncateTargetTable = request.TruncateTargetTable,
                CreateTableIfNotExists = request.CreateTableIfNotExists,
                UseTransaction = request.UseTransaction
            },
            cancellationToken);

        if (!result.IsSuccess)
        {
            throw new Exception(result.ErrorMessage);
        }

        return JsonSerializer.Serialize(new { result.Data!.TotalRowsWritten, result.Data.ExecutionTimeMs });
    }

    private async Task<string> ProcessBulkInsertAsync(
        IServiceProvider serviceProvider,
        Job job,
        IJobRepository jobRepository,
        CancellationToken cancellationToken)
    {
        var service = serviceProvider.GetRequiredService<IDataTransferService>();
        var request = JsonSerializer.Deserialize<BulkInsertJobRequest>(job.RequestPayload)!;

        await jobRepository.UpdateProgressAsync(job.Id, 10, "Preparing bulk insert...", cancellationToken);

        var data = JsonSerializer.Deserialize<List<Dictionary<string, object?>>>(request.DataJson)!;

        var result = await service.BulkInsertAsync(
            request.ConnectionString,
            request.TableName,
            data,
            new BulkInsertOptions
            {
                BatchSize = request.BatchSize,
                Timeout = request.Timeout,
                UseTransaction = request.UseTransaction
            },
            cancellationToken);

        if (!result.IsSuccess)
        {
            throw new Exception(result.ErrorMessage);
        }

        return JsonSerializer.Serialize(new { result.Data!.TotalRowsWritten, result.Data.ExecutionTimeMs });
    }

    private async Task<string> ProcessDataSyncAsync(
        IServiceProvider serviceProvider,
        Job job,
        IJobRepository jobRepository,
        CancellationToken cancellationToken)
    {
        var service = serviceProvider.GetRequiredService<IDataSyncService>();
        var request = JsonSerializer.Deserialize<DataSyncJobRequest>(job.RequestPayload)!;

        await jobRepository.UpdateProgressAsync(job.Id, 10, "Starting data synchronization...", cancellationToken);

        var result = await service.SyncDataAsync(
            request.SourceConnectionString,
            request.TargetConnectionString,
            request.SourceQuery,
            request.TargetTable,
            request.KeyColumns,
            new SyncOptions
            {
                BatchSize = request.BatchSize,
                Timeout = request.Timeout,
                UseTransaction = request.UseTransaction,
                DeleteAllBeforeInsert = request.DeleteAllBeforeInsert,
                ColumnMappings = request.ColumnMappings
            },
            cancellationToken);

        if (!result.IsSuccess)
        {
            throw new Exception(result.ErrorMessage);
        }

        return JsonSerializer.Serialize(new 
        { 
            result.Data!.TotalRowsRead, 
            result.Data.RowsDeleted, 
            result.Data.RowsInserted, 
            result.Data.ExecutionTimeMs 
        });
    }

    private async Task<string> ProcessMongoToMssqlAsync(
        IServiceProvider serviceProvider,
        Job job,
        IJobRepository jobRepository,
        CancellationToken cancellationToken)
    {
        var service = serviceProvider.GetRequiredService<IMongoToMssqlService>();
        var request = JsonSerializer.Deserialize<MongoToMssqlJobRequest>(job.RequestPayload)!;

        await jobRepository.UpdateProgressAsync(job.Id, 10, "Connecting to MongoDB...", cancellationToken);

        var options = new MongoToMssqlOptions
        {
            BatchSize = request.BatchSize,
            Timeout = request.Timeout,
            TruncateTargetTable = request.TruncateTargetTable,
            CreateTableIfNotExists = request.CreateTableIfNotExists,
            UseTransaction = request.UseTransaction,
            FlattenNestedDocuments = request.FlattenNestedDocuments,
            FlattenSeparator = request.FlattenSeparator,
            ArrayHandling = request.ArrayHandling,
            IncludeFields = request.IncludeFields,
            ExcludeFields = request.ExcludeFields,
            FieldMappings = request.FieldMappings
        };

        var result = !string.IsNullOrEmpty(request.AggregationPipeline)
            ? await service.TransferWithAggregationAsync(
                request.MongoConnectionString,
                request.MongoDatabaseName,
                request.MongoCollection,
                request.AggregationPipeline,
                request.MssqlConnectionString,
                request.TargetTable,
                options,
                cancellationToken)
            : await service.TransferAsync(
                request.MongoConnectionString,
                request.MongoDatabaseName,
                request.MongoCollection,
                request.MongoFilter,
                request.MssqlConnectionString,
                request.TargetTable,
                options,
                cancellationToken);

        if (!result.IsSuccess)
        {
            throw new Exception(result.ErrorMessage);
        }

        return JsonSerializer.Serialize(new
        {
            result.Data!.TotalDocumentsRead,
            result.Data.TotalRowsWritten,
            result.Data.FailedDocuments,
            result.Data.ExecutionTimeMs
        });
    }

    private async Task<string> ProcessMongoToMssqlJsonAsync(
        IServiceProvider serviceProvider,
        Job job,
        IJobRepository jobRepository,
        CancellationToken cancellationToken)
    {
        var service = serviceProvider.GetRequiredService<IMongoToMssqlService>();
        var request = JsonSerializer.Deserialize<MongoToMssqlJobRequest>(job.RequestPayload)!;

        await jobRepository.UpdateProgressAsync(job.Id, 10, "Connecting to MongoDB...", cancellationToken);

        var options = new MongoToMssqlOptions
        {
            BatchSize = request.BatchSize,
            Timeout = request.Timeout,
            TruncateTargetTable = request.TruncateTargetTable,
            UseTransaction = request.UseTransaction
        };

        var result = await service.TransferAsJsonAsync(
            request.MongoConnectionString,
            request.MongoDatabaseName,
            request.MongoCollection,
            request.MongoFilter,
            request.MssqlConnectionString,
            request.TargetTable,
            options,
            cancellationToken);

        if (!result.IsSuccess)
        {
            throw new Exception(result.ErrorMessage);
        }

        return JsonSerializer.Serialize(new
        {
            result.Data!.TotalDocumentsRead,
            result.Data.TotalRowsWritten,
            result.Data.ExecutionTimeMs
        });
    }
}

#region Job Request Models

internal class DataTransferJobRequest
{
    public string SourceConnectionString { get; set; } = string.Empty;
    public string TargetConnectionString { get; set; } = string.Empty;
    public string SourceQuery { get; set; } = string.Empty;
    public string TargetTable { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool TruncateTargetTable { get; set; }
    public bool CreateTableIfNotExists { get; set; }
    public bool UseTransaction { get; set; } = true;
}

internal class BulkInsertJobRequest
{
    public string ConnectionString { get; set; } = string.Empty;
    public string TableName { get; set; } = string.Empty;
    public string DataJson { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool UseTransaction { get; set; } = true;
}

internal class DataSyncJobRequest
{
    public string SourceConnectionString { get; set; } = string.Empty;
    public string TargetConnectionString { get; set; } = string.Empty;
    public string SourceQuery { get; set; } = string.Empty;
    public string TargetTable { get; set; } = string.Empty;
    public List<string> KeyColumns { get; set; } = new();
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool UseTransaction { get; set; } = true;
    public bool DeleteAllBeforeInsert { get; set; }
    public Dictionary<string, string>? ColumnMappings { get; set; }
}

internal class MongoToMssqlJobRequest
{
    public string MongoConnectionString { get; set; } = string.Empty;
    public string MongoDatabaseName { get; set; } = string.Empty;
    public string MongoCollection { get; set; } = string.Empty;
    public string? MongoFilter { get; set; }
    public string? AggregationPipeline { get; set; }
    public string MssqlConnectionString { get; set; } = string.Empty;
    public string TargetTable { get; set; } = string.Empty;
    public int BatchSize { get; set; } = 1000;
    public int Timeout { get; set; } = 300;
    public bool TruncateTargetTable { get; set; }
    public bool CreateTableIfNotExists { get; set; }
    public bool UseTransaction { get; set; } = true;
    public bool FlattenNestedDocuments { get; set; }
    public string FlattenSeparator { get; set; } = "_";
    public string ArrayHandling { get; set; } = "Serialize";
    public List<string>? IncludeFields { get; set; }
    public List<string>? ExcludeFields { get; set; }
    public Dictionary<string, string>? FieldMappings { get; set; }
}

#endregion
