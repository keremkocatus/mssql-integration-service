using FluentAssertions;
using Moq;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Services;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using System.Text.Json;

namespace MssqlIntegrationService.Tests.Application;

public class JobAppServiceTests
{
    private readonly Mock<IJobRepository> _mockJobRepository;
    private readonly Mock<IJobQueueService> _mockJobQueueService;
    private readonly JobAppService _service;

    public JobAppServiceTests()
    {
        _mockJobRepository = new Mock<IJobRepository>();
        _mockJobQueueService = new Mock<IJobQueueService>();
        _service = new JobAppService(_mockJobRepository.Object, _mockJobQueueService.Object);
    }

    #region CreateDataTransferJobAsync Tests

    [Fact]
    public async Task CreateDataTransferJobAsync_ValidRequest_CreatesJobAndEnqueues()
    {
        // Arrange
        var request = new DataTransferJobRequest
        {
            Source = new SourceConfig
            {
                ConnectionString = "Server=source;",
                Query = "SELECT * FROM Users"
            },
            Target = new TargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "Users"
            }
        };

        Job? capturedJob = null;
        _mockJobRepository
            .Setup(x => x.CreateAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Callback<Job, CancellationToken>((job, _) => capturedJob = job)
            .ReturnsAsync((Job job, CancellationToken _) => job);

        _mockJobQueueService
            .Setup(x => x.EnqueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        // Act
        var response = await _service.CreateDataTransferJobAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.JobId.Should().NotBeNullOrEmpty();
        response.Status.Should().Be("Pending");
        response.StatusUrl.Should().Contain(response.JobId);

        capturedJob.Should().NotBeNull();
        capturedJob!.Type.Should().Be(JobType.DataTransfer);
        capturedJob.Status.Should().Be(JobStatus.Pending);
        capturedJob.RequestPayload.Should().NotBeNullOrEmpty();

        _mockJobRepository.Verify(x => x.CreateAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()), Times.Once);
        _mockJobQueueService.Verify(x => x.EnqueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    #endregion

    #region CreateDataSyncJobAsync Tests

    [Fact]
    public async Task CreateDataSyncJobAsync_ValidRequest_CreatesJobAndEnqueues()
    {
        // Arrange
        var request = new DataSyncJobRequest
        {
            Source = new SourceConfig
            {
                ConnectionString = "Server=source;",
                Query = "SELECT * FROM Products"
            },
            Target = new SyncTargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "Products",
                KeyColumns = new List<string> { "ProductId" }
            }
        };

        Job? capturedJob = null;
        _mockJobRepository
            .Setup(x => x.CreateAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Callback<Job, CancellationToken>((job, _) => capturedJob = job)
            .ReturnsAsync((Job job, CancellationToken _) => job);

        _mockJobQueueService
            .Setup(x => x.EnqueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        // Act
        var response = await _service.CreateDataSyncJobAsync(request);

        // Assert
        response.Should().NotBeNull();
        response.JobId.Should().NotBeNullOrEmpty();
        response.Status.Should().Be("Pending");

        capturedJob.Should().NotBeNull();
        capturedJob!.Type.Should().Be(JobType.DataSync);
        capturedJob.Status.Should().Be(JobStatus.Pending);
    }

    #endregion

    #region CreateMongoToMssqlJobAsync Tests

    [Fact]
    public async Task CreateMongoToMssqlJobAsync_ValidRequest_CreatesJobAndEnqueues()
    {
        // Arrange
        var request = new MongoToMssqlJobRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "Users"
            }
        };

        Job? capturedJob = null;
        _mockJobRepository
            .Setup(x => x.CreateAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Callback<Job, CancellationToken>((job, _) => capturedJob = job)
            .ReturnsAsync((Job job, CancellationToken _) => job);

        _mockJobQueueService
            .Setup(x => x.EnqueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        // Act
        var response = await _service.CreateMongoToMssqlJobAsync(request);

        // Assert
        response.Should().NotBeNull();
        capturedJob!.Type.Should().Be(JobType.MongoToMssql);
    }

    [Fact]
    public async Task CreateMongoToMssqlJobAsync_WithAsJson_SetsCorrectJobType()
    {
        // Arrange
        var request = new MongoToMssqlJobRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "Users"
            },
            AsJson = true
        };

        Job? capturedJob = null;
        _mockJobRepository
            .Setup(x => x.CreateAsync(It.IsAny<Job>(), It.IsAny<CancellationToken>()))
            .Callback<Job, CancellationToken>((job, _) => capturedJob = job)
            .ReturnsAsync((Job job, CancellationToken _) => job);

        _mockJobQueueService
            .Setup(x => x.EnqueueAsync(It.IsAny<string>(), It.IsAny<CancellationToken>()))
            .Returns(ValueTask.CompletedTask);

        // Act
        var response = await _service.CreateMongoToMssqlJobAsync(request);

        // Assert
        capturedJob!.Type.Should().Be(JobType.MongoToMssqlJson);
    }

    #endregion

    #region GetJobStatusAsync Tests

    [Fact]
    public async Task GetJobStatusAsync_ExistingJob_ReturnsJobStatus()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();
        var job = new Job
        {
            Id = jobId,
            Type = JobType.DataTransfer,
            Status = JobStatus.Running,
            Progress = 50,
            ProgressMessage = "Processing...",
            CreatedAt = DateTime.UtcNow.AddMinutes(-5),
            StartedAt = DateTime.UtcNow.AddMinutes(-4)
        };

        _mockJobRepository
            .Setup(x => x.GetByIdAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(job);

        // Act
        var response = await _service.GetJobStatusAsync(jobId);

        // Assert
        response.Should().NotBeNull();
        response!.JobId.Should().Be(jobId);
        response.Status.Should().Be("Running");
        response.Progress.Should().Be(50);
        response.ProgressMessage.Should().Be("Processing...");
        response.Type.Should().Be("DataTransfer");
        response.IsFinished.Should().BeFalse();
    }

    [Fact]
    public async Task GetJobStatusAsync_CompletedJob_ReturnsResultPayload()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();
        var resultPayload = JsonSerializer.Serialize(new { TotalRows = 1000, Success = true });
        var job = new Job
        {
            Id = jobId,
            Type = JobType.DataTransfer,
            Status = JobStatus.Completed,
            Progress = 100,
            ResultPayload = resultPayload,
            CreatedAt = DateTime.UtcNow.AddMinutes(-10),
            StartedAt = DateTime.UtcNow.AddMinutes(-9),
            CompletedAt = DateTime.UtcNow.AddMinutes(-1)
        };

        _mockJobRepository
            .Setup(x => x.GetByIdAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(job);

        // Act
        var response = await _service.GetJobStatusAsync(jobId);

        // Assert
        response.Should().NotBeNull();
        response!.Status.Should().Be("Completed");
        response.Progress.Should().Be(100);
        response.Result.Should().NotBeNull();
        response.CompletedAt.Should().NotBeNull();
        response.IsFinished.Should().BeTrue();
    }

    [Fact]
    public async Task GetJobStatusAsync_FailedJob_ReturnsErrorMessage()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();
        var job = new Job
        {
            Id = jobId,
            Type = JobType.DataSync,
            Status = JobStatus.Failed,
            ErrorMessage = "Connection timeout",
            CreatedAt = DateTime.UtcNow.AddMinutes(-5),
            StartedAt = DateTime.UtcNow.AddMinutes(-4),
            CompletedAt = DateTime.UtcNow.AddMinutes(-3)
        };

        _mockJobRepository
            .Setup(x => x.GetByIdAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(job);

        // Act
        var response = await _service.GetJobStatusAsync(jobId);

        // Assert
        response.Should().NotBeNull();
        response!.Status.Should().Be("Failed");
        response.ErrorMessage.Should().Be("Connection timeout");
        response.IsFinished.Should().BeTrue();
    }

    [Fact]
    public async Task GetJobStatusAsync_NonExistingJob_ReturnsNull()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();

        _mockJobRepository
            .Setup(x => x.GetByIdAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((Job?)null);

        // Act
        var response = await _service.GetJobStatusAsync(jobId);

        // Assert
        response.Should().BeNull();
    }

    #endregion

    #region GetRecentJobsAsync Tests

    [Fact]
    public async Task GetRecentJobsAsync_ReturnsJobList()
    {
        // Arrange
        var jobs = new List<Job>
        {
            new Job { Id = "1", Type = JobType.DataTransfer, Status = JobStatus.Completed, CreatedAt = DateTime.UtcNow.AddMinutes(-10) },
            new Job { Id = "2", Type = JobType.DataSync, Status = JobStatus.Running, CreatedAt = DateTime.UtcNow.AddMinutes(-5) },
            new Job { Id = "3", Type = JobType.MongoToMssql, Status = JobStatus.Pending, CreatedAt = DateTime.UtcNow }
        };

        _mockJobRepository
            .Setup(x => x.GetRecentAsync(20, It.IsAny<CancellationToken>()))
            .ReturnsAsync(jobs);

        // Act
        var response = await _service.GetRecentJobsAsync(20);

        // Assert
        response.Should().NotBeNull();
        response.Jobs.Should().HaveCount(3);
        response.TotalCount.Should().Be(3);
        response.Jobs[0].JobId.Should().Be("1");
        response.Jobs[1].JobId.Should().Be("2");
        response.Jobs[2].JobId.Should().Be("3");
    }

    [Fact]
    public async Task GetRecentJobsAsync_EmptyList_ReturnsEmptyResponse()
    {
        // Arrange
        _mockJobRepository
            .Setup(x => x.GetRecentAsync(10, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new List<Job>());

        // Act
        var response = await _service.GetRecentJobsAsync(10);

        // Assert
        response.Should().NotBeNull();
        response.Jobs.Should().BeEmpty();
        response.TotalCount.Should().Be(0);
    }

    #endregion

    #region CancelJobAsync Tests

    [Fact]
    public async Task CancelJobAsync_PendingJob_CancelsSuccessfully()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();
        var job = new Job
        {
            Id = jobId,
            Status = JobStatus.Pending,
            CreatedAt = DateTime.UtcNow
        };

        _mockJobRepository
            .Setup(x => x.GetByIdAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(job);

        _mockJobRepository
            .Setup(x => x.UpdateStatusAsync(jobId, JobStatus.Cancelled, It.IsAny<string?>(), It.IsAny<CancellationToken>()))
            .Returns(Task.CompletedTask);

        // Act
        var result = await _service.CancelJobAsync(jobId);

        // Assert
        result.Should().BeTrue();
        _mockJobRepository.Verify(x => x.UpdateStatusAsync(jobId, JobStatus.Cancelled, It.IsAny<string?>(), It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task CancelJobAsync_RunningJob_ReturnsFalse()
    {
        // Arrange - Running jobs cannot be cancelled (only Pending)
        var jobId = Guid.NewGuid().ToString();
        var job = new Job
        {
            Id = jobId,
            Status = JobStatus.Running,
            CreatedAt = DateTime.UtcNow
        };

        _mockJobRepository
            .Setup(x => x.GetByIdAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(job);

        // Act
        var result = await _service.CancelJobAsync(jobId);

        // Assert - Running jobs cannot be cancelled
        result.Should().BeFalse();
        _mockJobRepository.Verify(x => x.UpdateStatusAsync(It.IsAny<string>(), It.IsAny<JobStatus>(), It.IsAny<string?>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CancelJobAsync_CompletedJob_ReturnsFalse()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();
        var job = new Job
        {
            Id = jobId,
            Status = JobStatus.Completed,
            CreatedAt = DateTime.UtcNow
        };

        _mockJobRepository
            .Setup(x => x.GetByIdAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(job);

        // Act
        var result = await _service.CancelJobAsync(jobId);

        // Assert
        result.Should().BeFalse();
        _mockJobRepository.Verify(x => x.UpdateStatusAsync(It.IsAny<string>(), It.IsAny<JobStatus>(), It.IsAny<string?>(), It.IsAny<CancellationToken>()), Times.Never);
    }

    [Fact]
    public async Task CancelJobAsync_NonExistingJob_ReturnsFalse()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();

        _mockJobRepository
            .Setup(x => x.GetByIdAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((Job?)null);

        // Act
        var result = await _service.CancelJobAsync(jobId);

        // Assert
        result.Should().BeFalse();
    }

    #endregion
}
