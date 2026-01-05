using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;
using MssqlIntegrationService.API.Controllers;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;

namespace MssqlIntegrationService.Tests.Controllers;

public class JobsControllerTests
{
    private readonly Mock<IJobAppService> _mockJobAppService;
    private readonly Mock<ILogger<JobsController>> _mockLogger;
    private readonly JobsController _controller;

    public JobsControllerTests()
    {
        _mockJobAppService = new Mock<IJobAppService>();
        _mockLogger = new Mock<ILogger<JobsController>>();
        _controller = new JobsController(_mockJobAppService.Object, _mockLogger.Object);
    }

    #region CreateDataTransferJob Tests

    [Fact]
    public async Task CreateDataTransferJob_ValidRequest_ReturnsAccepted()
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

        var expectedResponse = new JobCreatedResponse
        {
            JobId = Guid.NewGuid().ToString(),
            Status = "Pending",
            StatusUrl = "/api/jobs/123"
        };

        _mockJobAppService
            .Setup(x => x.CreateDataTransferJobAsync(request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.CreateDataTransferJob(request, CancellationToken.None);

        // Assert
        var acceptedResult = result.Should().BeOfType<AcceptedAtActionResult>().Subject;
        acceptedResult.StatusCode.Should().Be(StatusCodes.Status202Accepted);

        var response = acceptedResult.Value.Should().BeOfType<JobCreatedResponse>().Subject;
        response.JobId.Should().Be(expectedResponse.JobId);
        response.Status.Should().Be("Pending");
    }

    #endregion

    #region CreateDataSyncJob Tests

    [Fact]
    public async Task CreateDataSyncJob_ValidRequest_ReturnsAccepted()
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

        var expectedResponse = new JobCreatedResponse
        {
            JobId = Guid.NewGuid().ToString(),
            Status = "Pending"
        };

        _mockJobAppService
            .Setup(x => x.CreateDataSyncJobAsync(request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.CreateDataSyncJob(request, CancellationToken.None);

        // Assert
        result.Should().BeOfType<AcceptedAtActionResult>();
    }

    #endregion

    #region CreateMongoToMssqlJob Tests

    [Fact]
    public async Task CreateMongoToMssqlJob_ValidRequest_ReturnsAccepted()
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

        var expectedResponse = new JobCreatedResponse
        {
            JobId = Guid.NewGuid().ToString(),
            Status = "Pending"
        };

        _mockJobAppService
            .Setup(x => x.CreateMongoToMssqlJobAsync(request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.CreateMongoToMssqlJob(request, CancellationToken.None);

        // Assert
        result.Should().BeOfType<AcceptedAtActionResult>();
    }

    #endregion

    #region GetJobStatus Tests

    [Fact]
    public async Task GetJobStatus_ExistingJob_ReturnsOk()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();
        var expectedResponse = new JobStatusResponse
        {
            JobId = jobId,
            Status = "Running",
            Progress = 50,
            ProgressMessage = "Processing..."
        };

        _mockJobAppService
            .Setup(x => x.GetJobStatusAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.GetJobStatus(jobId, CancellationToken.None);

        // Assert
        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<JobStatusResponse>().Subject;
        response.JobId.Should().Be(jobId);
        response.Status.Should().Be("Running");
        response.Progress.Should().Be(50);
    }

    [Fact]
    public async Task GetJobStatus_NonExistingJob_ReturnsNotFound()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();

        _mockJobAppService
            .Setup(x => x.GetJobStatusAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync((JobStatusResponse?)null);

        // Act
        var result = await _controller.GetJobStatus(jobId, CancellationToken.None);

        // Assert
        result.Should().BeOfType<NotFoundObjectResult>();
    }

    [Fact]
    public async Task GetJobStatus_CompletedJob_ReturnsWithResult()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();
        var expectedResponse = new JobStatusResponse
        {
            JobId = jobId,
            Status = "Completed",
            Progress = 100,
            Result = new { TotalRows = 1000 },
            CompletedAt = DateTime.UtcNow,
            IsFinished = true
        };

        _mockJobAppService
            .Setup(x => x.GetJobStatusAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.GetJobStatus(jobId, CancellationToken.None);

        // Assert
        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<JobStatusResponse>().Subject;
        response.Status.Should().Be("Completed");
        response.Result.Should().NotBeNull();
        response.CompletedAt.Should().NotBeNull();
        response.IsFinished.Should().BeTrue();
    }

    [Fact]
    public async Task GetJobStatus_FailedJob_ReturnsWithError()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();
        var expectedResponse = new JobStatusResponse
        {
            JobId = jobId,
            Status = "Failed",
            ErrorMessage = "Connection timeout",
            IsFinished = true
        };

        _mockJobAppService
            .Setup(x => x.GetJobStatusAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.GetJobStatus(jobId, CancellationToken.None);

        // Assert
        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<JobStatusResponse>().Subject;
        response.Status.Should().Be("Failed");
        response.ErrorMessage.Should().Be("Connection timeout");
    }

    #endregion

    #region GetRecentJobs Tests

    [Fact]
    public async Task GetRecentJobs_ReturnsJobList()
    {
        // Arrange
        var expectedResponse = new JobListResponse
        {
            Jobs = new List<JobStatusResponse>
            {
                new() { JobId = "1", Status = "Completed", Type = "DataTransfer" },
                new() { JobId = "2", Status = "Running", Type = "DataSync" },
                new() { JobId = "3", Status = "Pending", Type = "MongoToMssql" }
            },
            TotalCount = 3
        };

        _mockJobAppService
            .Setup(x => x.GetRecentJobsAsync(50, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.GetRecentJobs(50, CancellationToken.None);

        // Assert
        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<JobListResponse>().Subject;
        response.Jobs.Should().HaveCount(3);
        response.TotalCount.Should().Be(3);
    }

    [Fact]
    public async Task GetRecentJobs_DefaultLimit_Uses50()
    {
        // Arrange
        _mockJobAppService
            .Setup(x => x.GetRecentJobsAsync(50, It.IsAny<CancellationToken>()))
            .ReturnsAsync(new JobListResponse { Jobs = new(), TotalCount = 0 });

        // Act
        var result = await _controller.GetRecentJobs(cancellationToken: CancellationToken.None);

        // Assert
        _mockJobAppService.Verify(x => x.GetRecentJobsAsync(50, It.IsAny<CancellationToken>()), Times.Once);
    }

    #endregion

    #region CancelJob Tests

    [Fact]
    public async Task CancelJob_PendingJob_ReturnsOk()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();

        _mockJobAppService
            .Setup(x => x.CancelJobAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(true);

        // Act
        var result = await _controller.CancelJob(jobId, CancellationToken.None);

        // Assert
        result.Should().BeOfType<OkObjectResult>();
    }

    [Fact]
    public async Task CancelJob_NonExistingOrCompletedJob_ReturnsBadRequest()
    {
        // Arrange
        var jobId = Guid.NewGuid().ToString();

        _mockJobAppService
            .Setup(x => x.CancelJobAsync(jobId, It.IsAny<CancellationToken>()))
            .ReturnsAsync(false);

        // Act
        var result = await _controller.CancelJob(jobId, CancellationToken.None);

        // Assert
        result.Should().BeOfType<BadRequestObjectResult>();
    }

    #endregion
}
