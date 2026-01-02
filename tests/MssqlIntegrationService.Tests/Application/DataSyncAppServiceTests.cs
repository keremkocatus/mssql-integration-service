using FluentAssertions;
using Moq;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Services;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Tests.Application;

public class DataSyncAppServiceTests
{
    private readonly Mock<IDataSyncService> _mockSyncService;
    private readonly DataSyncAppService _service;

    public DataSyncAppServiceTests()
    {
        _mockSyncService = new Mock<IDataSyncService>();
        _service = new DataSyncAppService(_mockSyncService.Object);
    }

    [Fact]
    public async Task SyncDataAsync_Success_ReturnsSyncResponse()
    {
        // Arrange
        var request = new DataSyncRequest
        {
            Source = new SyncSourceConfig
            {
                ConnectionString = "Server=source;",
                Query = "SELECT * FROM Users"
            },
            Target = new SyncTargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "Users",
                KeyColumns = new List<string> { "Id" }
            },
            Options = new SyncOptionsDto
            {
                BatchSize = 500,
                UseTransaction = true
            }
        };

        var syncResult = new SyncResult
        {
            TotalRowsRead = 100,
            RowsDeleted = 50,
            RowsInserted = 100,
            ExecutionTimeMs = 5000,
            SourceQuery = request.Source.Query,
            TargetTable = request.Target.TableName,
            KeyColumns = request.Target.KeyColumns,
            Warnings = new List<string> { "Created temp table", "Dropped temp table" }
        };

        _mockSyncService
            .Setup(x => x.SyncDataAsync(
                request.Source.ConnectionString,
                request.Target.ConnectionString,
                request.Source.Query,
                request.Target.TableName,
                request.Target.KeyColumns,
                It.IsAny<SyncOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<SyncResult>.Success(syncResult));

        // Act
        var response = await _service.SyncDataAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.TotalRowsRead.Should().Be(100);
        response.RowsDeleted.Should().Be(50);
        response.RowsInserted.Should().Be(100);
        response.ExecutionTimeMs.Should().Be(5000);
        response.KeyColumns.Should().Contain("Id");
        response.Warnings.Should().HaveCount(2);
    }

    [Fact]
    public async Task SyncDataAsync_Failure_ReturnsErrorResponse()
    {
        // Arrange
        var request = new DataSyncRequest
        {
            Source = new SyncSourceConfig
            {
                ConnectionString = "Server=source;",
                Query = "SELECT * FROM NonExistent"
            },
            Target = new SyncTargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "Users",
                KeyColumns = new List<string> { "Id" }
            }
        };

        var errorMessage = "Table does not exist";

        _mockSyncService
            .Setup(x => x.SyncDataAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<string>>(),
                It.IsAny<SyncOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<SyncResult>.Failure(errorMessage));

        // Act
        var response = await _service.SyncDataAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Be(errorMessage);
    }

    [Fact]
    public async Task SyncDataAsync_WithDeleteAll_PassesOptionCorrectly()
    {
        // Arrange
        var request = new DataSyncRequest
        {
            Source = new SyncSourceConfig
            {
                ConnectionString = "Server=source;",
                Query = "SELECT * FROM Users"
            },
            Target = new SyncTargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "Users",
                KeyColumns = new List<string> { "Id" }
            },
            Options = new SyncOptionsDto
            {
                DeleteAllBeforeInsert = true
            }
        };

        var syncResult = new SyncResult
        {
            TotalRowsRead = 100,
            RowsDeleted = 1000, // Deleted all
            RowsInserted = 100
        };

        _mockSyncService
            .Setup(x => x.SyncDataAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<string>>(),
                It.Is<SyncOptions>(o => o.DeleteAllBeforeInsert == true),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<SyncResult>.Success(syncResult));

        // Act
        var response = await _service.SyncDataAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.RowsDeleted.Should().Be(1000);
        _mockSyncService.Verify(x => x.SyncDataAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<List<string>>(),
            It.Is<SyncOptions>(o => o.DeleteAllBeforeInsert == true),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SyncDataAsync_WithColumnMappings_PassesMappingsCorrectly()
    {
        // Arrange
        var columnMappings = new Dictionary<string, string>
        {
            { "SourceId", "TargetId" },
            { "SourceName", "TargetName" }
        };

        var request = new DataSyncRequest
        {
            Source = new SyncSourceConfig
            {
                ConnectionString = "Server=source;",
                Query = "SELECT SourceId, SourceName FROM Users"
            },
            Target = new SyncTargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "NewUsers",
                KeyColumns = new List<string> { "SourceId" }
            },
            Options = new SyncOptionsDto
            {
                ColumnMappings = columnMappings
            }
        };

        var syncResult = new SyncResult
        {
            TotalRowsRead = 10,
            RowsDeleted = 5,
            RowsInserted = 10
        };

        _mockSyncService
            .Setup(x => x.SyncDataAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<List<string>>(),
                It.Is<SyncOptions>(o => o.ColumnMappings != null && o.ColumnMappings.Count == 2),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<SyncResult>.Success(syncResult));

        // Act
        var response = await _service.SyncDataAsync(request);

        // Assert
        response.Success.Should().BeTrue();
    }
}
