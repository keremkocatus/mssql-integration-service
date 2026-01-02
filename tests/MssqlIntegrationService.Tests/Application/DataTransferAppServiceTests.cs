using FluentAssertions;
using Moq;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Services;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Tests.Application;

public class DataTransferAppServiceTests
{
    private readonly Mock<IDataTransferService> _mockTransferService;
    private readonly DataTransferAppService _service;

    public DataTransferAppServiceTests()
    {
        _mockTransferService = new Mock<IDataTransferService>();
        _service = new DataTransferAppService(_mockTransferService.Object);
    }

    [Fact]
    public async Task TransferDataAsync_Success_ReturnsTransferResponse()
    {
        // Arrange
        var request = new DataTransferRequest
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
            },
            Options = new TransferOptionsDto
            {
                BatchSize = 500,
                TruncateTargetTable = true
            }
        };

        var transferResult = new TransferResult
        {
            TotalRowsRead = 100,
            TotalRowsWritten = 100,
            ExecutionTimeMs = 5000,
            SourceQuery = request.Source.Query,
            TargetTable = request.Target.TableName,
            Warnings = new List<string> { "Table truncated" }
        };

        _mockTransferService
            .Setup(x => x.TransferDataAsync(
                request.Source.ConnectionString,
                request.Target.ConnectionString,
                request.Source.Query,
                request.Target.TableName,
                It.IsAny<TransferOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<TransferResult>.Success(transferResult));

        // Act
        var response = await _service.TransferDataAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.TotalRowsRead.Should().Be(100);
        response.TotalRowsWritten.Should().Be(100);
        response.ExecutionTimeMs.Should().Be(5000);
        response.Warnings.Should().Contain("Table truncated");
    }

    [Fact]
    public async Task TransferDataAsync_Failure_ReturnsErrorResponse()
    {
        // Arrange
        var request = new DataTransferRequest
        {
            Source = new SourceConfig
            {
                ConnectionString = "Server=source;",
                Query = "SELECT * FROM NonExistentTable"
            },
            Target = new TargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "Users"
            }
        };

        var errorMessage = "Table does not exist";

        _mockTransferService
            .Setup(x => x.TransferDataAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<TransferOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<TransferResult>.Failure(errorMessage));

        // Act
        var response = await _service.TransferDataAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Be(errorMessage);
    }

    [Fact]
    public async Task BulkInsertAsync_Success_ReturnsInsertedCount()
    {
        // Arrange
        var request = new BulkInsertRequest
        {
            ConnectionString = "Server=test;",
            TableName = "Users",
            Data = new List<Dictionary<string, object?>>
            {
                new() { { "Name", "John" }, { "Email", "john@test.com" } },
                new() { { "Name", "Jane" }, { "Email", "jane@test.com" } }
            }
        };

        var transferResult = new TransferResult
        {
            TotalRowsWritten = 2,
            ExecutionTimeMs = 100
        };

        _mockTransferService
            .Setup(x => x.BulkInsertAsync(
                request.ConnectionString,
                request.TableName,
                It.IsAny<IEnumerable<IDictionary<string, object?>>>(),
                It.IsAny<BulkInsertOptions>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<TransferResult>.Success(transferResult));

        // Act
        var response = await _service.BulkInsertAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.TotalRowsInserted.Should().Be(2);
        response.ExecutionTimeMs.Should().Be(100);
    }

    [Fact]
    public async Task TransferDataAsync_WithColumnMappings_PassesMappingsCorrectly()
    {
        // Arrange
        var columnMappings = new Dictionary<string, string>
        {
            { "OldName", "NewName" },
            { "OldEmail", "NewEmail" }
        };

        var request = new DataTransferRequest
        {
            Source = new SourceConfig
            {
                ConnectionString = "Server=source;",
                Query = "SELECT OldName, OldEmail FROM Users"
            },
            Target = new TargetConfig
            {
                ConnectionString = "Server=target;",
                TableName = "NewUsers"
            },
            Options = new TransferOptionsDto
            {
                ColumnMappings = columnMappings
            }
        };

        var transferResult = new TransferResult
        {
            TotalRowsRead = 10,
            TotalRowsWritten = 10
        };

        _mockTransferService
            .Setup(x => x.TransferDataAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.Is<TransferOptions>(o => o.ColumnMappings != null && o.ColumnMappings.Count == 2),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<TransferResult>.Success(transferResult));

        // Act
        var response = await _service.TransferDataAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        _mockTransferService.Verify(x => x.TransferDataAsync(
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.IsAny<string>(),
            It.Is<TransferOptions>(o => o.ColumnMappings!.ContainsKey("OldName")),
            It.IsAny<CancellationToken>()), Times.Once);
    }
}
