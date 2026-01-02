using FluentAssertions;
using Moq;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Services;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Tests.Application;

public class DynamicQueryServiceTests
{
    private readonly Mock<IDynamicDatabaseService> _mockDatabaseService;
    private readonly DynamicQueryService _service;

    public DynamicQueryServiceTests()
    {
        _mockDatabaseService = new Mock<IDynamicDatabaseService>();
        _service = new DynamicQueryService(_mockDatabaseService.Object);
    }

    [Fact]
    public async Task TestConnectionAsync_Success_ReturnsSuccessResponse()
    {
        // Arrange
        var request = new TestConnectionRequest { ConnectionString = "Server=test;Database=test;" };

        _mockDatabaseService
            .Setup(x => x.TestConnectionAsync(request.ConnectionString, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<bool>.Success(true));

        // Act
        var response = await _service.TestConnectionAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.Message.Should().Be("Connection successful");
        response.ResponseTimeMs.Should().BeGreaterThanOrEqualTo(0);
    }

    [Fact]
    public async Task TestConnectionAsync_Failure_ReturnsErrorResponse()
    {
        // Arrange
        var request = new TestConnectionRequest { ConnectionString = "Invalid connection string" };
        var errorMessage = "Connection failed";

        _mockDatabaseService
            .Setup(x => x.TestConnectionAsync(request.ConnectionString, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<bool>.Failure(errorMessage));

        // Act
        var response = await _service.TestConnectionAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.Message.Should().Be(errorMessage);
    }

    [Fact]
    public async Task ExecuteQueryAsync_WithParameters_PassesParametersCorrectly()
    {
        // Arrange
        var parameters = new Dictionary<string, object?> { { "id", 1 } };
        var request = new DynamicQueryRequest
        {
            ConnectionString = "Server=test;",
            Query = "SELECT * FROM Users WHERE Id = @id",
            Parameters = parameters,
            Timeout = 60
        };

        var queryResult = new QueryResult
        {
            Rows = new List<IDictionary<string, object?>>(),
            RowCount = 0,
            ExecutionTimeMs = 10
        };

        _mockDatabaseService
            .Setup(x => x.ExecuteQueryAsync(
                request.ConnectionString,
                request.Query,
                It.Is<IDictionary<string, object?>>(p => p.ContainsKey("id")),
                request.Timeout,
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<QueryResult>.Success(queryResult));

        // Act
        var response = await _service.ExecuteQueryAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        _mockDatabaseService.Verify(x => x.ExecuteQueryAsync(
            request.ConnectionString,
            request.Query,
            It.IsAny<IDictionary<string, object?>>(),
            request.Timeout,
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task GetDatabaseInfoAsync_Success_ReturnsTableInfo()
    {
        // Arrange
        var request = new DatabaseInfoRequest
        {
            ConnectionString = "Server=test;",
            IncludeTables = true,
            IncludeColumns = true
        };

        var dbInfo = new DatabaseInfo
        {
            ServerName = "TestServer",
            DatabaseName = "TestDB",
            ServerVersion = "15.0",
            Edition = "Developer",
            Tables = new List<TableInfo>
            {
                new TableInfo
                {
                    SchemaName = "dbo",
                    TableName = "Users",
                    RowCount = 100,
                    Columns = new List<ColumnInfo>
                    {
                        new ColumnInfo { ColumnName = "Id", DataType = "int", IsNullable = false, IsPrimaryKey = true }
                    }
                }
            }
        };

        _mockDatabaseService
            .Setup(x => x.GetDatabaseInfoAsync(request.ConnectionString, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<DatabaseInfo>.Success(dbInfo));

        // Act
        var response = await _service.GetDatabaseInfoAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.ServerName.Should().Be("TestServer");
        response.DatabaseName.Should().Be("TestDB");
        response.Tables.Should().HaveCount(1);
        response.Tables![0].Columns.Should().HaveCount(1);
    }

    [Fact]
    public async Task GetDatabaseInfoAsync_WithoutColumns_DoesNotIncludeColumns()
    {
        // Arrange
        var request = new DatabaseInfoRequest
        {
            ConnectionString = "Server=test;",
            IncludeTables = true,
            IncludeColumns = false
        };

        var dbInfo = new DatabaseInfo
        {
            ServerName = "TestServer",
            DatabaseName = "TestDB",
            Tables = new List<TableInfo>
            {
                new TableInfo
                {
                    SchemaName = "dbo",
                    TableName = "Users",
                    Columns = new List<ColumnInfo>
                    {
                        new ColumnInfo { ColumnName = "Id" }
                    }
                }
            }
        };

        _mockDatabaseService
            .Setup(x => x.GetDatabaseInfoAsync(request.ConnectionString, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<DatabaseInfo>.Success(dbInfo));

        // Act
        var response = await _service.GetDatabaseInfoAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.Tables![0].Columns.Should().BeNull();
    }
}
