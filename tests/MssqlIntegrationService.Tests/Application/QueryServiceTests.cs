using FluentAssertions;
using Moq;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Services;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Tests.Application;

public class QueryServiceTests
{
    private readonly Mock<IDatabaseService> _mockDatabaseService;
    private readonly QueryService _queryService;

    public QueryServiceTests()
    {
        _mockDatabaseService = new Mock<IDatabaseService>();
        _queryService = new QueryService(_mockDatabaseService.Object);
    }

    [Fact]
    public async Task ExecuteQueryAsync_Success_ReturnsSuccessResponse()
    {
        // Arrange
        var request = new QueryRequest { Query = "SELECT * FROM Users" };
        var queryResult = new QueryResult
        {
            Rows = new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { { "Id", 1 }, { "Name", "John" } }
            },
            RowCount = 1,
            ExecutionTimeMs = 50
        };

        _mockDatabaseService
            .Setup(x => x.ExecuteQueryAsync(request.Query, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<QueryResult>.Success(queryResult));

        // Act
        var response = await _queryService.ExecuteQueryAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.RowCount.Should().Be(1);
        response.ExecutionTimeMs.Should().Be(50);
        response.Data.Should().HaveCount(1);
    }

    [Fact]
    public async Task ExecuteQueryAsync_Failure_ReturnsErrorResponse()
    {
        // Arrange
        var request = new QueryRequest { Query = "INVALID SQL" };
        var errorMessage = "Invalid SQL syntax";

        _mockDatabaseService
            .Setup(x => x.ExecuteQueryAsync(request.Query, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<QueryResult>.Failure(errorMessage, 102));

        // Act
        var response = await _queryService.ExecuteQueryAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Be(errorMessage);
        response.ErrorCode.Should().Be(102);
    }

    [Fact]
    public async Task ExecuteNonQueryAsync_Success_ReturnsAffectedRows()
    {
        // Arrange
        var request = new QueryRequest { Query = "UPDATE Users SET Name = 'Test'" };

        _mockDatabaseService
            .Setup(x => x.ExecuteNonQueryAsync(request.Query, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<int>.Success(5));

        // Act
        var response = await _queryService.ExecuteNonQueryAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.AffectedRows.Should().Be(5);
    }

    [Fact]
    public async Task ExecuteStoredProcedureAsync_Success_ReturnsData()
    {
        // Arrange
        var request = new StoredProcedureRequest { ProcedureName = "sp_GetUsers" };
        var queryResult = new QueryResult
        {
            Rows = new List<IDictionary<string, object?>>
            {
                new Dictionary<string, object?> { { "Id", 1 } },
                new Dictionary<string, object?> { { "Id", 2 } }
            },
            RowCount = 2,
            ExecutionTimeMs = 30
        };

        _mockDatabaseService
            .Setup(x => x.ExecuteStoredProcedureAsync(request.ProcedureName, null, It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<QueryResult>.Success(queryResult));

        // Act
        var response = await _queryService.ExecuteStoredProcedureAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.RowCount.Should().Be(2);
        response.Data.Should().HaveCount(2);
    }
}
