using FluentAssertions;
using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Tests.Domain;

public class QueryResultTests
{
    [Fact]
    public void QueryResult_ShouldInitializeWithDefaults()
    {
        // Arrange & Act
        var result = new QueryResult();

        // Assert
        result.Rows.Should().BeEmpty();
        result.RowCount.Should().Be(0);
        result.AffectedRows.Should().Be(0);
        result.ExecutionTimeMs.Should().Be(0);
    }

    [Fact]
    public void QueryResult_ShouldHoldData()
    {
        // Arrange
        var rows = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { { "Id", 1 }, { "Name", "Test" } },
            new Dictionary<string, object?> { { "Id", 2 }, { "Name", "Test2" } }
        };

        // Act
        var result = new QueryResult
        {
            Rows = rows,
            RowCount = 2,
            ExecutionTimeMs = 100
        };

        // Assert
        result.Rows.Should().HaveCount(2);
        result.RowCount.Should().Be(2);
        result.ExecutionTimeMs.Should().Be(100);
    }
}
