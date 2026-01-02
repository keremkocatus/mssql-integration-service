using FluentAssertions;
using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Tests.Domain;

public class TransferResultTests
{
    [Fact]
    public void TransferResult_ShouldInitializeWithDefaults()
    {
        // Arrange & Act
        var result = new TransferResult();

        // Assert
        result.TotalRowsRead.Should().Be(0);
        result.TotalRowsWritten.Should().Be(0);
        result.ExecutionTimeMs.Should().Be(0);
        result.Warnings.Should().BeEmpty();
    }

    [Fact]
    public void TransferOptions_ShouldHaveCorrectDefaults()
    {
        // Arrange & Act
        var options = new TransferOptions();

        // Assert
        options.BatchSize.Should().Be(1000);
        options.Timeout.Should().Be(300);
        options.TruncateTargetTable.Should().BeFalse();
        options.CreateTableIfNotExists.Should().BeFalse();
        options.UseTransaction.Should().BeTrue();
        options.ColumnMappings.Should().BeNull();
    }

    [Fact]
    public void BulkInsertOptions_ShouldHaveCorrectDefaults()
    {
        // Arrange & Act
        var options = new BulkInsertOptions();

        // Assert
        options.BatchSize.Should().Be(1000);
        options.Timeout.Should().Be(300);
        options.UseTransaction.Should().BeTrue();
    }
}
