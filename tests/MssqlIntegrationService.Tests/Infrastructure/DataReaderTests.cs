using System.Data;
using System.Data.Common;
using FluentAssertions;
using Moq;
using MssqlIntegrationService.Infrastructure.Data;

namespace MssqlIntegrationService.Tests.Infrastructure;

public class DataReaderTests
{
    #region RowCountingDataReader Tests

    [Fact]
    public void RowCountingDataReader_Constructor_WithNullReader_ThrowsArgumentNullException()
    {
        // Act & Assert
        var act = () => new RowCountingDataReader(null!);
        act.Should().Throw<ArgumentNullException>()
            .WithParameterName("innerReader");
    }

    [Fact]
    public void RowCountingDataReader_Read_IncrementsRowCount()
    {
        // Arrange
        var mockReader = CreateMockDataReader(3);
        using var countingReader = new RowCountingDataReader(mockReader.Object);

        // Act & Assert
        countingReader.RowCount.Should().Be(0);
        
        countingReader.Read().Should().BeTrue();
        countingReader.RowCount.Should().Be(1);
        
        countingReader.Read().Should().BeTrue();
        countingReader.RowCount.Should().Be(2);
        
        countingReader.Read().Should().BeTrue();
        countingReader.RowCount.Should().Be(3);
        
        countingReader.Read().Should().BeFalse();
        countingReader.RowCount.Should().Be(3); // Should not increment on false
    }

    [Fact]
    public async Task RowCountingDataReader_ReadAsync_IncrementsRowCount()
    {
        // Arrange
        var mockReader = CreateMockDataReaderAsync(2);
        using var countingReader = new RowCountingDataReader(mockReader.Object);

        // Act & Assert
        (await countingReader.ReadAsync(CancellationToken.None)).Should().BeTrue();
        countingReader.RowCount.Should().Be(1);
        
        (await countingReader.ReadAsync(CancellationToken.None)).Should().BeTrue();
        countingReader.RowCount.Should().Be(2);
        
        (await countingReader.ReadAsync(CancellationToken.None)).Should().BeFalse();
        countingReader.RowCount.Should().Be(2);
    }

    [Fact]
    public void RowCountingDataReader_DelegatesFieldCount()
    {
        // Arrange
        var mockReader = new Mock<DbDataReader>();
        mockReader.Setup(r => r.FieldCount).Returns(5);
        using var countingReader = new RowCountingDataReader(mockReader.Object);

        // Act & Assert
        countingReader.FieldCount.Should().Be(5);
    }

    [Fact]
    public void RowCountingDataReader_DelegatesGetName()
    {
        // Arrange
        var mockReader = new Mock<DbDataReader>();
        mockReader.Setup(r => r.GetName(0)).Returns("Id");
        mockReader.Setup(r => r.GetName(1)).Returns("Name");
        using var countingReader = new RowCountingDataReader(mockReader.Object);

        // Act & Assert
        countingReader.GetName(0).Should().Be("Id");
        countingReader.GetName(1).Should().Be("Name");
    }

    [Fact]
    public void RowCountingDataReader_DelegatesGetValue()
    {
        // Arrange
        var mockReader = new Mock<DbDataReader>();
        mockReader.Setup(r => r.GetValue(0)).Returns(42);
        mockReader.Setup(r => r.GetValue(1)).Returns("Test");
        using var countingReader = new RowCountingDataReader(mockReader.Object);

        // Act & Assert
        countingReader.GetValue(0).Should().Be(42);
        countingReader.GetValue(1).Should().Be("Test");
    }

    [Fact]
    public void RowCountingDataReader_DelegatesIsDBNull()
    {
        // Arrange
        var mockReader = new Mock<DbDataReader>();
        mockReader.Setup(r => r.IsDBNull(0)).Returns(false);
        mockReader.Setup(r => r.IsDBNull(1)).Returns(true);
        using var countingReader = new RowCountingDataReader(mockReader.Object);

        // Act & Assert
        countingReader.IsDBNull(0).Should().BeFalse();
        countingReader.IsDBNull(1).Should().BeTrue();
    }

    #endregion

    #region ObjectDataReader Tests

    [Fact]
    public void ObjectDataReader_Constructor_WithNullData_ThrowsArgumentNullException()
    {
        // Act & Assert
        var act = () => new ObjectDataReader<IDictionary<string, object?>>(null!);
        act.Should().Throw<ArgumentNullException>();
    }

    [Fact]
    public void ObjectDataReader_Read_IteratesThroughData()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Alice" },
            new Dictionary<string, object?> { ["Id"] = 2, ["Name"] = "Bob" },
            new Dictionary<string, object?> { ["Id"] = 3, ["Name"] = "Charlie" }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);

        // Act & Assert
        var rows = new List<(int Id, string Name)>();
        while (reader.Read())
        {
            rows.Add((
                (int)reader.GetValue(reader.GetOrdinal("Id")),
                (string)reader.GetValue(reader.GetOrdinal("Name"))
            ));
        }

        rows.Should().HaveCount(3);
        rows[0].Should().Be((1, "Alice"));
        rows[1].Should().Be((2, "Bob"));
        rows[2].Should().Be((3, "Charlie"));
    }

    [Fact]
    public void ObjectDataReader_RowCount_TracksReadRows()
    {
        // Arrange - WITHOUT explicit columns (auto-detect from first row)
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1 },
            new Dictionary<string, object?> { ["Id"] = 2 },
            new Dictionary<string, object?> { ["Id"] = 3 }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);

        // When columns are auto-detected, the first row is read during initialization
        // and RowCount starts at 1
        reader.RowCount.Should().Be(1);
        
        // First Read() returns the already-loaded first row
        reader.Read().Should().BeTrue();
        reader.GetValue(0).Should().Be(1);
        reader.RowCount.Should().Be(1); // Still 1 because first row was pre-loaded
        
        // Second Read() advances to the second row
        reader.Read().Should().BeTrue();
        reader.GetValue(0).Should().Be(2);
        reader.RowCount.Should().Be(2);
        
        // Third Read() advances to the third row
        reader.Read().Should().BeTrue();
        reader.GetValue(0).Should().Be(3);
        reader.RowCount.Should().Be(3);
        
        // No more rows
        reader.Read().Should().BeFalse();
        reader.RowCount.Should().Be(3);
    }
    
    [Fact]
    public void ObjectDataReader_WithExplicitColumns_RowCountStartsAtZero()
    {
        // Arrange - WITH explicit columns (no initialization read)
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1 },
            new Dictionary<string, object?> { ["Id"] = 2 }
        };
        var columns = new[] { "Id" };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data, columns);

        // With explicit columns, no row is read during initialization
        reader.RowCount.Should().Be(0);
        
        reader.Read().Should().BeTrue();
        reader.RowCount.Should().Be(1);
        
        reader.Read().Should().BeTrue();
        reader.RowCount.Should().Be(2);
        
        reader.Read().Should().BeFalse();
        reader.RowCount.Should().Be(2);
    }

    [Fact]
    public void ObjectDataReader_FieldCount_ReturnsColumnCount()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Test", ["Age"] = 25 }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);

        // Act & Assert
        reader.FieldCount.Should().Be(3);
    }

    [Fact]
    public void ObjectDataReader_GetName_ReturnsColumnName()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Test" }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);

        // Act & Assert
        reader.GetName(0).Should().Be("Id");
        reader.GetName(1).Should().Be("Name");
    }

    [Fact]
    public void ObjectDataReader_GetOrdinal_ReturnsColumnIndex()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Test" }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);

        // Act & Assert
        reader.GetOrdinal("Id").Should().Be(0);
        reader.GetOrdinal("Name").Should().Be(1);
        reader.GetOrdinal("name").Should().Be(1); // Case-insensitive
    }

    [Fact]
    public void ObjectDataReader_GetOrdinal_InvalidColumn_ThrowsIndexOutOfRange()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1 }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);

        // Act & Assert
        var act = () => reader.GetOrdinal("InvalidColumn");
        act.Should().Throw<IndexOutOfRangeException>();
    }

    [Fact]
    public void ObjectDataReader_GetValue_WithNullValue_ReturnsDBNull()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = null }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);
        reader.Read();

        // Act & Assert
        reader.GetValue(0).Should().Be(1);
        reader.GetValue(1).Should().Be(DBNull.Value);
    }

    [Fact]
    public void ObjectDataReader_GetValue_MissingKey_ReturnsDBNull()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1 }
        };
        var columns = new[] { "Id", "Name" }; // Name doesn't exist in data
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data, columns);
        reader.Read();

        // Act & Assert
        reader.GetValue(0).Should().Be(1);
        reader.GetValue(1).Should().Be(DBNull.Value);
    }

    [Fact]
    public void ObjectDataReader_WithExplicitColumns_UsesProvidedColumns()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Test", ["Age"] = 25 }
        };
        var columns = new[] { "Id", "Age" }; // Only include Id and Age
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data, columns);

        // Act & Assert
        reader.FieldCount.Should().Be(2);
        reader.GetName(0).Should().Be("Id");
        reader.GetName(1).Should().Be("Age");
    }

    [Fact]
    public void ObjectDataReader_EmptyData_ReturnsEmptyResult()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>();
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);

        // Act & Assert
        reader.FieldCount.Should().Be(0);
        reader.Read().Should().BeFalse();
    }

    [Fact]
    public void ObjectDataReader_IsDBNull_ReturnsCorrectValue()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = null }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);
        reader.Read();

        // Act & Assert
        reader.IsDBNull(0).Should().BeFalse();
        reader.IsDBNull(1).Should().BeTrue();
    }

    [Fact]
    public void ObjectDataReader_Indexer_ByOrdinal_ReturnsValue()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 42, ["Name"] = "Test" }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);
        reader.Read();

        // Act & Assert
        reader[0].Should().Be(42);
        reader[1].Should().Be("Test");
    }

    [Fact]
    public void ObjectDataReader_Indexer_ByName_ReturnsValue()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 42, ["Name"] = "Test" }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);
        reader.Read();

        // Act & Assert
        reader["Id"].Should().Be(42);
        reader["Name"].Should().Be("Test");
    }

    [Fact]
    public void ObjectDataReader_GetValues_FillsArray()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1, ["Name"] = "Test" }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);
        reader.Read();
        var values = new object[2];

        // Act
        var count = reader.GetValues(values);

        // Assert
        count.Should().Be(2);
        values[0].Should().Be(1);
        values[1].Should().Be("Test");
    }

    [Fact]
    public async Task ObjectDataReader_ReadAsync_IteratesData()
    {
        // Arrange
        var data = new List<IDictionary<string, object?>>
        {
            new Dictionary<string, object?> { ["Id"] = 1 },
            new Dictionary<string, object?> { ["Id"] = 2 }
        };
        using var reader = new ObjectDataReader<IDictionary<string, object?>>(data);

        // Act & Assert
        (await reader.ReadAsync(CancellationToken.None)).Should().BeTrue();
        reader.GetValue(0).Should().Be(1);
        
        (await reader.ReadAsync(CancellationToken.None)).Should().BeTrue();
        reader.GetValue(0).Should().Be(2);
        
        (await reader.ReadAsync(CancellationToken.None)).Should().BeFalse();
    }

    #endregion

    #region Helper Methods

    private static Mock<DbDataReader> CreateMockDataReader(int rowCount)
    {
        var mock = new Mock<DbDataReader>();
        var currentRow = 0;

        mock.Setup(r => r.Read())
            .Returns(() => currentRow++ < rowCount);

        return mock;
    }

    private static Mock<DbDataReader> CreateMockDataReaderAsync(int rowCount)
    {
        var mock = new Mock<DbDataReader>();
        var currentRow = 0;

        mock.Setup(r => r.ReadAsync(It.IsAny<CancellationToken>()))
            .ReturnsAsync(() => currentRow++ < rowCount);

        return mock;
    }

    #endregion
}
