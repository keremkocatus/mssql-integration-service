using FluentAssertions;
using Moq;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Services;
using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Tests.Application;

public class MongoToMssqlAppServiceTests
{
    private readonly Mock<IMongoToMssqlService> _mockMongoToMssqlService;
    private readonly MongoToMssqlAppService _service;

    public MongoToMssqlAppServiceTests()
    {
        _mockMongoToMssqlService = new Mock<IMongoToMssqlService>();
        _service = new MongoToMssqlAppService(_mockMongoToMssqlService.Object);
    }

    [Fact]
    public async Task TransferAsync_WithValidRequest_ReturnsSuccessResponse()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users",
                Filter = "{ \"status\": \"active\" }"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "dbo.Users"
            }
        };

        var expectedResult = new MongoToMssqlResult
        {
            SourceCollection = "users",
            TargetTable = "dbo.Users",
            TotalDocumentsRead = 100,
            TotalRowsWritten = 100,
            FailedDocuments = 0,
            ExecutionTimeMs = 500
        };

        _mockMongoToMssqlService
            .Setup(x => x.TransferAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<MongoToMssqlResult>.Success(expectedResult));

        // Act
        var response = await _service.TransferAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.TotalDocumentsRead.Should().Be(100);
        response.TotalRowsWritten.Should().Be(100);
        response.SourceCollection.Should().Be("users");
        response.TargetTable.Should().Be("dbo.Users");
    }

    [Fact]
    public async Task TransferAsync_WithAggregationPipeline_CallsAggregationMethod()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "orders",
                AggregationPipeline = "[{ \"$match\": { \"status\": \"completed\" } }]"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "CompletedOrders"
            }
        };

        var expectedResult = new MongoToMssqlResult
        {
            SourceCollection = "orders",
            TargetTable = "CompletedOrders",
            TotalDocumentsRead = 50,
            TotalRowsWritten = 50
        };

        _mockMongoToMssqlService
            .Setup(x => x.TransferWithAggregationAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<MongoToMssqlResult>.Success(expectedResult));

        // Act
        var response = await _service.TransferAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.TotalDocumentsRead.Should().Be(50);
        
        _mockMongoToMssqlService.Verify(
            x => x.TransferWithAggregationAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                "[{ \"$match\": { \"status\": \"completed\" } }]",
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()),
            Times.Once);
    }

    [Fact]
    public async Task TransferAsync_WithInvalidTableName_ReturnsValidationError()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Users; DROP TABLE Users--"  // SQL Injection attempt
            }
        };

        // Act
        var response = await _service.TransferAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("Invalid target table name");
    }

    [Fact]
    public async Task TransferAsync_WithInvalidFieldMapping_ReturnsValidationError()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Users"
            },
            Options = new MongoToMssqlOptionsDto
            {
                FieldMappings = new Dictionary<string, string>
                {
                    { "name", "Name; DROP TABLE--" }  // Invalid column name
                }
            }
        };

        // Act
        var response = await _service.TransferAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("Invalid MSSQL column name");
    }

    [Fact]
    public async Task TransferAsync_WhenServiceFails_ReturnsErrorResponse()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://invalid:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Users"
            }
        };

        _mockMongoToMssqlService
            .Setup(x => x.TransferAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<MongoToMssqlResult>.Failure("MongoDB Error: Connection refused"));

        // Act
        var response = await _service.TransferAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("MongoDB Error");
    }

    [Fact]
    public async Task TransferAsync_WithOptions_PassesOptionsCorrectly()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Users"
            },
            Options = new MongoToMssqlOptionsDto
            {
                BatchSize = 500,
                Timeout = 600,
                TruncateTargetTable = true,
                CreateTableIfNotExists = true,
                FlattenNestedDocuments = true,
                FlattenSeparator = "__",
                ArrayHandling = "Skip",
                FieldMappings = new Dictionary<string, string>
                {
                    { "firstName", "FirstName" },
                    { "lastName", "LastName" }
                },
                IncludeFields = new List<string> { "firstName", "lastName", "email" },
                ExcludeFields = new List<string> { "password" }
            }
        };

        var expectedResult = new MongoToMssqlResult
        {
            TotalDocumentsRead = 10,
            TotalRowsWritten = 10
        };

        MongoToMssqlOptions? capturedOptions = null;
        _mockMongoToMssqlService
            .Setup(x => x.TransferAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, string, string?, string, string, MongoToMssqlOptions?, CancellationToken>(
                (_, _, _, _, _, _, options, _) => capturedOptions = options)
            .ReturnsAsync(Result<MongoToMssqlResult>.Success(expectedResult));

        // Act
        await _service.TransferAsync(request);

        // Assert
        capturedOptions.Should().NotBeNull();
        capturedOptions!.BatchSize.Should().Be(500);
        capturedOptions.Timeout.Should().Be(600);
        capturedOptions.TruncateTargetTable.Should().BeTrue();
        capturedOptions.CreateTableIfNotExists.Should().BeTrue();
        capturedOptions.FlattenNestedDocuments.Should().BeTrue();
        capturedOptions.FlattenSeparator.Should().Be("__");
        capturedOptions.ArrayHandling.Should().Be("Skip");
        capturedOptions.FieldMappings.Should().ContainKey("firstName");
        capturedOptions.IncludeFields.Should().Contain("email");
        capturedOptions.ExcludeFields.Should().Contain("password");
    }

    [Fact]
    public async Task TransferAsync_WithWarnings_ReturnsWarningsInResponse()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Users"
            }
        };

        var expectedResult = new MongoToMssqlResult
        {
            TotalDocumentsRead = 100,
            TotalRowsWritten = 95,
            FailedDocuments = 5,
            Warnings = new List<string>
            {
                "5 documents failed due to schema mismatch",
                "Table 'Users' created"
            }
        };

        _mockMongoToMssqlService
            .Setup(x => x.TransferAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<MongoToMssqlResult>.Success(expectedResult));

        // Act
        var response = await _service.TransferAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.FailedDocuments.Should().Be(5);
        response.Warnings.Should().HaveCount(2);
        response.Warnings.Should().Contain("5 documents failed due to schema mismatch");
    }

    #region TransferAsJson Tests

    [Fact]
    public async Task TransferAsJsonAsync_WithValidRequest_ReturnsSuccessResponse()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users",
                Filter = "{ \"status\": \"active\" }"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Users"
            }
        };

        var expectedResult = new MongoToMssqlResult
        {
            SourceCollection = "users",
            TargetTable = "Users_JSON",
            TotalDocumentsRead = 100,
            TotalRowsWritten = 100,
            FailedDocuments = 0,
            ExecutionTimeMs = 300
        };

        _mockMongoToMssqlService
            .Setup(x => x.TransferAsJsonAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<MongoToMssqlResult>.Success(expectedResult));

        // Act
        var response = await _service.TransferAsJsonAsync(request);

        // Assert
        response.Success.Should().BeTrue();
        response.TotalDocumentsRead.Should().Be(100);
        response.TotalRowsWritten.Should().Be(100);
        response.SourceCollection.Should().Be("users");
        response.TargetTable.Should().Be("Users_JSON");
    }

    [Fact]
    public async Task TransferAsJsonAsync_WithInvalidTableName_ReturnsValidationError()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Invalid;Table--Name"
            }
        };

        // Act
        var response = await _service.TransferAsJsonAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Contain("Invalid target table name");
    }

    [Fact]
    public async Task TransferAsJsonAsync_WithOptions_PassesOptionsCorrectly()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Users"
            },
            Options = new MongoToMssqlOptionsDto
            {
                BatchSize = 5000,
                Timeout = 600,
                TruncateTargetTable = true,
                UseTransaction = false
            }
        };

        var expectedResult = new MongoToMssqlResult
        {
            SourceCollection = "users",
            TargetTable = "Users_JSON",
            TotalDocumentsRead = 50,
            TotalRowsWritten = 50
        };

        MongoToMssqlOptions? capturedOptions = null;
        _mockMongoToMssqlService
            .Setup(x => x.TransferAsJsonAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, string, string, string?, string, string, MongoToMssqlOptions?, CancellationToken>(
                (_, _, _, _, _, _, options, _) => capturedOptions = options)
            .ReturnsAsync(Result<MongoToMssqlResult>.Success(expectedResult));

        // Act
        await _service.TransferAsJsonAsync(request);

        // Assert
        capturedOptions.Should().NotBeNull();
        capturedOptions!.BatchSize.Should().Be(5000);
        capturedOptions.Timeout.Should().Be(600);
        capturedOptions.TruncateTargetTable.Should().BeTrue();
        capturedOptions.UseTransaction.Should().BeFalse();
    }

    [Fact]
    public async Task TransferAsJsonAsync_WhenServiceFails_ReturnsErrorResponse()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "users"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Users"
            }
        };

        _mockMongoToMssqlService
            .Setup(x => x.TransferAsJsonAsync(
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<string?>(),
                It.IsAny<string>(),
                It.IsAny<string>(),
                It.IsAny<MongoToMssqlOptions?>(),
                It.IsAny<CancellationToken>()))
            .ReturnsAsync(Result<MongoToMssqlResult>.Failure("MongoDB connection failed"));

        // Act
        var response = await _service.TransferAsJsonAsync(request);

        // Assert
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Be("MongoDB connection failed");
    }

    #endregion
}
