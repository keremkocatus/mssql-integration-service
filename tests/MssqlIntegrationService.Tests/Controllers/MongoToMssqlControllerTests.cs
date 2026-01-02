using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Moq;
using MssqlIntegrationService.API.Controllers;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Application.Services;
using System.Text.Json;

namespace MssqlIntegrationService.Tests.Controllers;

public class MongoToMssqlControllerTests
{
    private readonly Mock<IMongoToMssqlAppService> _mockAppService;
    private readonly Mock<ILogger<MongoToMssqlController>> _mockLogger;
    private readonly MongoToMssqlController _controller;

    public MongoToMssqlControllerTests()
    {
        _mockAppService = new Mock<IMongoToMssqlAppService>();
        _mockLogger = new Mock<ILogger<MongoToMssqlController>>();
        _controller = new MongoToMssqlController(_mockAppService.Object, _mockLogger.Object);
    }

    [Fact]
    public async Task Transfer_WithValidRequest_ReturnsOkResult()
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

        var expectedResponse = new MongoToMssqlResponse
        {
            Success = true,
            SourceCollection = "users",
            TargetTable = "Users",
            TotalDocumentsRead = 100,
            TotalRowsWritten = 100,
            ExecutionTimeMs = 500
        };

        _mockAppService
            .Setup(x => x.TransferAsync(request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.Transfer(request, CancellationToken.None);

        // Assert
        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<MongoToMssqlResponse>().Subject;
        
        response.Success.Should().BeTrue();
        response.TotalDocumentsRead.Should().Be(100);
        response.TotalRowsWritten.Should().Be(100);
    }

    [Fact]
    public async Task Transfer_WhenServiceFails_ReturnsBadRequest()
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

        var expectedResponse = new MongoToMssqlResponse
        {
            Success = false,
            ErrorMessage = "MongoDB connection failed"
        };

        _mockAppService
            .Setup(x => x.TransferAsync(request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.Transfer(request, CancellationToken.None);

        // Assert
        var badRequestResult = result.Should().BeOfType<BadRequestObjectResult>().Subject;
        var response = badRequestResult.Value.Should().BeOfType<MongoToMssqlResponse>().Subject;
        
        response.Success.Should().BeFalse();
        response.ErrorMessage.Should().Be("MongoDB connection failed");
    }

    [Fact]
    public async Task Transfer_WithAggregationPipeline_ReturnsOkResult()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "analytics",
                CollectionName = "events",
                AggregationPipeline = @"[
                    { ""$match"": { ""type"": ""pageview"" } },
                    { ""$group"": { ""_id"": ""$page"", ""count"": { ""$sum"": 1 } } }
                ]"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "PageViews"
            }
        };

        var expectedResponse = new MongoToMssqlResponse
        {
            Success = true,
            TotalDocumentsRead = 1000,
            TotalRowsWritten = 50
        };

        _mockAppService
            .Setup(x => x.TransferAsync(request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.Transfer(request, CancellationToken.None);

        // Assert
        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<MongoToMssqlResponse>().Subject;
        
        response.Success.Should().BeTrue();
        response.TotalDocumentsRead.Should().Be(1000);
        response.TotalRowsWritten.Should().Be(50);  // Aggregated results
    }

    [Fact]
    public async Task Transfer_WithFieldMappings_PassesRequestCorrectly()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "customers"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "Customers"
            },
            Options = new MongoToMssqlOptionsDto
            {
                FieldMappings = new Dictionary<string, string>
                {
                    { "firstName", "FirstName" },
                    { "lastName", "LastName" },
                    { "emailAddress", "Email" }
                }
            }
        };

        var expectedResponse = new MongoToMssqlResponse { Success = true };

        MongoToMssqlRequest? capturedRequest = null;
        _mockAppService
            .Setup(x => x.TransferAsync(It.IsAny<MongoToMssqlRequest>(), It.IsAny<CancellationToken>()))
            .Callback<MongoToMssqlRequest, CancellationToken>((req, _) => capturedRequest = req)
            .ReturnsAsync(expectedResponse);

        // Act
        await _controller.Transfer(request, CancellationToken.None);

        // Assert
        capturedRequest.Should().NotBeNull();
        capturedRequest!.Options!.FieldMappings.Should().HaveCount(3);
        capturedRequest.Options.FieldMappings["firstName"].Should().Be("FirstName");
    }

    [Fact]
    public async Task Transfer_WithAllOptions_ProcessesCorrectly()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "inventory",
                CollectionName = "products",
                Filter = "{ \"inStock\": true }"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=warehouse;",
                TableName = "dbo.ActiveProducts"
            },
            Options = new MongoToMssqlOptionsDto
            {
                BatchSize = 1000,
                Timeout = 300,
                TruncateTargetTable = true,
                CreateTableIfNotExists = true,
                FlattenNestedDocuments = true,
                FlattenSeparator = "_",
                ArrayHandling = "Json",
                IncludeFields = new List<string> { "name", "price", "category" },
                ExcludeFields = new List<string> { "internalNotes" }
            }
        };

        var expectedResponse = new MongoToMssqlResponse
        {
            Success = true,
            TotalDocumentsRead = 500,
            TotalRowsWritten = 500,
            ExecutionTimeMs = 1200,
            Warnings = new List<string> { "Table 'dbo.ActiveProducts' was truncated" }
        };

        _mockAppService
            .Setup(x => x.TransferAsync(request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.Transfer(request, CancellationToken.None);

        // Assert
        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<MongoToMssqlResponse>().Subject;
        
        response.Success.Should().BeTrue();
        response.TotalRowsWritten.Should().Be(500);
        response.Warnings.Should().Contain("Table 'dbo.ActiveProducts' was truncated");
    }

    [Fact]
    public async Task Transfer_WithPartialFailure_ReturnsSuccessWithWarnings()
    {
        // Arrange
        var request = new MongoToMssqlRequest
        {
            Source = new MongoSourceConfig
            {
                ConnectionString = "mongodb://localhost:27017",
                DatabaseName = "testdb",
                CollectionName = "mixed_data"
            },
            Target = new MssqlTargetConfig
            {
                ConnectionString = "Server=localhost;Database=test;",
                TableName = "CleanedData"
            }
        };

        var expectedResponse = new MongoToMssqlResponse
        {
            Success = true,  // Overall success despite some failures
            TotalDocumentsRead = 100,
            TotalRowsWritten = 92,
            FailedDocuments = 8,
            Warnings = new List<string>
            {
                "8 documents skipped due to incompatible schema",
                "Array fields were serialized as JSON"
            }
        };

        _mockAppService
            .Setup(x => x.TransferAsync(request, It.IsAny<CancellationToken>()))
            .ReturnsAsync(expectedResponse);

        // Act
        var result = await _controller.Transfer(request, CancellationToken.None);

        // Assert
        var okResult = result.Should().BeOfType<OkObjectResult>().Subject;
        var response = okResult.Value.Should().BeOfType<MongoToMssqlResponse>().Subject;
        
        response.Success.Should().BeTrue();
        response.FailedDocuments.Should().Be(8);
        response.TotalRowsWritten.Should().Be(92);
        response.Warnings.Should().HaveCount(2);
    }
}
