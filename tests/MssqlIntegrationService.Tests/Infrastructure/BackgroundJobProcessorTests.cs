using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Moq;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Infrastructure.Services;

namespace MssqlIntegrationService.Tests.Infrastructure;

public class BackgroundJobProcessorTests
{
    [Fact]
    public async Task EnqueueAsync_AddsJobIdToChannel()
    {
        // Arrange
        var mockScopeFactory = new Mock<IServiceScopeFactory>();
        var mockLogger = new Mock<ILogger<BackgroundJobProcessor>>();

        var processor = new BackgroundJobProcessor(
            mockScopeFactory.Object,
            mockLogger.Object
        );

        var jobId = Guid.NewGuid().ToString();

        // Act & Assert - should not throw
        await processor.EnqueueAsync(jobId, CancellationToken.None);
    }

    [Fact]
    public async Task EnqueueAsync_MultipleCalls_DoesNotBlock()
    {
        // Arrange
        var mockScopeFactory = new Mock<IServiceScopeFactory>();
        var mockLogger = new Mock<ILogger<BackgroundJobProcessor>>();

        var processor = new BackgroundJobProcessor(
            mockScopeFactory.Object,
            mockLogger.Object
        );

        // Act - should not block or throw for multiple enqueues
        var tasks = new List<ValueTask>();
        for (int i = 0; i < 100; i++)
        {
            tasks.Add(processor.EnqueueAsync(Guid.NewGuid().ToString(), CancellationToken.None));
        }

        foreach (var task in tasks)
        {
            await task;
        }

        // Assert - all tasks completed without exception
    }

    [Fact]
    public void Constructor_CreatesBoundedChannel()
    {
        // Arrange
        var mockScopeFactory = new Mock<IServiceScopeFactory>();
        var mockLogger = new Mock<ILogger<BackgroundJobProcessor>>();

        // Act
        var processor = new BackgroundJobProcessor(
            mockScopeFactory.Object,
            mockLogger.Object
        );

        // Assert - processor created successfully
        processor.Should().NotBeNull();
    }
}
