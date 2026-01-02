using System.Net;
using System.Text.Json;
using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Moq;
using MssqlIntegrationService.API.Middleware;
using MssqlIntegrationService.Domain.Exceptions;

namespace MssqlIntegrationService.Tests.Middleware;

public class ExceptionMiddlewareTests
{
    private readonly Mock<ILogger<ExceptionMiddleware>> _loggerMock;
    private readonly Mock<IHostEnvironment> _environmentMock;

    public ExceptionMiddlewareTests()
    {
        _loggerMock = new Mock<ILogger<ExceptionMiddleware>>();
        _environmentMock = new Mock<IHostEnvironment>();
    }

    [Fact]
    public async Task InvokeAsync_NoException_ShouldCallNext()
    {
        // Arrange
        var context = new DefaultHttpContext();
        var nextCalled = false;
        RequestDelegate next = _ =>
        {
            nextCalled = true;
            return Task.CompletedTask;
        };

        var middleware = new ExceptionMiddleware(next, _loggerMock.Object, _environmentMock.Object);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        nextCalled.Should().BeTrue();
    }

    [Fact]
    public async Task InvokeAsync_ValidationException_ShouldReturn400()
    {
        // Arrange
        var context = CreateHttpContext();
        var exception = new ValidationException("Invalid input");
        RequestDelegate next = _ => throw exception;

        var middleware = new ExceptionMiddleware(next, _loggerMock.Object, _environmentMock.Object);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        context.Response.StatusCode.Should().Be((int)HttpStatusCode.BadRequest);
        var response = await ReadResponseAsync(context);
        response.Should().Contain("Invalid input");
    }

    [Fact]
    public async Task InvokeAsync_ValidationExceptionWithMultipleErrors_ShouldReturnErrors()
    {
        // Arrange
        var context = CreateHttpContext();
        var errors = new[] { "Error 1", "Error 2", "Error 3" };
        var exception = new ValidationException(errors);
        RequestDelegate next = _ => throw exception;

        var middleware = new ExceptionMiddleware(next, _loggerMock.Object, _environmentMock.Object);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        context.Response.StatusCode.Should().Be((int)HttpStatusCode.BadRequest);
        var response = await ReadResponseAsync(context);
        response.Should().Contain("Error 1");
        response.Should().Contain("Error 2");
        response.Should().Contain("Error 3");
    }

    [Fact]
    public async Task InvokeAsync_NotFoundException_ShouldReturn404()
    {
        // Arrange
        var context = CreateHttpContext();
        var exception = new NotFoundException("User", 123);
        RequestDelegate next = _ => throw exception;

        var middleware = new ExceptionMiddleware(next, _loggerMock.Object, _environmentMock.Object);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        context.Response.StatusCode.Should().Be((int)HttpStatusCode.NotFound);
        var response = await ReadResponseAsync(context);
        response.Should().Contain("User");
        response.Should().Contain("123");
    }

    [Fact]
    public async Task InvokeAsync_DatabaseException_ShouldReturn503()
    {
        // Arrange
        var context = CreateHttpContext();
        var exception = new DatabaseException("Connection failed", "CONN_001");
        RequestDelegate next = _ => throw exception;

        var middleware = new ExceptionMiddleware(next, _loggerMock.Object, _environmentMock.Object);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        context.Response.StatusCode.Should().Be((int)HttpStatusCode.ServiceUnavailable);
        var response = await ReadResponseAsync(context);
        response.Should().Contain("Connection failed");
    }

    [Fact]
    public async Task InvokeAsync_GenericException_InDevelopment_ShouldReturnDetails()
    {
        // Arrange
        _environmentMock.Setup(e => e.EnvironmentName).Returns("Development");
        var context = CreateHttpContext();
        var exception = new InvalidOperationException("Detailed error message");
        RequestDelegate next = _ => throw exception;

        var middleware = new ExceptionMiddleware(next, _loggerMock.Object, _environmentMock.Object);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        context.Response.StatusCode.Should().Be((int)HttpStatusCode.InternalServerError);
        var response = await ReadResponseAsync(context);
        response.Should().Contain("Detailed error message");
    }

    [Fact]
    public async Task InvokeAsync_GenericException_InProduction_ShouldHideDetails()
    {
        // Arrange
        _environmentMock.Setup(e => e.EnvironmentName).Returns("Production");
        var context = CreateHttpContext();
        var exception = new InvalidOperationException("Sensitive error details");
        RequestDelegate next = _ => throw exception;

        var middleware = new ExceptionMiddleware(next, _loggerMock.Object, _environmentMock.Object);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        context.Response.StatusCode.Should().Be((int)HttpStatusCode.InternalServerError);
        var response = await ReadResponseAsync(context);
        response.Should().NotContain("Sensitive error details");
        response.Should().Contain("An unexpected error occurred");
    }

    [Fact]
    public async Task InvokeAsync_Exception_ShouldLogError()
    {
        // Arrange
        var context = CreateHttpContext();
        var exception = new Exception("Test error");
        RequestDelegate next = _ => throw exception;

        var middleware = new ExceptionMiddleware(next, _loggerMock.Object, _environmentMock.Object);

        // Act
        await middleware.InvokeAsync(context);

        // Assert
        _loggerMock.Verify(
            x => x.Log(
                LogLevel.Error,
                It.IsAny<EventId>(),
                It.Is<It.IsAnyType>((v, t) => true),
                exception,
                It.IsAny<Func<It.IsAnyType, Exception?, string>>()),
            Times.Once);
    }

    private static DefaultHttpContext CreateHttpContext()
    {
        var context = new DefaultHttpContext();
        context.Response.Body = new MemoryStream();
        return context;
    }

    private static async Task<string> ReadResponseAsync(HttpContext context)
    {
        context.Response.Body.Seek(0, SeekOrigin.Begin);
        using var reader = new StreamReader(context.Response.Body);
        return await reader.ReadToEndAsync();
    }
}
