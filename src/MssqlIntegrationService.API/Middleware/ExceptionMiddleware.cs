using System.Net;
using System.Text.Json;
using MssqlIntegrationService.Domain.Exceptions;

namespace MssqlIntegrationService.API.Middleware;

public class ExceptionMiddleware
{
    private readonly RequestDelegate _next;
    private readonly ILogger<ExceptionMiddleware> _logger;
    private readonly IHostEnvironment _environment;

    public ExceptionMiddleware(RequestDelegate next, ILogger<ExceptionMiddleware> logger, IHostEnvironment environment)
    {
        _next = next;
        _logger = logger;
        _environment = environment;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        try
        {
            await _next(context);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "An unhandled exception occurred");
            await HandleExceptionAsync(context, ex);
        }
    }

    private async Task HandleExceptionAsync(HttpContext context, Exception exception)
    {
        context.Response.ContentType = "application/json";
        
        var (statusCode, errorMessage, errors) = exception switch
        {
            ValidationException validationEx => (
                (int)HttpStatusCode.BadRequest,
                validationEx.Message,
                validationEx.Errors
            ),
            NotFoundException => (
                (int)HttpStatusCode.NotFound,
                exception.Message,
                (IReadOnlyList<string>?)null
            ),
            DatabaseException dbEx => (
                (int)HttpStatusCode.ServiceUnavailable,
                dbEx.Message,
                (IReadOnlyList<string>?)null
            ),
            _ => (
                (int)HttpStatusCode.InternalServerError,
                _environment.IsDevelopment() ? exception.Message : "An unexpected error occurred.",
                (IReadOnlyList<string>?)null
            )
        };

        context.Response.StatusCode = statusCode;

        var response = new
        {
            Success = false,
            ErrorMessage = errorMessage,
            ErrorCode = statusCode,
            Errors = errors
        };

        var options = new JsonSerializerOptions 
        { 
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = System.Text.Json.Serialization.JsonIgnoreCondition.WhenWritingNull
        };
        await context.Response.WriteAsync(JsonSerializer.Serialize(response, options));
    }
}

public static class ExceptionMiddlewareExtensions
{
    public static IApplicationBuilder UseExceptionMiddleware(this IApplicationBuilder builder)
    {
        return builder.UseMiddleware<ExceptionMiddleware>();
    }
}
