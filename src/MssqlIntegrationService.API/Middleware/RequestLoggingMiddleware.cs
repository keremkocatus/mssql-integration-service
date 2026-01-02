using System.Diagnostics;
using System.Text;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Options;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Infrastructure.Logging;

namespace MssqlIntegrationService.API.Middleware;

/// <summary>
/// Request logging middleware - logs all HTTP requests to configured destinations
/// </summary>
public class RequestLoggingMiddleware
{
    private readonly RequestDelegate _next;
    private readonly LoggingOptions _options;
    private readonly IEnumerable<IRequestLogger> _loggers;

    public RequestLoggingMiddleware(
        RequestDelegate next,
        IOptions<LoggingOptions> options,
        IEnumerable<IRequestLogger> loggers)
    {
        _next = next;
        _options = options.Value;
        _loggers = loggers;
    }

    public async Task InvokeAsync(HttpContext context)
    {
        // Skip if logging is disabled
        if (!_options.Enabled)
        {
            await _next(context);
            return;
        }

        // Skip excluded paths
        var path = context.Request.Path.Value ?? "";
        if (_options.ExcludePaths.Any(p => path.StartsWith(p, StringComparison.OrdinalIgnoreCase)))
        {
            await _next(context);
            return;
        }

        var stopwatch = Stopwatch.StartNew();
        var log = new RequestLog
        {
            HttpMethod = context.Request.Method,
            Path = path,
            QueryString = context.Request.QueryString.Value ?? string.Empty,
            ClientIp = GetClientIp(context),
            UserAgent = context.Request.Headers.UserAgent.FirstOrDefault()
        };

        // Log headers if enabled
        if (_options.LogHeaders)
        {
            foreach (var header in context.Request.Headers)
            {
                // Skip sensitive headers
                if (!IsSensitiveHeader(header.Key))
                {
                    log.RequestHeaders[header.Key] = header.Value.ToString();
                }
            }
        }

        // Read request body if enabled
        if (_options.LogRequestBody && context.Request.ContentLength > 0)
        {
            log.RequestBody = await ReadRequestBodyAsync(context);
        }

        // Replace response body stream to capture response
        var originalBodyStream = context.Response.Body;
        using var responseBodyStream = new MemoryStream();
        context.Response.Body = responseBodyStream;

        try
        {
            await _next(context);
        }
        catch (Exception ex)
        {
            log.ErrorMessage = ex.Message;
            log.StackTrace = ex.StackTrace;
            throw;
        }
        finally
        {
            stopwatch.Stop();
            log.ResponseTimeMs = stopwatch.ElapsedMilliseconds;
            log.StatusCode = context.Response.StatusCode;

            // Read response body if enabled
            if (_options.LogResponseBody)
            {
                log.ResponseBody = await ReadResponseBodyAsync(responseBodyStream);
            }

            // Copy response body back to original stream
            responseBodyStream.Seek(0, SeekOrigin.Begin);
            await responseBodyStream.CopyToAsync(originalBodyStream);
            context.Response.Body = originalBodyStream;

            // Log to all enabled loggers
            await LogToAllAsync(log, context.RequestAborted);
        }
    }

    private async Task LogToAllAsync(RequestLog log, CancellationToken cancellationToken)
    {
        var tasks = _loggers
            .Where(l => l.IsEnabled)
            .Select(l => SafeLogAsync(l, log, cancellationToken));

        await Task.WhenAll(tasks);
    }

    private static async Task SafeLogAsync(IRequestLogger logger, RequestLog log, CancellationToken cancellationToken)
    {
        try
        {
            await logger.LogAsync(log, cancellationToken);
        }
        catch (Exception ex)
        {
            // Don't let logging failures affect the request
            Console.WriteLine($"[{logger.LoggerType}Logger] Error: {ex.Message}");
        }
    }

    private async Task<string?> ReadRequestBodyAsync(HttpContext context)
    {
        try
        {
            context.Request.EnableBuffering();
            context.Request.Body.Position = 0;

            using var reader = new StreamReader(
                context.Request.Body,
                encoding: Encoding.UTF8,
                detectEncodingFromByteOrderMarks: false,
                bufferSize: 1024,
                leaveOpen: true);

            var body = await reader.ReadToEndAsync();
            context.Request.Body.Position = 0;

            if (body.Length > _options.MaxBodyLength)
            {
                return body[.._options.MaxBodyLength] + "... [truncated]";
            }

            return body;
        }
        catch
        {
            return null;
        }
    }

    private async Task<string?> ReadResponseBodyAsync(MemoryStream responseBodyStream)
    {
        try
        {
            responseBodyStream.Seek(0, SeekOrigin.Begin);
            var body = await new StreamReader(responseBodyStream).ReadToEndAsync();
            responseBodyStream.Seek(0, SeekOrigin.Begin);

            if (body.Length > _options.MaxBodyLength)
            {
                return body[.._options.MaxBodyLength] + "... [truncated]";
            }

            return body;
        }
        catch
        {
            return null;
        }
    }

    private static string? GetClientIp(HttpContext context)
    {
        // Check for forwarded header (behind proxy/load balancer)
        var forwardedFor = context.Request.Headers["X-Forwarded-For"].FirstOrDefault();
        if (!string.IsNullOrEmpty(forwardedFor))
        {
            return forwardedFor.Split(',').First().Trim();
        }

        return context.Connection.RemoteIpAddress?.ToString();
    }

    private static bool IsSensitiveHeader(string headerName)
    {
        var sensitiveHeaders = new[]
        {
            "Authorization",
            "Cookie",
            "Set-Cookie",
            "X-API-Key",
            "Api-Key",
            "X-Auth-Token"
        };

        return sensitiveHeaders.Contains(headerName, StringComparer.OrdinalIgnoreCase);
    }
}

/// <summary>
/// Extension methods for request logging middleware
/// </summary>
public static class RequestLoggingMiddlewareExtensions
{
    public static IApplicationBuilder UseRequestLogging(this IApplicationBuilder builder)
    {
        return builder.UseMiddleware<RequestLoggingMiddleware>();
    }
}
