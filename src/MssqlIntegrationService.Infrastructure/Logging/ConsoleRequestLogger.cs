using System.Text.Json;
using Microsoft.Extensions.Options;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Infrastructure.Logging;

/// <summary>
/// Console/Terminal logger implementation
/// </summary>
public class ConsoleRequestLogger : IRequestLogger
{
    private readonly ConsoleLoggingOptions _options;
    private readonly bool _masterEnabled;
    private static readonly object _lock = new();

    public ConsoleRequestLogger(IOptions<LoggingOptions> options)
    {
        _options = options.Value.Console;
        _masterEnabled = options.Value.Enabled;
    }

    public string LoggerType => "Console";
    public bool IsEnabled => _masterEnabled && _options.Enabled;

    public Task LogAsync(RequestLog log, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return Task.CompletedTask;

        lock (_lock)
        {
            if (_options.Format == "Detailed")
            {
                WriteDetailedLog(log);
            }
            else
            {
                WriteSimpleLog(log);
            }
        }

        return Task.CompletedTask;
    }

    private void WriteSimpleLog(RequestLog log)
    {
        var statusColor = GetStatusColor(log.StatusCode);
        var methodColor = GetMethodColor(log.HttpMethod);

        if (_options.UseColors)
        {
            Console.Write($"[{log.Timestamp:HH:mm:ss}] ");
            WriteColored(log.HttpMethod.PadRight(7), methodColor);
            Console.Write($" {log.Path} ");
            WriteColored($"{log.StatusCode}", statusColor);
            Console.WriteLine($" {log.ResponseTimeMs}ms");

            if (!string.IsNullOrEmpty(log.ErrorMessage))
            {
                WriteColored($"           ERROR: {log.ErrorMessage}\n", ConsoleColor.Red);
            }
        }
        else
        {
            Console.WriteLine($"[{log.Timestamp:HH:mm:ss}] {log.HttpMethod.PadRight(7)} {log.Path} {log.StatusCode} {log.ResponseTimeMs}ms");
            if (!string.IsNullOrEmpty(log.ErrorMessage))
            {
                Console.WriteLine($"           ERROR: {log.ErrorMessage}");
            }
        }
    }

    private void WriteDetailedLog(RequestLog log)
    {
        var separator = new string('-', 80);
        
        if (_options.UseColors)
        {
            WriteColored(separator + "\n", ConsoleColor.DarkGray);
        }
        else
        {
            Console.WriteLine(separator);
        }

        Console.WriteLine($"Timestamp:     {log.Timestamp:yyyy-MM-dd HH:mm:ss.fff}");
        Console.WriteLine($"Request ID:    {log.Id}");
        
        if (_options.UseColors)
        {
            Console.Write("Method:        ");
            WriteColored($"{log.HttpMethod}\n", GetMethodColor(log.HttpMethod));
        }
        else
        {
            Console.WriteLine($"Method:        {log.HttpMethod}");
        }
        
        Console.WriteLine($"Path:          {log.Path}");
        
        if (!string.IsNullOrEmpty(log.QueryString))
        {
            Console.WriteLine($"Query:         {log.QueryString}");
        }
        
        Console.WriteLine($"Client IP:     {log.ClientIp}");
        Console.WriteLine($"User Agent:    {log.UserAgent}");

        if (_options.UseColors)
        {
            Console.Write("Status:        ");
            WriteColored($"{log.StatusCode}\n", GetStatusColor(log.StatusCode));
        }
        else
        {
            Console.WriteLine($"Status:        {log.StatusCode}");
        }
        
        Console.WriteLine($"Duration:      {log.ResponseTimeMs}ms");

        if (!string.IsNullOrEmpty(log.RequestBody))
        {
            Console.WriteLine($"Request Body:  {TruncateBody(log.RequestBody, 500)}");
        }

        if (!string.IsNullOrEmpty(log.ResponseBody))
        {
            Console.WriteLine($"Response Body: {TruncateBody(log.ResponseBody, 500)}");
        }

        if (!string.IsNullOrEmpty(log.ErrorMessage))
        {
            if (_options.UseColors)
            {
                WriteColored($"Error:         {log.ErrorMessage}\n", ConsoleColor.Red);
            }
            else
            {
                Console.WriteLine($"Error:         {log.ErrorMessage}");
            }
        }

        if (_options.UseColors)
        {
            WriteColored(separator + "\n", ConsoleColor.DarkGray);
        }
        else
        {
            Console.WriteLine(separator);
        }
    }

    private static void WriteColored(string text, ConsoleColor color)
    {
        var originalColor = Console.ForegroundColor;
        Console.ForegroundColor = color;
        Console.Write(text);
        Console.ForegroundColor = originalColor;
    }

    private static ConsoleColor GetStatusColor(int statusCode) => statusCode switch
    {
        >= 200 and < 300 => ConsoleColor.Green,
        >= 300 and < 400 => ConsoleColor.Cyan,
        >= 400 and < 500 => ConsoleColor.Yellow,
        >= 500 => ConsoleColor.Red,
        _ => ConsoleColor.White
    };

    private static ConsoleColor GetMethodColor(string method) => method.ToUpper() switch
    {
        "GET" => ConsoleColor.Blue,
        "POST" => ConsoleColor.Green,
        "PUT" => ConsoleColor.Yellow,
        "DELETE" => ConsoleColor.Red,
        "PATCH" => ConsoleColor.Magenta,
        _ => ConsoleColor.White
    };

    private static string TruncateBody(string body, int maxLength)
    {
        if (body.Length <= maxLength) return body;
        return body[..maxLength] + "... [truncated]";
    }
}
