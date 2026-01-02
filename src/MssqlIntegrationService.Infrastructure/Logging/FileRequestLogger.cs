using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.Extensions.Options;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;

namespace MssqlIntegrationService.Infrastructure.Logging;

/// <summary>
/// File logger implementation (supports txt and json formats)
/// </summary>
public class FileRequestLogger : IRequestLogger, IDisposable
{
    private readonly FileLoggingOptions _options;
    private readonly bool _masterEnabled;
    private readonly SemaphoreSlim _semaphore = new(1, 1);
    private readonly JsonSerializerOptions _jsonOptions;
    private string _currentFilePath = string.Empty;
    private DateTime _currentFileDate;

    public FileRequestLogger(IOptions<LoggingOptions> options)
    {
        _options = options.Value.File;
        _masterEnabled = options.Value.Enabled;
        
        _jsonOptions = new JsonSerializerOptions
        {
            WriteIndented = false,
            PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
            DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
        };

        if (IsEnabled)
        {
            EnsureDirectoryExists();
        }
    }

    public string LoggerType => "File";
    public bool IsEnabled => _masterEnabled && _options.Enabled;

    public async Task LogAsync(RequestLog log, CancellationToken cancellationToken = default)
    {
        if (!IsEnabled) return;

        await _semaphore.WaitAsync(cancellationToken);
        try
        {
            var filePath = GetCurrentFilePath();
            var logLine = FormatLog(log);

            await File.AppendAllTextAsync(filePath, logLine + Environment.NewLine, Encoding.UTF8, cancellationToken);
            
            // Check file size and rotate if needed
            await CheckAndRotateFile(filePath, cancellationToken);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    private string GetCurrentFilePath()
    {
        var now = DateTime.UtcNow;
        var shouldRotate = _options.RollingInterval switch
        {
            "Hourly" => now.Date != _currentFileDate.Date || now.Hour != _currentFileDate.Hour,
            _ => now.Date != _currentFileDate.Date // Daily
        };

        if (shouldRotate || string.IsNullOrEmpty(_currentFilePath))
        {
            _currentFileDate = now;
            var dateFormat = _options.RollingInterval == "Hourly" ? "yyyy-MM-dd-HH" : "yyyy-MM-dd";
            var fileName = _options.FileNamePattern.Replace("{date}", now.ToString(dateFormat));
            var extension = _options.Format.ToLower() == "json" ? ".json" : ".txt";
            _currentFilePath = Path.Combine(_options.Directory, fileName + extension);
        }

        return _currentFilePath;
    }

    private string FormatLog(RequestLog log)
    {
        if (_options.Format.ToLower() == "json")
        {
            return JsonSerializer.Serialize(log, _jsonOptions);
        }
        else
        {
            // Text format
            var sb = new StringBuilder();
            sb.Append($"[{log.Timestamp:yyyy-MM-dd HH:mm:ss.fff}] ");
            sb.Append($"[{log.Id}] ");
            sb.Append($"{log.HttpMethod} {log.Path}");
            
            if (!string.IsNullOrEmpty(log.QueryString))
            {
                sb.Append($"?{log.QueryString}");
            }
            
            sb.Append($" -> {log.StatusCode} ({log.ResponseTimeMs}ms)");
            sb.Append($" | IP: {log.ClientIp}");

            if (!string.IsNullOrEmpty(log.RequestBody))
            {
                sb.Append($" | Body: {TruncateForLog(log.RequestBody, 500)}");
            }

            if (!string.IsNullOrEmpty(log.ErrorMessage))
            {
                sb.Append($" | ERROR: {log.ErrorMessage}");
            }

            return sb.ToString();
        }
    }

    private void EnsureDirectoryExists()
    {
        if (!Directory.Exists(_options.Directory))
        {
            Directory.CreateDirectory(_options.Directory);
        }

        // Clean up old files if retention is configured
        if (_options.RetentionDays > 0)
        {
            Task.Run(() => CleanupOldFiles());
        }
    }

    private Task CheckAndRotateFile(string filePath, CancellationToken cancellationToken)
    {
        if (_options.MaxFileSizeMB <= 0) return Task.CompletedTask;

        var fileInfo = new FileInfo(filePath);
        if (!fileInfo.Exists) return Task.CompletedTask;

        var maxSizeBytes = _options.MaxFileSizeMB * 1024 * 1024;
        if (fileInfo.Length >= maxSizeBytes)
        {
            // Rotate: rename current file with index
            var index = 1;
            var basePath = Path.Combine(
                Path.GetDirectoryName(filePath)!,
                Path.GetFileNameWithoutExtension(filePath));
            var extension = Path.GetExtension(filePath);

            string newPath;
            do
            {
                newPath = $"{basePath}.{index}{extension}";
                index++;
            } while (File.Exists(newPath));

            File.Move(filePath, newPath);
            _currentFilePath = string.Empty; // Force new file creation
        }

        return Task.CompletedTask;
    }

    private void CleanupOldFiles()
    {
        try
        {
            var directory = new DirectoryInfo(_options.Directory);
            if (!directory.Exists) return;

            var cutoffDate = DateTime.UtcNow.AddDays(-_options.RetentionDays);
            var pattern = _options.Format.ToLower() == "json" ? "*.json" : "*.txt";

            foreach (var file in directory.GetFiles(pattern))
            {
                if (file.LastWriteTimeUtc < cutoffDate)
                {
                    try
                    {
                        file.Delete();
                    }
                    catch
                    {
                        // Ignore deletion errors
                    }
                }
            }
        }
        catch
        {
            // Ignore cleanup errors
        }
    }

    private static string TruncateForLog(string text, int maxLength)
    {
        if (string.IsNullOrEmpty(text)) return text;
        
        // Remove newlines for single-line logging
        text = text.Replace("\r\n", " ").Replace("\n", " ").Replace("\r", " ");
        
        if (text.Length <= maxLength) return text;
        return text[..maxLength] + "...";
    }

    public void Dispose()
    {
        _semaphore.Dispose();
    }
}
