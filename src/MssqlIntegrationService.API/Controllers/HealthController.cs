using Microsoft.AspNetCore.Mvc;

namespace MssqlIntegrationService.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class HealthController : ControllerBase
{
    private readonly ILogger<HealthController> _logger;

    public HealthController(ILogger<HealthController> logger)
    {
        _logger = logger;
    }

    /// <summary>
    /// Health check endpoint
    /// </summary>
    [HttpGet]
    [ProducesResponseType(typeof(HealthResponse), StatusCodes.Status200OK)]
    public IActionResult Get()
    {
        return Ok(new HealthResponse
        {
            Status = "Healthy",
            Timestamp = DateTime.UtcNow,
            Version = GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0"
        });
    }

    /// <summary>
    /// Detailed health check
    /// </summary>
    [HttpGet("details")]
    [ProducesResponseType(typeof(DetailedHealthResponse), StatusCodes.Status200OK)]
    public IActionResult GetDetails()
    {
        var process = System.Diagnostics.Process.GetCurrentProcess();

        return Ok(new DetailedHealthResponse
        {
            Status = "Healthy",
            Timestamp = DateTime.UtcNow,
            Version = GetType().Assembly.GetName().Version?.ToString() ?? "1.0.0",
            Environment = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT") ?? "Production",
            MachineName = Environment.MachineName,
            ProcessId = process.Id,
            MemoryUsageMB = process.WorkingSet64 / 1024 / 1024,
            Uptime = DateTime.UtcNow - process.StartTime.ToUniversalTime()
        });
    }
}

public class HealthResponse
{
    public required string Status { get; set; }
    public DateTime Timestamp { get; set; }
    public required string Version { get; set; }
}

public class DetailedHealthResponse : HealthResponse
{
    public required string Environment { get; set; }
    public required string MachineName { get; set; }
    public int ProcessId { get; set; }
    public long MemoryUsageMB { get; set; }
    public TimeSpan Uptime { get; set; }
}
