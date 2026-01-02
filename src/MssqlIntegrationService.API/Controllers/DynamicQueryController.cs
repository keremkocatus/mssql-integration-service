using Microsoft.AspNetCore.Mvc;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;

namespace MssqlIntegrationService.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class DynamicQueryController : ControllerBase
{
    private readonly IDynamicQueryService _queryService;
    private readonly ILogger<DynamicQueryController> _logger;

    public DynamicQueryController(IDynamicQueryService queryService, ILogger<DynamicQueryController> logger)
    {
        _queryService = queryService;
        _logger = logger;
    }

    /// <summary>
    /// Test database connection
    /// </summary>
    [HttpPost("test-connection")]
    [ProducesResponseType(typeof(TestConnectionResponse), StatusCodes.Status200OK)]
    public async Task<IActionResult> TestConnection([FromBody] TestConnectionRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Testing connection...");

        var response = await _queryService.TestConnectionAsync(request, cancellationToken);

        return Ok(response);
    }

    /// <summary>
    /// Get database information (server, tables, columns)
    /// </summary>
    [HttpPost("database-info")]
    [ProducesResponseType(typeof(DatabaseInfoResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(DatabaseInfoResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> GetDatabaseInfo([FromBody] DatabaseInfoRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Getting database info...");

        var response = await _queryService.GetDatabaseInfoAsync(request, cancellationToken);

        if (!response.Success)
        {
            return BadRequest(response);
        }

        return Ok(response);
    }

    /// <summary>
    /// Execute a SELECT query with dynamic connection string
    /// </summary>
    [HttpPost("execute")]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteQuery([FromBody] DynamicQueryRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing dynamic query");

        var response = await _queryService.ExecuteQueryAsync(request, cancellationToken);

        if (!response.Success)
        {
            _logger.LogWarning("Dynamic query failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        return Ok(response);
    }

    /// <summary>
    /// Execute INSERT, UPDATE, DELETE with dynamic connection string
    /// </summary>
    [HttpPost("execute-nonquery")]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteNonQuery([FromBody] DynamicQueryRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing dynamic non-query");

        var response = await _queryService.ExecuteNonQueryAsync(request, cancellationToken);

        if (!response.Success)
        {
            _logger.LogWarning("Dynamic non-query failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        return Ok(response);
    }

    /// <summary>
    /// Execute stored procedure with dynamic connection string
    /// </summary>
    [HttpPost("execute-sp")]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteStoredProcedure([FromBody] DynamicStoredProcedureRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing dynamic stored procedure: {ProcedureName}", request.ProcedureName);

        var response = await _queryService.ExecuteStoredProcedureAsync(request, cancellationToken);

        if (!response.Success)
        {
            _logger.LogWarning("Dynamic stored procedure failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        return Ok(response);
    }
}
