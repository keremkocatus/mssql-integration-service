using Microsoft.AspNetCore.Mvc;
using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;

namespace MssqlIntegrationService.API.Controllers;

[ApiController]
[Route("api/[controller]")]
public class QueryController : ControllerBase
{
    private readonly IQueryService _queryService;
    private readonly ILogger<QueryController> _logger;

    public QueryController(IQueryService queryService, ILogger<QueryController> logger)
    {
        _queryService = queryService;
        _logger = logger;
    }

    /// <summary>
    /// Execute a SELECT query and return results
    /// </summary>
    [HttpPost("execute")]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteQuery([FromBody] QueryRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing query: {Query}", request.Query);
        
        var response = await _queryService.ExecuteQueryAsync(request, cancellationToken);
        
        if (!response.Success)
        {
            _logger.LogWarning("Query failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        return Ok(response);
    }

    /// <summary>
    /// Execute INSERT, UPDATE, or DELETE query
    /// </summary>
    [HttpPost("execute-nonquery")]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteNonQuery([FromBody] QueryRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing non-query: {Query}", request.Query);
        
        var response = await _queryService.ExecuteNonQueryAsync(request, cancellationToken);
        
        if (!response.Success)
        {
            _logger.LogWarning("Non-query failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        return Ok(response);
    }

    /// <summary>
    /// Execute a stored procedure
    /// </summary>
    [HttpPost("execute-sp")]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(QueryResponse), StatusCodes.Status400BadRequest)]
    public async Task<IActionResult> ExecuteStoredProcedure([FromBody] StoredProcedureRequest request, CancellationToken cancellationToken)
    {
        _logger.LogInformation("Executing stored procedure: {ProcedureName}", request.ProcedureName);
        
        var response = await _queryService.ExecuteStoredProcedureAsync(request, cancellationToken);
        
        if (!response.Success)
        {
            _logger.LogWarning("Stored procedure failed: {Error}", response.ErrorMessage);
            return BadRequest(response);
        }

        return Ok(response);
    }
}
