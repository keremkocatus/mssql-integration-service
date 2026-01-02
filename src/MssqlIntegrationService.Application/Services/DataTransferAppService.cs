using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;

namespace MssqlIntegrationService.Application.Services;

public class DataTransferAppService : IDataTransferAppService
{
    private readonly IDataTransferService _dataTransferService;

    public DataTransferAppService(IDataTransferService dataTransferService)
    {
        _dataTransferService = dataTransferService;
    }

    public async Task<DataTransferResponse> TransferDataAsync(DataTransferRequest request, CancellationToken cancellationToken = default)
    {
        // ===== SQL INJECTION VALIDATION =====
        if (!SqlValidator.IsValidTableName(request.Target.TableName))
        {
            return new DataTransferResponse
            {
                Success = false,
                ErrorMessage = $"Invalid table name: '{request.Target.TableName}'"
            };
        }

        if (request.Options?.ColumnMappings != null)
        {
            foreach (var mapping in request.Options.ColumnMappings)
            {
                if (!SqlValidator.IsValidColumnName(mapping.Key) || !SqlValidator.IsValidColumnName(mapping.Value))
                {
                    return new DataTransferResponse
                    {
                        Success = false,
                        ErrorMessage = $"Invalid column mapping: '{mapping.Key}' -> '{mapping.Value}'"
                    };
                }
            }
        }
        // ===== END VALIDATION =====

        var options = request.Options != null
            ? new TransferOptions
            {
                BatchSize = request.Options.BatchSize,
                Timeout = request.Options.Timeout,
                TruncateTargetTable = request.Options.TruncateTargetTable,
                CreateTableIfNotExists = request.Options.CreateTableIfNotExists,
                ColumnMappings = request.Options.ColumnMappings,
                UseTransaction = request.Options.UseTransaction
            }
            : null;

        var result = await _dataTransferService.TransferDataAsync(
            request.Source.ConnectionString,
            request.Target.ConnectionString,
            request.Source.Query,
            request.Target.TableName,
            options,
            cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            return new DataTransferResponse
            {
                Success = true,
                TotalRowsRead = result.Data.TotalRowsRead,
                TotalRowsWritten = result.Data.TotalRowsWritten,
                ExecutionTimeMs = result.Data.ExecutionTimeMs,
                SourceQuery = result.Data.SourceQuery,
                TargetTable = result.Data.TargetTable,
                Warnings = result.Data.Warnings
            };
        }

        return new DataTransferResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage
        };
    }

    public async Task<BulkInsertResponse> BulkInsertAsync(BulkInsertRequest request, CancellationToken cancellationToken = default)
    {
        // ===== SQL INJECTION VALIDATION =====
        if (!SqlValidator.IsValidTableName(request.TableName))
        {
            return new BulkInsertResponse
            {
                Success = false,
                ErrorMessage = $"Invalid table name: '{request.TableName}'"
            };
        }
        // ===== END VALIDATION =====

        var options = request.Options != null
            ? new BulkInsertOptions
            {
                BatchSize = request.Options.BatchSize,
                Timeout = request.Options.Timeout,
                UseTransaction = request.Options.UseTransaction
            }
            : null;

        var data = request.Data.Cast<IDictionary<string, object?>>().ToList();

        var result = await _dataTransferService.BulkInsertAsync(
            request.ConnectionString,
            request.TableName,
            data,
            options,
            cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            return new BulkInsertResponse
            {
                Success = true,
                TotalRowsInserted = result.Data.TotalRowsWritten,
                ExecutionTimeMs = result.Data.ExecutionTimeMs
            };
        }

        return new BulkInsertResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage
        };
    }
}
