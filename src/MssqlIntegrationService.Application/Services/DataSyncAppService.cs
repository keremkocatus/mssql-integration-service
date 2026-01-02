using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;

namespace MssqlIntegrationService.Application.Services;

public class DataSyncAppService : IDataSyncAppService
{
    private readonly IDataSyncService _dataSyncService;

    public DataSyncAppService(IDataSyncService dataSyncService)
    {
        _dataSyncService = dataSyncService;
    }

    public async Task<DataSyncResponse> SyncDataAsync(DataSyncRequest request, CancellationToken cancellationToken = default)
    {
        // ===== SQL INJECTION VALIDATION =====
        var validation = SqlValidator.Validate(
            tableName: request.Target.TableName,
            columnNames: request.Target.KeyColumns
        );

        if (!validation.IsValid)
        {
            return new DataSyncResponse
            {
                Success = false,
                ErrorMessage = $"Validation failed: {validation}"
            };
        }

        // Check column mappings if provided
        if (request.Options?.ColumnMappings != null)
        {
            foreach (var mapping in request.Options.ColumnMappings)
            {
                if (!SqlValidator.IsValidColumnName(mapping.Key) || !SqlValidator.IsValidColumnName(mapping.Value))
                {
                    return new DataSyncResponse
                    {
                        Success = false,
                        ErrorMessage = $"Invalid column mapping: '{mapping.Key}' -> '{mapping.Value}'"
                    };
                }
            }
        }
        // ===== END VALIDATION =====

        var options = request.Options != null
            ? new SyncOptions
            {
                BatchSize = request.Options.BatchSize,
                Timeout = request.Options.Timeout,
                UseTransaction = request.Options.UseTransaction,
                DeleteAllBeforeInsert = request.Options.DeleteAllBeforeInsert,
                ColumnMappings = request.Options.ColumnMappings
            }
            : null;

        var result = await _dataSyncService.SyncDataAsync(
            request.Source.ConnectionString,
            request.Target.ConnectionString,
            request.Source.Query,
            request.Target.TableName,
            request.Target.KeyColumns,
            options,
            cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            return new DataSyncResponse
            {
                Success = true,
                TotalRowsRead = result.Data.TotalRowsRead,
                RowsDeleted = result.Data.RowsDeleted,
                RowsInserted = result.Data.RowsInserted,
                ExecutionTimeMs = result.Data.ExecutionTimeMs,
                SourceQuery = result.Data.SourceQuery,
                TargetTable = result.Data.TargetTable,
                KeyColumns = result.Data.KeyColumns,
                Warnings = result.Data.Warnings
            };
        }

        return new DataSyncResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage
        };
    }
}
