using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;

namespace MssqlIntegrationService.Application.Services;

public class DataSyncAppService : IDataSyncAppService
{
    private readonly IDataSyncService _dataSyncService;
    private readonly ISchemaService _schemaService;

    public DataSyncAppService(IDataSyncService dataSyncService, ISchemaService schemaService)
    {
        _dataSyncService = dataSyncService;
        _schemaService = schemaService;
    }

    public async Task<DataSyncResponse> SyncDataAsync(DataSyncRequest request, CancellationToken cancellationToken = default)
    {
        // ===== AUTO-DETECT KEY COLUMNS IF NOT PROVIDED =====
        List<string> keyColumns;
        bool keyColumnsAutoDetected = false;
        string? keyColumnsSource = null;

        if (request.Target.KeyColumns == null || request.Target.KeyColumns.Count == 0)
        {
            // Skip auto-detection if DeleteAllBeforeInsert is true (key columns not needed)
            if (request.Options?.DeleteAllBeforeInsert == true)
            {
                keyColumns = new List<string>(); // Empty list, won't be used
            }
            else
            {
                // Auto-detect from target table schema using SchemaService
                var keyColumnsResult = await _schemaService.GetKeyColumnsAsync(
                    request.Target.ConnectionString,
                    request.Target.TableName,
                    cancellationToken);

                if (!keyColumnsResult.IsSuccess || keyColumnsResult.Data == null || keyColumnsResult.Data.Count == 0)
                {
                    return new DataSyncResponse
                    {
                        Success = false,
                        ErrorMessage = keyColumnsResult.ErrorMessage ?? 
                            $"Could not auto-detect key columns for table '{request.Target.TableName}'. " +
                            "Please provide KeyColumns manually or ensure the target table has a Primary Key or Unique Index."
                    };
                }

                keyColumns = keyColumnsResult.Data;
                keyColumnsAutoDetected = true;
                keyColumnsSource = "auto-detected from schema";
            }
        }
        else
        {
            keyColumns = request.Target.KeyColumns;
        }

        // ===== SQL INJECTION VALIDATION =====
        var validation = SqlValidator.Validate(
            tableName: request.Target.TableName,
            columnNames: keyColumns
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
            keyColumns,
            options,
            cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            var response = new DataSyncResponse
            {
                Success = true,
                TotalRowsRead = result.Data.TotalRowsRead,
                RowsDeleted = result.Data.RowsDeleted,
                RowsInserted = result.Data.RowsInserted,
                ExecutionTimeMs = result.Data.ExecutionTimeMs,
                SourceQuery = result.Data.SourceQuery,
                TargetTable = result.Data.TargetTable,
                KeyColumns = result.Data.KeyColumns,
                Warnings = result.Data.Warnings ?? new List<string>()
            };

            // Add info about auto-detected key columns
            if (keyColumnsAutoDetected)
            {
                response.Warnings!.Insert(0, $"KeyColumns {keyColumnsSource}: [{string.Join(", ", keyColumns)}]");
            }

            return response;
        }

        return new DataSyncResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage
        };
    }
}
