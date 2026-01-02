using MssqlIntegrationService.Application.DTOs;
using MssqlIntegrationService.Application.Interfaces;
using MssqlIntegrationService.Domain.Entities;
using MssqlIntegrationService.Domain.Interfaces;
using MssqlIntegrationService.Domain.Validation;

namespace MssqlIntegrationService.Application.Services;

public class MongoToMssqlAppService : IMongoToMssqlAppService
{
    private readonly IMongoToMssqlService _mongoToMssqlService;

    public MongoToMssqlAppService(IMongoToMssqlService mongoToMssqlService)
    {
        _mongoToMssqlService = mongoToMssqlService;
    }

    public async Task<MongoToMssqlResponse> TransferAsync(MongoToMssqlRequest request, CancellationToken cancellationToken = default)
    {
        // ===== VALIDATION =====
        if (!SqlValidator.IsValidTableName(request.Target.TableName))
        {
            return new MongoToMssqlResponse
            {
                Success = false,
                ErrorMessage = $"Invalid target table name: '{request.Target.TableName}'"
            };
        }

        if (request.Options?.FieldMappings != null)
        {
            foreach (var mapping in request.Options.FieldMappings)
            {
                if (!SqlValidator.IsValidColumnName(mapping.Value))
                {
                    return new MongoToMssqlResponse
                    {
                        Success = false,
                        ErrorMessage = $"Invalid MSSQL column name in field mapping: '{mapping.Value}'"
                    };
                }
            }
        }
        // ===== END VALIDATION =====

        var options = request.Options != null
            ? new MongoToMssqlOptions
            {
                BatchSize = request.Options.BatchSize,
                Timeout = request.Options.Timeout,
                TruncateTargetTable = request.Options.TruncateTargetTable,
                CreateTableIfNotExists = request.Options.CreateTableIfNotExists,
                UseTransaction = request.Options.UseTransaction,
                FieldMappings = request.Options.FieldMappings,
                IncludeFields = request.Options.IncludeFields,
                ExcludeFields = request.Options.ExcludeFields,
                FlattenNestedDocuments = request.Options.FlattenNestedDocuments,
                FlattenSeparator = request.Options.FlattenSeparator,
                ArrayHandling = request.Options.ArrayHandling
            }
            : null;

        // Use aggregation pipeline if provided, otherwise use filter
        var result = !string.IsNullOrEmpty(request.Source.AggregationPipeline)
            ? await _mongoToMssqlService.TransferWithAggregationAsync(
                request.Source.ConnectionString,
                request.Source.DatabaseName,
                request.Source.CollectionName,
                request.Source.AggregationPipeline,
                request.Target.ConnectionString,
                request.Target.TableName,
                options,
                cancellationToken)
            : await _mongoToMssqlService.TransferAsync(
                request.Source.ConnectionString,
                request.Source.DatabaseName,
                request.Source.CollectionName,
                request.Source.Filter,
                request.Target.ConnectionString,
                request.Target.TableName,
                options,
                cancellationToken);

        if (result.IsSuccess && result.Data != null)
        {
            return new MongoToMssqlResponse
            {
                Success = true,
                SourceCollection = result.Data.SourceCollection,
                TargetTable = result.Data.TargetTable,
                TotalDocumentsRead = result.Data.TotalDocumentsRead,
                TotalRowsWritten = result.Data.TotalRowsWritten,
                FailedDocuments = result.Data.FailedDocuments,
                ExecutionTimeMs = result.Data.ExecutionTimeMs,
                Warnings = result.Data.Warnings
            };
        }

        return new MongoToMssqlResponse
        {
            Success = false,
            ErrorMessage = result.ErrorMessage
        };
    }
}
