using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Domain.Interfaces;

public interface IDataTransferService
{
    Task<Result<TransferResult>> TransferDataAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string sourceQuery,
        string targetTable,
        TransferOptions? options = null,
        CancellationToken cancellationToken = default);

    Task<Result<TransferResult>> BulkInsertAsync(
        string connectionString,
        string tableName,
        IEnumerable<IDictionary<string, object?>> data,
        BulkInsertOptions? options = null,
        CancellationToken cancellationToken = default);
}
