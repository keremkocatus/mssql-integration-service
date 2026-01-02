using MssqlIntegrationService.Domain.Common;
using MssqlIntegrationService.Domain.Entities;

namespace MssqlIntegrationService.Domain.Interfaces;

public interface IDataSyncService
{
    /// <summary>
    /// Syncs data from source to target using delete-insert pattern:
    /// 1. Read data from source database
    /// 2. Create temp table in target database
    /// 3. Insert data into temp table
    /// 4. Delete matching rows from target table (based on key columns)
    /// 5. Insert from temp table to target table
    /// 6. Drop temp table
    /// </summary>
    Task<Result<SyncResult>> SyncDataAsync(
        string sourceConnectionString,
        string targetConnectionString,
        string sourceQuery,
        string targetTable,
        List<string> keyColumns,
        SyncOptions? options = null,
        CancellationToken cancellationToken = default);
}
