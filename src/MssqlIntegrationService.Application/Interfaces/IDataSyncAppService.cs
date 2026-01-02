using MssqlIntegrationService.Application.DTOs;

namespace MssqlIntegrationService.Application.Interfaces;

public interface IDataSyncAppService
{
    /// <summary>
    /// Syncs data from source database to target database using delete-insert pattern
    /// </summary>
    Task<DataSyncResponse> SyncDataAsync(DataSyncRequest request, CancellationToken cancellationToken = default);
}
