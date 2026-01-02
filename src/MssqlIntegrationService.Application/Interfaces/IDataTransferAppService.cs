using MssqlIntegrationService.Application.DTOs;

namespace MssqlIntegrationService.Application.Interfaces;

public interface IDataTransferAppService
{
    Task<DataTransferResponse> TransferDataAsync(DataTransferRequest request, CancellationToken cancellationToken = default);
    Task<BulkInsertResponse> BulkInsertAsync(BulkInsertRequest request, CancellationToken cancellationToken = default);
}
