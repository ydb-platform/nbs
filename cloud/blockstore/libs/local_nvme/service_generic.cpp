#include "service.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor,
    ITaskQueuePtr backgroundExecutor)
{
    Y_UNUSED(
        config,
        logging,
        deviceProvider,
        nvmeManager,
        executor,
        backgroundExecutor);

    return CreateLocalNVMeServiceStub();
}

}   // namespace NCloud::NBlockStore
