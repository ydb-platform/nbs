#include "service.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigConstPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor,
    ITaskQueuePtr backgroundExecutor)
{
    Y_UNUSED(
        config,
        logging,
        monitoring,
        deviceProvider,
        nvmeManager,
        executor,
        backgroundExecutor);

    return CreateLocalNVMeServiceStub();
}

}   // namespace NCloud::NBlockStore
