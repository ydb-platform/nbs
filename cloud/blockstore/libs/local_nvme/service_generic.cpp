#include "service.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    TLocalNVMeConfigPtr config,
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor,
    NMonitoring::TDynamicCountersPtr counters)
{
    Y_UNUSED(config, logging, deviceProvider, nvmeManager, executor, counters);

    return CreateLocalNVMeServiceStub();
}

}   // namespace NCloud::NBlockStore
