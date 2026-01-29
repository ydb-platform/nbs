#include "service.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

ILocalNVMeServicePtr CreateLocalNVMeService(
    ILoggingServicePtr logging,
    ILocalNVMeDeviceProviderPtr deviceProvider,
    NNvme::INvmeManagerPtr nvmeManager,
    TExecutorPtr executor)
{
    Y_UNUSED(logging, deviceProvider, nvmeManager, executor);

    return CreateLocalNVMeServiceStub();
}

}   // namespace NCloud::NBlockStore
