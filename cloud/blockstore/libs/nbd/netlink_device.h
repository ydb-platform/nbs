#pragma once

#include "device.h"

#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString devicePath,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    ITaskQueuePtr executor);

IDevicePtr CreateFreeNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString devicePrefix,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    ITaskQueuePtr executor);

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    ITaskQueuePtr executor);

}   // namespace NCloud::NBlockStore::NBD
