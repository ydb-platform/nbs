#pragma once

#include "device.h"

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString devicePath,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    bool fallback);

IDevicePtr CreateFreeNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString devicePrefix,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    bool fallback);

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    bool fallback);

}   // namespace NCloud::NBlockStore::NBD
