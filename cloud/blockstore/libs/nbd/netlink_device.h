#pragma once

#include "device.h"

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateNetlinkDevice(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString deviceName,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    bool reconfigure,
    bool withoutLibnl);

IDeviceFactoryPtr CreateNetlinkDeviceFactory(
    ILoggingServicePtr logging,
    TDuration requestTimeout,
    TDuration connectionTimeout,
    bool reconfigure,
    bool withoutLibnl);

}   // namespace NCloud::NBlockStore::NBD
