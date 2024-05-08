#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/network/address.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct IDevice
{
    virtual ~IDevice() = default;

    virtual void Start() = 0;

    // some devices can be reconfigured, so we want to differentiate between
    // cases when user explicitly asked us to stop the device and it being
    // stopped for technical reasons like service restart
    virtual void Stop(bool deleteDevice) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IDeviceFactory
{
    virtual ~IDeviceFactory() = default;

    // blockCount and blockSize can be simply passed to the kernel, can be
    // dropped or can be used to validate export info upon connection
    // establishment
    virtual IDevicePtr Create(
        const TNetworkAddress& connectAddress,
        TString deviceName,
        ui64 blockCount,
        ui32 blockSize) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDevicePtr CreateDevice(
    ILoggingServicePtr logging,
    const TNetworkAddress& connectAddress,
    TString deviceName,
    TDuration timeout);

IDevicePtr CreateDeviceStub();

IDeviceFactoryPtr CreateDeviceFactory(
    ILoggingServicePtr logging,
    TDuration timeout);

}   // namespace NCloud::NBlockStore::NBD
