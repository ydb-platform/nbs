#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/network/address.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct IDeviceConnection
    : public IStartable
{
};

////////////////////////////////////////////////////////////////////////////////

struct IDeviceConnectionFactory
{
    virtual ~IDeviceConnectionFactory() = default;

    // blockCount and blockSize can be simply passed to the kernel, can be
    // dropped or can be used to validate export info upon connection
    // establishment
    virtual IDeviceConnectionPtr Create(
        const TNetworkAddress& connectAddress,
        TString deviceName,
        ui64 blockCount,
        ui32 blockSize) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDeviceConnectionPtr CreateDeviceConnection(
    ILoggingServicePtr logging,
    const TNetworkAddress& connectAddress,
    TString deviceName,
    TDuration timeout);

IDeviceConnectionPtr CreateDeviceConnectionStub();

IDeviceConnectionFactoryPtr CreateDeviceConnectionFactory(
    ILoggingServicePtr logging,
    TDuration timeout);

}   // namespace NCloud::NBlockStore::NBD
