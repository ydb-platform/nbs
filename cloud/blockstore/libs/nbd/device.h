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

    virtual IDeviceConnectionPtr Create(
        TNetworkAddress connectAddress,
        TString deviceName) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDeviceConnectionPtr CreateDeviceConnection(
    ILoggingServicePtr logging,
    TNetworkAddress connectAddress,
    TString deviceName,
    TDuration timeout);

IDeviceConnectionPtr CreateDeviceConnectionStub();

IDeviceConnectionFactoryPtr CreateDeviceConnectionFactory(
    ILoggingServicePtr logging,
    TDuration timeout);

}   // namespace NCloud::NBlockStore::NBD
