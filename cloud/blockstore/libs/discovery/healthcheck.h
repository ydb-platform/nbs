#pragma once

#include "public.h"

#include "instance.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct IHealthChecker: IStartable
{
    virtual ~IHealthChecker() = default;

    virtual NThreading::TFuture<void> UpdateStatus(
        TInstanceList& instances) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IHealthCheckerPtr CreateHealthChecker(
    TDiscoveryConfigPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IPingClientPtr insecurePingClient,
    IPingClientPtr securePingClient);

IHealthCheckerPtr CreateHealthCheckerStub();

}   // namespace NCloud::NBlockStore::NDiscovery
