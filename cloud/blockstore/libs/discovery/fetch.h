#pragma once

#include "public.h"

#include "instance.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct IInstanceFetcher: IStartable
{
    virtual ~IInstanceFetcher() = default;

    virtual NThreading::TFuture<void> FetchInstances(
        TInstanceList& instances) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IInstanceFetcherPtr CreateConductorInstanceFetcher(
    TDiscoveryConfigPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    ITimerPtr timer,
    ISchedulerPtr scheduler);

IInstanceFetcherPtr CreateStaticInstanceFetcher(
    TDiscoveryConfigPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring);

IInstanceFetcherPtr CreateInstanceFetcherStub(
    TDeque<TInstanceInfo> instances = {});

}   // namespace NCloud::NBlockStore::NDiscovery
