#pragma once

#include "public.h"

#include "instance.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct IInstanceFetcher
    : IStartable
{
    virtual ~IInstanceFetcher() = default;

    virtual NThreading::TFuture<NProto::TError> FetchInstances(
        TInstanceList& instances) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// An EDS fetcher also answers for host patterns other than its configured
// ones: the cells ask it for the hosts of each cell. FetchHosts may be called
// concurrently, from several cells at once, and after Stop, when it answers
// with an error; the future may be completed on any thread.
struct IEdsInstanceFetcher
    : IInstanceFetcher
{
    virtual NThreading::TFuture<TResultOrError<TVector<TString>>> FetchHosts(
        TVector<TString> hostPatterns) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IInstanceFetcherPtr CreateConductorInstanceFetcher(
    TDiscoveryConfigConstPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    ITimerPtr timer,
    ISchedulerPtr scheduler);

IInstanceFetcherPtr CreateStaticInstanceFetcher(
    TDiscoveryConfigConstPtr config,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring);

IInstanceFetcherPtr CreateInstanceFetcherStub(
    TDeque<TInstanceInfo> instances = {});

// Lists nothing as instances and answers every pattern with the same hosts.
IEdsInstanceFetcherPtr CreateEdsInstanceFetcherStub(
    TVector<TString> hosts = {});

}   // namespace NCloud::NBlockStore::NDiscovery
