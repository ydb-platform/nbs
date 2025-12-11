#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>

#include <cloud/storage/core/libs/common/startable.h>

namespace NCloud::NBlockStore::NDiscovery {

////////////////////////////////////////////////////////////////////////////////

struct IDiscoveryService: IStartable
{
    virtual void ServeRequest(
        const NProto::TDiscoverInstancesRequest& request,
        NProto::TDiscoverInstancesResponse* response) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IDiscoveryServicePtr CreateDiscoveryService(
    TDiscoveryConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IBanListPtr banList,
    IInstanceFetcherPtr staticInstanceFetcher,
    IInstanceFetcherPtr conductorInstanceFetcher,
    IHealthCheckerPtr healthChecker,
    IBalancingPolicyPtr balancingPolicy);

IDiscoveryServicePtr CreateDiscoveryServiceStub(
    TString defaultHost = {},
    ui16 defaultInsecurePort = 0,
    ui16 defaultSecurePort = 0);

}   // namespace NCloud::NBlockStore::NDiscovery
