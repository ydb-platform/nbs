#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/grpc/public.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public IStartable
{
    virtual IBlockStorePtr CreateEndpoint() = 0;

    virtual IBlockStorePtr CreateDataEndpoint() = 0;

    virtual IBlockStorePtr CreateDataEndpoint(
        const TString& socketPath) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IMultiHostClient
    : public IStartable
{
    virtual IBlockStorePtr CreateEndpoint(
        const TString& host,
        ui32 port,
        bool isSecure) = 0;

    virtual IBlockStorePtr CreateDataEndpoint(
        const TString& host,
        ui32 port,
        bool isSecure) = 0;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<IClientPtr> CreateClient(
    TClientAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats,
    ICertificateProviderPtr certificateProvider);

TResultOrError<IMultiHostClientPtr> CreateMultiHostClient(
    TClientAppConfigPtr config,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    IServerStatsPtr clientStats,
    ICertificateProviderPtr certificateProvider);

}   // namespace NCloud::NBlockStore::NClient
