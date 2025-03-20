#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TRetryState
{
    const TInstant Started = TInstant::Now();

    TDuration RetryTimeout;
    ui32 Retries = 0;
    bool DoneInstantRetry = false;
};

////////////////////////////////////////////////////////////////////////////////

struct TRetrySpec
{
    bool ShouldRetry = false;
    bool IsRetriableError = false;
    TDuration Backoff;
};

////////////////////////////////////////////////////////////////////////////////

struct IRetryPolicy
{
    virtual ~IRetryPolicy() = default;

    virtual TRetrySpec ShouldRetry(
        TRetryState& state,
        const NProto::TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRetryPolicyPtr CreateRetryPolicy(
    TClientAppConfigPtr config,
    std::optional<NProto::EStorageMediaKind> mediaKind);

IBlockStorePtr CreateDurableClient(
    TClientAppConfigPtr config,
    IBlockStorePtr client,
    IRetryPolicyPtr retryPolicy,
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRequestStatsPtr requestStats,
    IVolumeStatsPtr volumeStats);

}   // namespace NCloud::NBlockStore::NClient
