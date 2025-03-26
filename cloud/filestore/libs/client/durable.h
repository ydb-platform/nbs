#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/datetime/base.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TRetryState
{
    const TInstant Started = TInstant::Now();

    TDuration RetryTimeout;
    TDuration Backoff;
    ui32 Retries = 0;
    bool DoneInstantRetry = false;
};

////////////////////////////////////////////////////////////////////////////////

struct IRetryPolicy
{
    virtual ~IRetryPolicy() = default;

    virtual bool ShouldRetry(
        TRetryState& state,
        const NProto::TError& error) = 0;
};

////////////////////////////////////////////////////////////////////////////////

IRetryPolicyPtr CreateRetryPolicy(TClientConfigPtr config);

IFileStoreServicePtr CreateDurableClient(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRetryPolicyPtr retryPolicy,
    IFileStoreServicePtr client);

IEndpointManagerPtr CreateDurableClient(
    ILoggingServicePtr logging,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IRetryPolicyPtr retryPolicy,
    IEndpointManagerPtr client);

}   // namespace NCloud::NFileStore::NClient
