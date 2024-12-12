#pragma once

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/libs/iam/iface/public.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct TClientFactories
{
    std::function<NIamClient::IIamTokenClientPtr(
        NIamClient::TIamClientConfigPtr config,
        ILoggingServicePtr logging,
        ISchedulerPtr scheduler,
        ITimerPtr timer)> IamClientFactory;
};

}   // namespace NCloud::NFileStore::NClient
