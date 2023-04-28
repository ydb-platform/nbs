#pragma once

#include "public.h"

#include <cloud/storage/core/config/iam.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NIamClient {

////////////////////////////////////////////////////////////////////////////////

class TIamClientConfig
{
private:
    const NProto::TIamClientConfig Config;

public:
    explicit TIamClientConfig(NProto::TIamClientConfig config = {});

    const NProto::TIamClientConfig& GetIamServiceConfig() const
    {
        return Config;
    }

    bool IsValid() const
    {
        return GetMetadataServiceUrl() || GetTokenAgentUnixSocket();
    }

    TString GetMetadataServiceUrl() const;
    TString GetTokenAgentUnixSocket() const;
    TDuration GetInitialRetryTimeout() const;
    TDuration GetRetryTimeoutIncrement() const;
    ui32 GetRetryAttempts() const;
    TDuration GetGrpcTimeout() const;
    TDuration GetHttpTimeout() const;
    TDuration GetTokenRefreshTimeout() const;
};

}   // namespace NCloud::NIamClient
