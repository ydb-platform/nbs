#pragma once

#include "throttler_metrics.h"

#include <functional>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TTestThrottlerMetrics final: public IThrottlerMetrics
{
    using TRegisterHandler =
        std::function<void(const TString&, const TString&)>;
    using TUnregisterHandler =
        std::function<void(const TString&, const TString&)>;
    using TTrimHandler = std::function<void(TInstant)>;
    using TUpdateUsedQuotaHandler = std::function<void(ui64)>;
    using TUpdateMaxUsedQuotaHandler = std::function<void()>;

    TRegisterHandler RegisterHandler;
    TUnregisterHandler UnregisterHandler;
    TTrimHandler TrimHandler;
    TUpdateUsedQuotaHandler UpdateUsedQuotaHandler;
    TUpdateMaxUsedQuotaHandler UpdateMaxUsedQuotaHandler;

    void Register(const TString& diskId, const TString& clientId) override
    {
        RegisterHandler(diskId, clientId);
    }

    void Unregister(const TString& diskId, const TString& clientId) override
    {
        UnregisterHandler(diskId, clientId);
    }

    void Trim(TInstant now) override
    {
        TrimHandler(now);
    }

    void UpdateUsedQuota(ui64 quota) override
    {
        UpdateUsedQuotaHandler(quota);
    }

    void UpdateMaxUsedQuota() override
    {
        UpdateMaxUsedQuotaHandler();
    }
};

}   // namespace NCloud::NBlockStore
