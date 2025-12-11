#pragma once

#include "public.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IThrottlerMetrics
{
    virtual ~IThrottlerMetrics() = default;

    virtual void Register(const TString& diskId, const TString& clientId) = 0;
    virtual void Unregister(const TString& diskId, const TString& clientId) = 0;
    virtual void Trim(TInstant now) = 0;

    virtual void UpdateUsedQuota(ui64 quota) = 0;
    virtual void UpdateMaxUsedQuota() = 0;
};

////////////////////////////////////////////////////////////////////////////////

IThrottlerMetricsPtr CreateThrottlerMetricsStub();
IThrottlerMetricsPtr CreateThrottlerMetrics(
    ITimerPtr timer,
    NMonitoring::TDynamicCountersPtr rootGroup,
    const TString& componentLabel);

}   // namespace NCloud::NBlockStore
