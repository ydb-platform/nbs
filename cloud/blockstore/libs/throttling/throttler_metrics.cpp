#include "throttler_metrics.h"

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TThrottlerMetricsStub final
    : public IThrottlerMetrics
{
public:
    void Register(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void Unregister(
        const TString& diskId,
        const TString& clientId) override
    {
        Y_UNUSED(diskId);
        Y_UNUSED(clientId);
    }

    void Trim(TInstant now) override
    {
        Y_UNUSED(now);
    }

    void UpdateUsedQuota(ui64 quota) override
    {
        Y_UNUSED(quota);
    }

    void UpdateMaxUsedQuota() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerMetricsPtr CreateThrottlerMetricsStub()
{
    return std::make_shared<TThrottlerMetricsStub>();
}

}   // namespace NCloud::NBlockStore
