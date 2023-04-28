#include "throttler_metrics.h"

#include <cloud/blockstore/libs/diagnostics/quota_metrics.h>
#include <cloud/blockstore/libs/throttling/throttler_metrics_base.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

using TQuotaKey = std::pair<TString, TString>;

class TThrottlerMetrics final
    : public TThrottlerMetricsBase<TThrottlerMetrics, TQuotaKey>
{
private:
    TLog Log;

public:
    TThrottlerMetrics(
            ITimerPtr timer,
            NMonitoring::TDynamicCountersPtr rootGroup,
            ILoggingServicePtr logging)
        : TThrottlerMetricsBase(
            std::move(timer),
            rootGroup->GetSubgroup("component", "server"),
            rootGroup
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster"))
        , Log(logging->CreateLog("BLOCKSTORE_SERVER"))
    {}

    void Register(
        const TString& diskId,
        const TString& clientId) override
    {
        RegisterQuota(MakeQuotaKey(diskId, clientId));
    }

    void Unregister(
        const TString& diskId,
        const TString& clientId) override
    {
        UnregisterQuota(MakeQuotaKey(diskId, clientId));
    }

    std::pair<TString, TString> GetMetricsPath(const TQuotaKey& key) const
    {
        return key;
    }

    TQuotaKey MakeQuotaKey(const TString& diskId, const TString& clientId) const
    {
        if (clientId.empty()) {
            STORAGE_ERROR("Client id is empty");
        }

        if (diskId.empty()) {
            STORAGE_ERROR("Disk id is empty");
        }

        return std::make_pair(
            diskId.empty() ? "none" : diskId,
            clientId.empty() ? "none" : clientId);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerMetricsPtr CreateServiceThrottlerMetrics(
    ITimerPtr timer,
    NMonitoring::TDynamicCountersPtr rootGroup,
    ILoggingServicePtr logging)
{
    return std::make_shared<TThrottlerMetrics>(
        std::move(timer),
        std::move(rootGroup),
        std::move(logging));
}

}   // namespace NCloud::NBlockStore
