#pragma once

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TUsedQuotaMetrics
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TUsedQuotaMetrics(ITimerPtr timer);
    ~TUsedQuotaMetrics();

    void Register(NMonitoring::TDynamicCountersPtr counters);
    void Unregister();
    void UpdateQuota(ui64 quota);
    void UpdateMaxQuota();
    void UpdateMaxQuota(ui64 maxQuota);
};

}   // namespace NCloud::NBlockStore
