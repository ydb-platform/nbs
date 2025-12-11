#include "quota_metrics.h"

#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TUsedQuotaMetrics::TImpl
{
    NMonitoring::TDynamicCountersPtr Group;
    NMonitoring::TDynamicCounters::TCounterPtr UsedQuota;
    NMonitoring::TDynamicCounters::TCounterPtr MaxUsedQuota;

    TMaxCalculator<DEFAULT_BUCKET_COUNT> MaxUsedQuotaCalc;

    TAtomic IsInitialized = 0;
    TRWMutex Mutex;

    explicit TImpl(ITimerPtr timer)
        : MaxUsedQuotaCalc(std::move(timer))
    {}

    void Register(NMonitoring::TDynamicCountersPtr counters)
    {
        auto guard = TWriteGuard(Mutex);
        if (!AtomicGet(IsInitialized) && counters) {
            Group = std::move(counters);
            UsedQuota = Group->GetCounter("UsedQuota");
            MaxUsedQuota = Group->GetCounter("MaxUsedQuota");
            AtomicSet(IsInitialized, 1);
        }
    }

    void Unregister()
    {
        auto guard = TWriteGuard(Mutex);
        if (AtomicGet(IsInitialized)) {
            Group->RemoveCounter("UsedQuota");
            Group->RemoveCounter("MaxUsedQuota");
            UsedQuota.Drop();
            MaxUsedQuota.Drop();
            for (ui32 i = 0; i < DEFAULT_BUCKET_COUNT + 1; ++i) {
                MaxUsedQuotaCalc.Add(0);
                MaxUsedQuotaCalc.NextValue();
            }
            AtomicSet(IsInitialized, 0);
        }
    }

    void UpdateQuota(ui64 quota)
    {
        auto guard = TReadGuard(Mutex);
        MaxUsedQuotaCalc.Add(quota);
        if (UsedQuota) {
            UsedQuota->Set(quota);
        }
    }

    void UpdateMaxQuota()
    {
        auto guard = TWriteGuard(Mutex);
        if (MaxUsedQuota) {
            MaxUsedQuota->Set(MaxUsedQuotaCalc.NextValue());
        }
    }

    void UpdateMaxQuota(ui64 maxQuota)
    {
        auto guard = TWriteGuard(Mutex);
        MaxUsedQuotaCalc.NextValue();
        if (MaxUsedQuota) {
            MaxUsedQuota->Set(maxQuota);
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

TUsedQuotaMetrics::TUsedQuotaMetrics(ITimerPtr timer)
    : Impl(std::make_unique<TImpl>(std::move(timer)))
{}

TUsedQuotaMetrics::~TUsedQuotaMetrics() = default;

void TUsedQuotaMetrics::Register(NMonitoring::TDynamicCountersPtr counters)
{
    if (!AtomicGet(Impl->IsInitialized)) {
        Impl->Register(std::move(counters));
    }
}

void TUsedQuotaMetrics::Unregister()
{
    if (AtomicGet(Impl->IsInitialized)) {
        Impl->Unregister();
    }
}

void TUsedQuotaMetrics::UpdateQuota(ui64 quota)
{
    if (AtomicGet(Impl->IsInitialized)) {
        Impl->UpdateQuota(quota);
    }
}

void TUsedQuotaMetrics::UpdateMaxQuota()
{
    if (AtomicGet(Impl->IsInitialized)) {
        Impl->UpdateMaxQuota();
    }
}

void TUsedQuotaMetrics::UpdateMaxQuota(ui64 maxQuota)
{
    if (AtomicGet(Impl->IsInitialized)) {
        Impl->UpdateMaxQuota(maxQuota);
    }
}

}   // namespace NCloud::NBlockStore
