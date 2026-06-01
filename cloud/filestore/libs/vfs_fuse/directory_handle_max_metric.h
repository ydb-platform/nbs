#pragma once

#include <cloud/filestore/libs/diagnostics/metrics/label.h>
#include <cloud/filestore/libs/diagnostics/metrics/registry.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <atomic>
#include <memory>
#include <utility>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t DirectoryHandleMaxBucketCount = 60;   // 1 minute window

////////////////////////////////////////////////////////////////////////////////

template <size_t BucketCount>
class TMaxMetric
{
private:
    std::atomic<i64> Value = 0;
    std::unique_ptr<TMaxCalculator<BucketCount>> MaxCalc;
    std::atomic<i64> MaxCounter = 0;

public:
    explicit TMaxMetric(ITimerPtr timer)
        : MaxCalc(
              std::make_unique<TMaxCalculator<BucketCount>>(std::move(timer)))
    {}

    void Register(
        NMetrics::IMetricsRegistry& metricsRegistry,
        TStringBuf counterName)
    {
        metricsRegistry.Register(
            {NMetrics::CreateSensor(TString(counterName))},
            MaxCounter);
    }

    void Set(ui64 value)
    {
        Value.store(static_cast<i64>(value));
        MaxCalc->Add(value);
    }

    void Change(i64 delta)
    {
        const i64 newVal = Value.fetch_add(delta) + delta;
        Y_DEBUG_ABORT_UNLESS(newVal >= 0);
        MaxCalc->Add(static_cast<ui64>(newVal));
    }

    void UpdateMax()
    {
        MaxCalc->Add(static_cast<ui64>(Value.load()));
        MaxCounter.store(static_cast<i64>(MaxCalc->NextValue()));
    }

};

}   // namespace NCloud::NFileStore::NFuse
