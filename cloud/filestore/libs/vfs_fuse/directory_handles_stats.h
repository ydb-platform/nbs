#pragma once

#include <cloud/filestore/libs/diagnostics/module_stats.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/max_calculator.h>

#include <library/cpp/deprecated/atomic/atomic.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/yassert.h>

#include <memory>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t DirectoryHandlesMaxBucketCount = 60;   // 1 minute window

////////////////////////////////////////////////////////////////////////////////

template <size_t BucketCount>
class TMaxMetric
{
private:
    TAtomic Value = 0;
    std::unique_ptr<TMaxCalculator<BucketCount>> MaxCalc;
    NMonitoring::TDynamicCounters::TCounterPtr MaxCounter;

public:
    TMaxMetric(
        ITimerPtr timer,
        NMonitoring::TDynamicCountersPtr counters,
        TStringBuf counterName)
        : MaxCalc(
              std::make_unique<TMaxCalculator<BucketCount>>(std::move(timer)))
        , MaxCounter(counters->GetCounter(TString{counterName}, false))
    {}

    void Change(i64 delta)
    {
        i64 newVal = AtomicAdd(Value, delta);
        Y_DEBUG_ABORT_UNLESS(newVal >= 0);
        MaxCalc->Add(newVal);
    }

    void UpdateMax()
    {
        MaxCounter->Set(MaxCalc->NextValue());
    }
};

////////////////////////////////////////////////////////////////////////////////

class TDirectoryHandlesStats final: public IModuleStats
{
private:
    NMonitoring::TDynamicCountersPtr Counters;
    TMaxMetric<DirectoryHandlesMaxBucketCount> CacheSize;
    TMaxMetric<DirectoryHandlesMaxBucketCount> ChunkCount;

    void ChangeCacheSize(i64 delta);
    void ChangeChunkCount(i64 delta);

public:
    explicit TDirectoryHandlesStats(ITimerPtr timer);

    TStringBuf GetName() const override;
    NMonitoring::TDynamicCountersPtr GetCounters() override;

    void IncreaseCacheSize(size_t value);
    void DecreaseCacheSize(size_t value);
    void IncreaseChunkCount(size_t value);
    void DecreaseChunkCount(size_t value);

    void UpdateStats() override;
};

using TDirectoryHandlesStatsPtr = std::shared_ptr<TDirectoryHandlesStats>;

////////////////////////////////////////////////////////////////////////////////

TDirectoryHandlesStatsPtr CreateDirectoryHandlesStats(
    IModuleStatsRegistryPtr registry,
    ITimerPtr timer,
    const TString& fileSystemId,
    const TString& clientId,
    const TString& cloudId,
    const TString& folderId);

}   // namespace NCloud::NFileStore::NFuse
