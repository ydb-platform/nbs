#pragma once

#include "public.h"
#include "config.h"

#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

class TVolumePerformanceCalculator
{
    static constexpr const ui32 SampleCount =
        UpdateCountersInterval.Seconds() / UpdateStatsInterval.Seconds();

private:
    const NCloud::NProto::EStorageMediaKind MediaKind;
    const TVolumePerfSettings ConfigSettings;
    const ui32 ExpectedIoParallelism;

    TAtomic ExpectedScore = 0;
    TAtomic CurrentScore = 0;

    ui64 UpdateCounter = 0;
    TIntrusivePtr<NMonitoring::TCounterForPtr> Counter;

    bool IsEnabled = false;
    THotSwap<TVolumePerfSettings> PerfSettings;

    std::array<bool, SampleCount> Samples = {0};
    TAtomic SufferCount = 0;

private:
    constexpr bool IsRwRequest(EBlockStoreRequest requestType) const;

    TVolumePerfSettings GetConfigSettings(TDiagnosticsConfigPtr diagnosticsConfig) const;

public:
    TVolumePerformanceCalculator(
        const NProto::TVolume& volume,
        TDiagnosticsConfigPtr diagnosticsConfig);

    void Register(NMonitoring::TDynamicCounters& counters, const NProto::TVolume& volume);
    void Register(const NProto::TVolume& volume);

    void OnRequestCompleted(
        EBlockStoreRequest requestType,
        ui64 requestStarted,
        ui64 requestCompleted,
        ui64 postponedTime,
        ui32 requestBytes);

    bool UpdateStats();

    ui32 GetSufferCount() const
    {
        return AtomicGet(SufferCount);
    }

    bool IsSuffering() const
    {
        return GetSufferCount() != 0;
    }

    // for testing purpose
    TDuration GetExpectedReadCost(ui32 requestBytes) const;
    TDuration GetExpectedWriteCost(ui32 requestBytes) const;

    TDuration GetExpectedCost() const;
    TDuration GetCurrentCost() const;
};

////////////////////////////////////////////////////////////////////////////////

class TSufferCounters
{
private:
    using TDynamicCounterPtr = NMonitoring::TDynamicCounters::TCounterPtr;

    template<typename T>
    using TSufferArray =
        std::array<T, NCloud::NProto::EStorageMediaKind_ARRAYSIZE>;

    NMonitoring::TDynamicCountersPtr Counters;
    TDynamicCounterPtr Total;

    TSufferArray<ui64> RunCounters = {};

    TDynamicCounterPtr Hdd;
    TDynamicCounterPtr Ssd;
    TDynamicCounterPtr SsdNonrepl;
    TDynamicCounterPtr SsdMirror2;
    TDynamicCounterPtr SsdMirror3;
    TDynamicCounterPtr SsdLocal;

public:
    explicit TSufferCounters(NMonitoring::TDynamicCountersPtr counters)
        : Counters(std::move(counters))
    {}

    ui64 UpdateCounter(TDynamicCounterPtr& counter, TStringBuf label, ui64 value);

    void PublishCounters();

    void OnDiskSuffer(NCloud::NProto::EStorageMediaKind kind)
    {
        ++RunCounters[kind];
    }
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
