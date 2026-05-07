#include "volume_perf.h"

#include "config.h"

#include <cloud/blockstore/libs/service/request_helpers.h>

#include <cloud/storage/core/libs/common/media.h>
#include <cloud/storage/core/libs/throttling/helpers.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/threading/hot_swap/hot_swap.h>

#include <util/datetime/cputimer.h>
#include <util/generic/hash.h>
#include <util/system/rwlock.h>

namespace NCloud::NBlockStore {

using namespace NMonitoring;

////////////////////////////////////////////////////////////////////////////////

TVolumePerfSettings TVolumePerformanceCalculator::GetConfigSettings(
    TDiagnosticsConfigPtr diagnosticsConfig) const
{
    switch (MediaKind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
            return diagnosticsConfig->GetNonreplPerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: {
            return diagnosticsConfig->GetHddNonreplPerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: {
            return diagnosticsConfig->GetMirror2PerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: {
            return diagnosticsConfig->GetMirror3PerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL: {
            return diagnosticsConfig->GetLocalSSDPerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL: {
            return diagnosticsConfig->GetLocalHDDPerfSettings();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            return diagnosticsConfig->GetSsdPerfSettings();
        }
        default: {
            return diagnosticsConfig->GetHddPerfSettings();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TVolumePerformanceCalculator::TVolumePerformanceCalculator(
        const NProto::TVolume& volume,
        TDiagnosticsConfigPtr diagnosticsConfig)
    : MediaKind(volume.GetStorageMediaKind())
    , ConfigSettings(GetConfigSettings(diagnosticsConfig))
    , ExpectedIoParallelism(diagnosticsConfig->GetExpectedIoParallelism())
{
    TIntrusivePtr<TVolumePerfSettings> settings =
        new TVolumePerfSettings(ConfigSettings);
    PerfSettings.AtomicStore(settings);
}

void TVolumePerformanceCalculator::Register(const NProto::TVolume& volume)
{
    const auto old = *PerfSettings.AtomicLoad();
    if (!old.IsValid()) {
        return;
    }
    if (!ConfigSettings.IgnorePerformanceProfileLimits) {
        const auto& profile = volume.GetPerformanceProfile();
        TVolumePerfSettings clientSettings(
            Min(ConfigSettings.ReadIops, profile.GetMaxReadIops()),
            Min(ConfigSettings.ReadBandwidth, profile.GetMaxReadBandwidth()),
            Min(ConfigSettings.WriteIops, profile.GetMaxWriteIops()),
            Min(ConfigSettings.WriteBandwidth, profile.GetMaxWriteBandwidth()),
            ConfigSettings.CriticalFactor,
            ConfigSettings.IgnorePerformanceProfileLimits);

        if (clientSettings.IsValid() && old != clientSettings) {
            PerfSettings.AtomicStore(new TVolumePerfSettings(clientSettings));
        }
    }
    IsEnabled = true;
}

void TVolumePerformanceCalculator::Register(
    TDynamicCounters& counters,
    const NProto::TVolume& volume)
{
    Register(volume);
    if (IsEnabled && !Counter) {
        Counter = counters.GetCounter("Suffer", false);
    }
    if (IsEnabled && !SmoothCounter) {
        SmoothCounter = counters.GetCounter("SmoothSuffer", false);
    }
    if (IsEnabled && !CriticalCounter) {
        CriticalCounter = counters.GetCounter("CriticalSuffer", false);
    }
}

void TVolumePerformanceCalculator::OnRequestCompleted(
    EBlockStoreRequest requestType,
    ui64 requestStarted,
    ui64 requestCompleted,
    ui64 waitTime,
    ui32 requestBytes)
{
    bool isRead = IsReadRequest(requestType);
    bool isWrite = IsWriteRequest(requestType);

    if (IsEnabled && (isRead || isWrite)) {
        if (isRead) {
            AtomicAdd(
                ExpectedScore,
                GetExpectedReadCost(requestBytes).MicroSeconds());
        } else {
            AtomicAdd(
                ExpectedScore,
                GetExpectedWriteCost(requestBytes).MicroSeconds());
        }
        auto requestTime = requestCompleted - requestStarted;
        auto execTime = 0;
        if (requestTime > waitTime) {
            execTime = requestTime - waitTime;
        }
        AtomicAdd(CurrentScore, CyclesToDurationSafe(execTime).MicroSeconds());
    }
}

bool TVolumePerformanceCalculator::DidSuffer(
    long expectedScore,
    long actualScore) const
{
    return (expectedScore < ExpectedIoParallelism * 1e6)
        && (actualScore > expectedScore);
}

bool TVolumePerformanceCalculator::UpdateStats()
{
    if (!IsEnabled) {
        return false;
    }

    auto expectedScore = AtomicGet(ExpectedScore);
    auto actualScore = AtomicGet(CurrentScore);
    bool suffered = DidSuffer(expectedScore, actualScore);

    AtomicAdd(SufferCount, suffered - Samples[UpdateCounter].Suffered);
    Samples[UpdateCounter] = {suffered, expectedScore, actualScore};
    UpdateCounter = (UpdateCounter + 1) % SampleCount;

    ui64 windowExpectedScore = 0;
    ui64 windowActualScore = 0;
    for (const auto& sample: Samples) {
        windowExpectedScore += sample.ExpectedScore;
        windowActualScore += sample.ActualScore;
    }

    AtomicSet(
        SmoothSufferCount,
        DidSuffer(windowExpectedScore, windowActualScore));

    ui32 criticalFactor = Max(2u, ConfigSettings.CriticalFactor);
    AtomicSet(
        CriticalSufferCount,
        DidSuffer(windowExpectedScore * criticalFactor, windowActualScore));

    if (!UpdateCounter && Counter) {
        *Counter = SufferCount;
    }

    if (!UpdateCounter && SmoothCounter) {
        *SmoothCounter = SmoothSufferCount;
    }

    if (!UpdateCounter && CriticalCounter) {
        *CriticalCounter = CriticalSufferCount;
    }

    AtomicSub(CurrentScore, actualScore);
    AtomicSub(ExpectedScore, expectedScore);

    return suffered;
}

////////////////////////////////////////////////////////////////////////////////

ui64 TSufferCounters::UpdateCounter(
    TDynamicCounterPtr& counter,
    const TString& diskType,
    ui64 value)
{
    if (!counter) {
        counter = Counters
            ->GetSubgroup("type", diskType)
            ->GetCounter(DisksSufferCounterName, false);
    }
    *counter = value;
    return value;
}

void TSufferCounters::PublishCounters()
{
    ui64 total = 0;

    auto doUpdateCounter =
        [&](TDynamicCounterPtr& counter, NProto::EStorageMediaKind mediaKind)
    {
        total += UpdateCounter(
            counter,
            MediaKindToString(mediaKind),
            RunCounters[mediaKind]);
    };

    // clang-format off
    doUpdateCounter(Ssd,        NProto::STORAGE_MEDIA_SSD);
    doUpdateCounter(SsdNonrepl, NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    doUpdateCounter(HddNonrepl, NProto::STORAGE_MEDIA_HDD_NONREPLICATED);
    doUpdateCounter(SsdMirror2, NProto::STORAGE_MEDIA_SSD_MIRROR2);
    doUpdateCounter(SsdMirror3, NProto::STORAGE_MEDIA_SSD_MIRROR3);
    doUpdateCounter(SsdLocal,   NProto::STORAGE_MEDIA_SSD_LOCAL);
    doUpdateCounter(HddLocal,   NProto::STORAGE_MEDIA_HDD_LOCAL);

    total += UpdateCounter(
        Hdd,
        MediaKindToString(NProto::STORAGE_MEDIA_HDD),
        RunCounters      [NProto::STORAGE_MEDIA_HDD] +
        RunCounters      [NProto::STORAGE_MEDIA_HYBRID] +
        RunCounters      [NProto::STORAGE_MEDIA_DEFAULT]);
    // clang-format on

    if (!Total && !total) {
        return;
    }
    if (!Total) {
        Total = Counters->GetCounter(DisksSufferCounterName, false);
    }
    *Total = total;

    RunCounters.fill(0);
}

////////////////////////////////////////////////////////////////////////////////

TDuration TVolumePerformanceCalculator::GetExpectedReadCost(
    ui32 requestBytes) const
{
    auto perf = PerfSettings.AtomicLoad();
    return ExpectedIoParallelism * CostPerIO(
        perf->ReadIops,
        perf->ReadBandwidth,
        requestBytes);
}

TDuration TVolumePerformanceCalculator::GetExpectedWriteCost(
    ui32 requestBytes) const
{
    auto perf = PerfSettings.AtomicLoad();
    return ExpectedIoParallelism * CostPerIO(
        perf->WriteIops,
        perf->WriteBandwidth,
        requestBytes);
}

TDuration TVolumePerformanceCalculator::GetExpectedCost() const
{
    return TDuration::MicroSeconds(AtomicGet(ExpectedScore));
}

TDuration TVolumePerformanceCalculator::GetCurrentCost() const
{
    return TDuration::MicroSeconds(AtomicGet(CurrentScore));
}


}   // namespace NCloud::NBlockStore
