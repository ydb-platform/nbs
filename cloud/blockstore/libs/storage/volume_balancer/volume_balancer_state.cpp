#include "volume_balancer_state.h"

#include <cloud/blockstore/libs/diagnostics/volume_stats.h>

#include <cloud/storage/core/libs/common/media.h>
#include "cloud/storage/core/libs/throttling/helpers.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/system/hostname.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration MaxPullDelay = TDuration::Hours(24);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVolumeBalancerState::TVolumeInfo::TVolumeInfo(TDuration pullInterval)
    : BackoffDelayProvider(pullInterval, MaxPullDelay)
    , LastSuccessfulPull(TInstant::Now())
{}

TVolumeBalancerState::TVolumeBalancerState(
    TStorageConfigPtr storageConfig,
    TDiagnosticsConfigPtr diagnosticsConfig)
    : StorageConfig(std::move(storageConfig))
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , InitialVolumePreemptionType(StorageConfig->GetVolumePreemptionType())
    , OverridenVolumePreemptionType(StorageConfig->GetVolumePreemptionType())
    , PullDelayResetTimespan(StorageConfig->GetInitialPullDelay())
{}

void TVolumeBalancerState::UpdateVolumeStats(
    TVector<NProto::TVolumeBalancerDiskStats> stats,
    TPerfGuaranteesMap perfMap,
    ui64 cpuLack,
    TInstant now)
{
    CpuLack = cpuLack;

    bool emergencyCpu =
        cpuLack >= StorageConfig->GetCpuLackThreshold();

    THashSet<TString> knownDisks;
    for (const auto& d: Volumes) {
        knownDisks.insert(d.first);
    }

    for (auto& v: stats) {
        auto it = Volumes.emplace(v.GetDiskId(), TVolumeInfo(StorageConfig->GetInitialPullDelay())).first;

        auto& info = it->second;

        info.PreemptionSource = v.GetPreemptionSource();
        info.CloudId = std::move(*v.MutableCloudId());
        info.FolderId = std::move(*v.MutableFolderId());
        info.Host = std::move(*v.MutableHost());
        info.MediaKind = v.GetStorageMediaKind();

        info.IsLocal = v.GetIsLocal();
        if (info.IsLocal) {
            if (info.NextPullAttempt != TInstant{}) {
                info.NextPullAttempt = {};
                info.LastSuccessfulPull = now;   // Rough but ok
            }
            if (info.LastSuccessfulPull + PullDelayResetTimespan <= now) {
                info.BackoffDelayProvider.Reset();
            }
        } else if (!info.NextPullAttempt) {
            info.NextPullAttempt =
                now + info.BackoffDelayProvider.GetDelayAndIncrease();
        }

        if (auto perfIt = perfMap.find(it->first); perfIt != perfMap.end()) {
            info.SufferCount = perfIt->second;
        }

        if (info.IsLocal) {
            TYdbDiskLoadCounters currentLoadCounters{
                v.GetReadBlobCount(),
                v.GetWriteBlobCount(),
                v.GetReadBlobBytes(),
                v.GetWriteBlobBytes(),
            };

            info.Cost = CalculateCost(info, currentLoadCounters);

            info.LoadCounters = currentLoadCounters;
        } else {
            info.Cost = MakeError(E_FAIL, "Volume is preempted");
            info.LoadCounters = std::nullopt;
        }

        knownDisks.erase(v.GetDiskId());
    }

    for (const auto& d: knownDisks) {
        Volumes.erase(d);
    }

    if (emergencyCpu) {
        VolumeToPull = {};
        UpdateVolumeToPush();
    } else {
        VolumeToPush = {};
        UpdateVolumeToPull(now);
    }
}

TString TVolumeBalancerState::GetVolumeToPush() const
{
    return VolumeToPush;
}

TString TVolumeBalancerState::GetVolumeToPull() const
{
    return VolumeToPull;
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeBalancerState::RenderLocalVolumes(TStringStream& out) const
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Volume"; }
                    TABLEH() { out << "Preemption allowed"; }
                    TABLEH() { out << "Suffer Count"; }
                    TABLEH() { out << "IO Cost"; }
                }
            }
            for (const auto& [diskId, info]: Volumes) {
                if (info.IsLocal) {
                    TABLER() {
                        TABLED() { out << diskId; }
                        TABLED() {
                            const bool enabled =
                                IsVolumePreemptible(diskId, info);
                            out << (enabled ? "Yes" : "No");
                        }
                        TABLED() {
                            out <<info.SufferCount;
                        }
                        TABLED() {
                            out
                                << (HasError(info.Cost.GetError())
                                        ? info.Cost.GetError().GetMessage()
                                        : info.Cost.GetResult().ToString());
                        }
                    }
                }
            }
        }
    }
}

void TVolumeBalancerState::RenderPreemptedVolumes(
    TStringStream& out,
    TInstant now) const
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-bordered") {
            TABLEHEAD() {
                TABLER() {
                    TABLEH() { out << "Volume"; }
                    TABLEH() { out << "Next pull delay timeout"; }
                    TABLEH() { out << "Estimated time to pull back"; }
                }
            }
            for (const auto& v: Volumes) {
                if (!v.second.IsLocal) {
                    auto cls = (v.second.NextPullAttempt <= now)
                        ? "success"
                        : "danger";
                    TABLER_CLASS(cls) {
                        TABLED() { out << v.first; }
                        TABLED() {
                            out << v.second.BackoffDelayProvider.GetDelay();
                        }
                        TABLED() { out << v.second.NextPullAttempt; }
                    }
                }
            }
        }
    }
}

void TVolumeBalancerState::RenderConfig(TStringStream& out) const
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Volume preemption type"; }
                    TABLED() {
                        out << EVolumePreemptionType_Name(
                            StorageConfig->GetVolumePreemptionType());
                    }
                }
                TABLER() {
                    TABLED() { out << "CpuLackThreshold"; }
                    TABLED() { out << StorageConfig->GetCpuLackThreshold(); }
                }
                TABLER() {
                    TABLED() { out << "Initial pull delay"; }
                    TABLED() { out << StorageConfig->GetInitialPullDelay(); }
                }
            }
        }
    }
}

void TVolumeBalancerState::RenderState(TStringStream& out) const
{
    HTML(out) {
        TABLE_SORTABLE_CLASS("table table-condensed") {
            TABLEHEAD() {
                TABLER() {
                    TABLED() { out << "Enabled"; }
                    TABLED() { out << ToString(IsEnabled); }
                }
                TABLER() {
                    TABLED() { out << "CPUs needed"; }
                    TABLED() { out << CpuLack; }
                }
            }
        }
    }
}

void TVolumeBalancerState::RenderHtml(TStringStream& out, TInstant now) const
{
    HTML(out) {
        TAG(TH3) { out << "State"; }
        RenderState(out);

        TAG(TH3) { out << "Config"; }
        RenderConfig(out);

        TAG(TH3) { out << "Local Volumes"; }
        RenderLocalVolumes(out);

        TAG(TH3) { out << "Preempted Volumes"; }
        RenderPreemptedVolumes(out, now);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TVolumeBalancerState::UpdateVolumeToPush()
{
    const bool moveMostHeavy = StorageConfig->GetVolumePreemptionType() ==
        NProto::PREEMPTION_MOVE_MOST_HEAVY;

    VolumeToPush = {};

    ui64 value = moveMostHeavy ? 0 : Max<ui64>();
    for (auto v = Volumes.begin(); v != Volumes.end(); ++v) {
        if (!IsVolumePreemptible(v->first, v->second)) {
            continue;
        }

        if (moveMostHeavy) {
            if (value < v->second.SufferCount) {
                value = v->second.SufferCount;
                VolumeToPush = v->first;
            }
        } else {
            if (value > v->second.SufferCount) {
                value = v->second.SufferCount;
                VolumeToPush = v->first;
            }
        }
    }
}

void TVolumeBalancerState::UpdateVolumeToPull(TInstant now)
{
    VolumeToPull = {};

    for (const auto& v: Volumes) {
        if (!v.second.IsLocal &&
            v.second.PreemptionSource == NProto::EPreemptionSource::SOURCE_BALANCER &&
            v.second.NextPullAttempt <= now &&
            !VolumesInProgress.count(v.first))
        {
            VolumeToPull = v.first;
            return;
        }
    }

}

bool TVolumeBalancerState::IsVolumePreemptible(
    const TString& diskId,
    const TVolumeInfo& volume) const
{
    const bool isFeatureEnabledForFolder = StorageConfig->IsBalancerFeatureEnabled(
        volume.CloudId,
        volume.FolderId,
        diskId);

    const bool balancerEnabled = isFeatureEnabledForFolder || GetEnabled();

    // NProto::STORAGE_MEDIA_DEFAULT means that volume mounting
    // is still in progress and will change to something else
    // as soon as it completes.
    const bool isSuitableMediaKind =
        (volume.MediaKind != NProto::STORAGE_MEDIA_DEFAULT) &&
        !IsDiskRegistryMediaKind(volume.MediaKind);

    return volume.IsLocal &&
        balancerEnabled &&
        isSuitableMediaKind &&
        !VolumesInProgress.count(diskId);
}

TResultOrError<TDuration> TVolumeBalancerState::CalculateCost(
        const TVolumeInfo& info,
        const TYdbDiskLoadCounters& currentLoad) const
{
    if (!info.LoadCounters.has_value()) {
        // Unable to calculate cost: No counters from previous iteration
        return MakeError(E_FAIL, "No counters from previous iteration");
    }

    const auto& last = info.LoadCounters.value();

    if (currentLoad.WriteBlobBytes < last.WriteBlobBytes ||
        currentLoad.WriteBlobCount < last.WriteBlobCount ||
        currentLoad.ReadBlobBytes < last.ReadBlobBytes ||
        currentLoad.WriteBlobCount < last.WriteBlobCount)
    {
        // Unable to calculate cost: Negative counters diff
        return MakeError(E_FAIL, "Negative counters diff");
    }

    const auto perfSettings =
        GetPerfSettings(*DiagnosticsConfig, info.MediaKind);

    if (!perfSettings.WriteIops || !perfSettings.ReadIops) {
        // Unable to calculate cost: CostPerIO is undefined for 0 maxIops
        return MakeError(E_FAIL, "Perf settings not configured");
    }

    const ui32 expectedParallelism =
        DiagnosticsConfig->GetExpectedIoParallelism();

    const TDuration writeCost =
        expectedParallelism *
        CostPerIO(
            perfSettings.WriteIops,
            perfSettings.WriteBandwidth,
            currentLoad.WriteBlobBytes - last.WriteBlobBytes,
            currentLoad.WriteBlobCount - last.WriteBlobCount);

    const TDuration readCost =
        expectedParallelism *
        CostPerIO(
            perfSettings.ReadIops,
            perfSettings.ReadBandwidth,
            currentLoad.ReadBlobBytes - last.ReadBlobBytes,
            currentLoad.WriteBlobCount - last.WriteBlobCount);

    return writeCost + readCost;
}

}   // namespace NCloud::NBlockStore::NStorage
