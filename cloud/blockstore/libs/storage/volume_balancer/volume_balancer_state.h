#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/config.h>

#include <cloud/storage/core/libs/features/features_config.h>

#include <contrib/ydb/core/tablet/tablet_metrics.h>

#include <util/datetime/base.h>
#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TVolumeBalancerState
{
    struct TVolumeInfo
    {
        TString CloudId;
        TString FolderId;

        bool IsLocal = true;
        NProto::EPreemptionSource PreemptionSource = NProto::SOURCE_BALANCER;
        NProto::EStorageMediaKind MediaKind = NProto::STORAGE_MEDIA_DEFAULT;

        TString Host;

        TInstant NextPullAttempt;
        TDuration PullInterval;

        ui32 SufferCount = 0;

        TVolumeInfo(TDuration pullInterval)
            : PullInterval(pullInterval)
        {}
    };

public:
    using TPerfGuaranteesMap = THashMap<TString, ui32>;

private:
    TStorageConfigPtr StorageConfig;

    ui64 CpuLack = 0;

    THashMap<TString, TVolumeInfo> Volumes;
    THashSet<TString> VolumesInProgress;

    TInstant LastStateChange;

    TString VolumeToPush;
    TString VolumeToPull;

    bool IsEnabled = true;

    const NProto::EVolumePreemptionType InitialVolumePreemptionType;
    NProto::EVolumePreemptionType OverrideVolumePreemptionType;

public:
    TVolumeBalancerState(TStorageConfigPtr storageConfig);

    TString GetVolumeToPush() const;
    TString GetVolumeToPull() const;

    void UpdateVolumeStats(
        TVector<NProto::TVolumeBalancerDiskStats> stats,
        TPerfGuaranteesMap perfMap,
        ui64 cpuLack,
        TInstant now);

    void RenderHtml(TStringStream& out, TInstant now) const;

    void SetEnabled(bool enable)
    {
        IsEnabled = enable;
    }

    bool GetEnabled() const
    {
        return GetVolumePreemptionType() != NProto::PREEMPTION_NONE &&
               IsEnabled;
    }

    void OverrideVolumePreemptionTypeIfPossible(
        NProto::EVolumePreemptionType volumePreemptionType)
    {
        OverrideVolumePreemptionType = volumePreemptionType;
    }

    NProto::EVolumePreemptionType GetVolumePreemptionType() const
    {
        // We prioritize Immediate Control Board overriden configs over
        // Config Dispatcher ones
        return StorageConfig->GetVolumePreemptionType() ==
                       InitialVolumePreemptionType
                   ? OverrideVolumePreemptionType
                   : StorageConfig->GetVolumePreemptionType();
    }

    void SetVolumeInProgress(TString volume)
    {
        VolumesInProgress.emplace(std::move(volume));
    }

    void SetVolumeInProgressCompleted(TString volume)
    {
        VolumesInProgress.erase(std::move(volume));
    }

private:
    void RenderLocalVolumes(TStringStream& out) const;
    void RenderPreemptedVolumes(TStringStream& out, TInstant now) const;
    void RenderConfig(TStringStream& out) const;
    void RenderState(TStringStream& out) const;

    void UpdateVolumeToPush();
    void UpdateVolumeToPull(TInstant now);

    bool IsVolumePreemptible(
        const TString& diskId,
        const TVolumeInfo& volume) const;
};

}   // namespace NCloud::NBlockStore::NStorage
