#pragma once

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <cloud/storage/core/libs/common/media.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TDiskPerfData
{
    bool CountersRegistered = false;
    bool HasCheckpoint = false;
    bool HasClients = false;
    bool IsPreempted = false;

    TPartitionDiskCounters DiskCounters;
    TVolumeSelfCounters VolumeSelfCounters;

    TPartitionDiskCounters YdbDiskCounters;
    TVolumeSelfCounters YdbVolumeSelfCounters;

    NMonitoring::TDynamicCounters::TCounterPtr VolumeBindingCounter;

    ui64 VolumeSystemCpu = 0;
    ui64 VolumeUserCpu = 0;

    TDiskPerfData(
            EPublishingPolicy policy,
            EHistogramCounterOptions histCounterOptions)
        : DiskCounters(policy, histCounterOptions)
        , VolumeSelfCounters(policy, histCounterOptions)
        , YdbDiskCounters(policy, histCounterOptions)
        , YdbVolumeSelfCounters(policy, histCounterOptions)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TTotalCounters
{
    TPartitionDiskCounters PartAcc;
    TVolumeSelfCounters VolumeAcc;
    TSimpleCounter TotalDiskCount;
    TSimpleCounter TotalDiskCountLast15Min;
    TSimpleCounter TotalDiskCountLastHour;
    TSimpleCounter TotalPartitionCount;
    TSimpleCounter VolumeLoadTimeUnder1Sec;
    TSimpleCounter VolumeLoadTime1To5Sec;
    TSimpleCounter VolumeLoadTimeOver5Sec;
    TSimpleCounter VolumeStartTimeUnder1Sec;
    TSimpleCounter VolumeStartTime1To5Sec;
    TSimpleCounter VolumeStartTimeOver5Sec;

    TTotalCounters(
            EPublishingPolicy policy,
            EHistogramCounterOptions histCounterOptions)
        : PartAcc(policy, histCounterOptions)
        , VolumeAcc(policy, histCounterOptions)
    {}

    void Register(NMonitoring::TDynamicCountersPtr counters);
    void Reset();
    void Publish(TInstant now);
    void UpdatePartCounters(const TPartitionDiskCounters& source);
    void UpdateVolumeSelfCounters(const TVolumeSelfCounters& source);
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeRequestCounters
{
    TCumulativeCounter ReadCount;
    TCumulativeCounter ReadBytes;
    TCumulativeCounter ReadVoidBytes;
    TCumulativeCounter ReadNonVoidBytes;
    TCumulativeCounter WriteCount;
    TCumulativeCounter WriteBytes;
    TCumulativeCounter ZeroCount;
    TCumulativeCounter ZeroBytes;

    void Register(NMonitoring::TDynamicCountersPtr counters);
    void Publish(TInstant now);
    void Reset();
    void UpdateCounters(const TPartitionDiskCounters& source);
};

////////////////////////////////////////////////////////////////////////////////

class TBlobLoadCounters
{
public:
    TBlobLoadCounters(
        const TString& mediaKind,
        ui64 maxGroupReadIops,
        ui64 maxGroupWriteIops,
        ui64 maxGroupReadThroughput,
        ui64 maxGroupWriteThroughput);

    void Register(NMonitoring::TDynamicCountersPtr counters);
    void Publish(const NBlobMetrics::TBlobLoadMetrics& metrics, TInstant now);

public:
    const TString MediaKind;
    const ui64 MaxGroupReadIops;
    const ui64 MaxGroupWriteIops;
    const ui64 MaxGroupReadThroughput;
    const ui64 MaxGroupWriteThroughput;

    TSolomonValueHolder UsedGroupsCount;
};

////////////////////////////////////////////////////////////////////////////////

struct TVolumeStatsInfo
{
    NProto::TVolume VolumeInfo;
    ui64 VolumeTabletId = 0;

    TDiskPerfData PerfCounters;
    NBlobMetrics::TBlobLoadMetrics OffsetBlobMetrics;
    TInstant ApproximateStartTs;
    TDuration ApproximateBootstrapTime;

    THashMap<ui64, TVector<NKikimr::TTabletChannelInfo>> ChannelInfos;

    TVolumeStatsInfo(
            NProto::TVolume config,
            EHistogramCounterOptions histCounterOptions)
        : VolumeInfo(std::move(config))
        , PerfCounters(EPublishingPolicy::All, histCounterOptions)
    {}

    bool IsDiskRegistryBased() const
    {
        return IsDiskRegistryMediaKind(VolumeInfo.GetStorageMediaKind());
    }
};

struct TRecentVolumeStatsInfo
{
    TString DiskId;
    bool IsSystem = false;
    NProto::EStorageMediaKind StorageMediaKind = NProto::STORAGE_MEDIA_DEFAULT;
    TInstant RemoveTs;
};

////////////////////////////////////////////////////////////////////////////////

class TStatsServiceState
{
public:
    using TVolumesMap = THashMap<TString, TVolumeStatsInfo>;
    using TRecentVolumesList = TList<TRecentVolumeStatsInfo>;
    using TRecentVolumesMap = THashMap<TStringBuf, TRecentVolumesList::iterator>;

private:
    TVolumesMap VolumesById;

    TRecentVolumesList RecentVolumes;
    TRecentVolumesMap RecentVolumesById;

    TTotalCounters Total;
    TTotalCounters Hdd;
    TTotalCounters Ssd;
    TTotalCounters SsdNonrepl;
    TTotalCounters HddNonrepl;
    TTotalCounters SsdMirror2;
    TTotalCounters SsdMirror3;
    TTotalCounters SsdLocal;
    TTotalCounters HddLocal;
    TTotalCounters SsdSystem;
    TTotalCounters HddSystem;

    TVolumeRequestCounters LocalVolumes;
    TVolumeRequestCounters NonlocalVolumes;

    TBlobLoadCounters SsdBlobLoadCounters;
    TBlobLoadCounters HddBlobLoadCounters;

    bool StatsUploadingCompleted = true;
    EHistogramCounterOptions HistCounterOptions;

public:
    void RemoveVolume(TInstant now, const TString& diskId);
    TVolumeStatsInfo* GetVolume(const TString& diskId);
    TVolumeStatsInfo* GetOrAddVolume(
        const TString& diskId,
        NProto::TVolume config);

    TStatsServiceState(
            const TStorageConfig& config,
            const TDiagnosticsConfig& diagConfig)
        : Total(EPublishingPolicy::All, diagConfig.GetHistogramCounterOptions())
        , Hdd(EPublishingPolicy::Repl, diagConfig.GetHistogramCounterOptions())
        , Ssd(EPublishingPolicy::Repl, diagConfig.GetHistogramCounterOptions())
        , SsdNonrepl(
            EPublishingPolicy::DiskRegistryBased,
            diagConfig.GetHistogramCounterOptions())
        , HddNonrepl(
            EPublishingPolicy::DiskRegistryBased,
            diagConfig.GetHistogramCounterOptions())
        , SsdMirror2(
            EPublishingPolicy::DiskRegistryBased,
            diagConfig.GetHistogramCounterOptions())
        , SsdMirror3(
            EPublishingPolicy::DiskRegistryBased,
            diagConfig.GetHistogramCounterOptions())
        , SsdLocal(
            EPublishingPolicy::DiskRegistryBased,
            diagConfig.GetHistogramCounterOptions())
        , HddLocal(
            EPublishingPolicy::DiskRegistryBased,
            diagConfig.GetHistogramCounterOptions())
        , SsdSystem(
            EPublishingPolicy::Repl,
            diagConfig.GetHistogramCounterOptions())
        , HddSystem(
            EPublishingPolicy::Repl,
            diagConfig.GetHistogramCounterOptions())
        , SsdBlobLoadCounters(
            config.GetCommonSSDPoolKind(),
            config.GetMaxSSDGroupReadIops(),
            config.GetMaxSSDGroupWriteIops(),
            config.GetMaxSSDGroupReadBandwidth(),
            config.GetMaxSSDGroupWriteBandwidth())
        , HddBlobLoadCounters(
            config.GetCommonHDDPoolKind(),
            config.GetMaxHDDGroupReadIops(),
            config.GetMaxHDDGroupWriteIops(),
            config.GetMaxHDDGroupReadBandwidth(),
            config.GetMaxHDDGroupWriteBandwidth())
        , HistCounterOptions(diagConfig.GetHistogramCounterOptions())
    {}

    const TVolumesMap& GetVolumes() const
    {
        return VolumesById;
    }

    TVolumesMap& GetVolumes()
    {
        return VolumesById;
    }

    const TRecentVolumesList& UpdateAndGetRecentVolumes(TInstant now);

    TTotalCounters& GetTotalCounters()
    {
        return Total;
    }

    TTotalCounters& GetHddCounters()
    {
        return Hdd;
    }

    TTotalCounters& GetSsdCounters()
    {
        return Ssd;
    }

    TTotalCounters& GetSsdNonreplCounters()
    {
        return SsdNonrepl;
    }

    TTotalCounters& GetHddNonreplCounters()
    {
        return HddNonrepl;
    }

    TTotalCounters& GetSsdMirror2Counters()
    {
        return SsdMirror2;
    }

    TTotalCounters& GetSsdMirror3Counters()
    {
        return SsdMirror3;
    }

    TTotalCounters& GetSsdLocalCounters()
    {
        return SsdLocal;
    }

    TTotalCounters& GetHddLocalCounters()
    {
        return HddLocal;
    }

    TTotalCounters& GetSsdSystemCounters()
    {
        return SsdSystem;
    }

    TTotalCounters& GetHddSystemCounters()
    {
        return HddSystem;
    }

    TTotalCounters& GetCounters(
        bool isSystem,
        const NProto::EStorageMediaKind mediaKind)
    {
        switch (mediaKind) {
            case NCloud::NProto::STORAGE_MEDIA_SSD: {
                return isSystem ? SsdSystem : Ssd;
            }
            case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: return SsdNonrepl;
            case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: return HddNonrepl;
            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: return SsdMirror2;
            case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: return SsdMirror3;
            case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL: return SsdLocal;
            case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL: return HddLocal;
            case NCloud::NProto::STORAGE_MEDIA_HDD:
            case NCloud::NProto::STORAGE_MEDIA_HYBRID:
            case NCloud::NProto::STORAGE_MEDIA_DEFAULT:
                return isSystem ? HddSystem : Hdd;
            default: {}
        }

        Y_ABORT("unsupported media kind: %u", static_cast<ui32>(mediaKind));
    }

    TTotalCounters& GetCounters(const NProto::TVolume& volume)
    {
        return GetCounters(volume.GetIsSystem(), volume.GetStorageMediaKind());
    }

    TVolumeRequestCounters& GetLocalVolumesCounters()
    {
        return LocalVolumes;
    }

    TVolumeRequestCounters& GetNonlocalVolumesCounters()
    {
        return NonlocalVolumes;
    }

    TBlobLoadCounters& GetSsdBlobCounters()
    {
        return SsdBlobLoadCounters;
    }

    TBlobLoadCounters& GetHddBlobCounters()
    {
        return HddBlobLoadCounters;
    }

    void SetStatsUploadingCompleted(bool completed)
    {
        StatsUploadingCompleted = completed;
    }

    bool GetStatsUploadingCompleted() const
    {
        return StatsUploadingCompleted;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
