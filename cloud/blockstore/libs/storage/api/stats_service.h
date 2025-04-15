#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/storage/core/disk_counters.h>
#include <cloud/blockstore/libs/storage/core/metrics.h>
#include <cloud/blockstore/libs/storage/core/groups_info.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_STATS_SERVICE_REQUESTS(xxx, ...)                            \
    xxx(GetVolumeStats,                   __VA_ARGS__)                         \
// BLOCKSTORE_SERVICE_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvStatsService
{
    //
    // RegisterVolume notification
    //

    struct TRegisterVolume
    {
        const TString DiskId;
        const ui64 TabletId;
        const NProto::TVolume Config;

        TRegisterVolume(
                TString diskId,
                ui64 tabletId,
                NProto::TVolume config)
            : DiskId(std::move(diskId))
            , TabletId(tabletId)
            , Config(std::move(config))
        {}
    };

    //
    // UnregisterVolume notification
    //

    struct TUnregisterVolume
    {
        const TString DiskId;
        const NProto::TError Error;

        TUnregisterVolume(TString DiskId, NProto::TError error = {})
            : DiskId(std::move(DiskId))
            , Error(std::move(error))
        {}
    };

    //
    // VolumeConfigUpdated notification
    //

    struct TVolumeConfigUpdated
    {
        const TString DiskId;
        const NProto::TVolume Config;

        TVolumeConfigUpdated(
                TString diskId,
                NProto::TVolume config)
            : DiskId(std::move(diskId))
            , Config(std::move(config))
        {}
    };

    //
    // BootExternalResponse notification
    //

    struct TBootExternalResponse  // TODO:_ naming
    {
        const TString DiskId;
        const ui64 VolumeTabletId;  // TODO:_ seems we don't need this
        const ui64 PartitionTabletId;
        TVector<NKikimr::TTabletChannelInfo> ChannelInfos;

        TBootExternalResponse(
                TString diskId,
                ui64 volumeTabletId,
                ui64 partitionTabletId,
                TVector<NKikimr::TTabletChannelInfo> channelInfos)
            : DiskId(std::move(diskId))
            , VolumeTabletId(volumeTabletId)
            , PartitionTabletId(partitionTabletId)
            , ChannelInfos(std::move(channelInfos))
        {}
    };

    //
    // VolumePartCounters notification
    //

    struct TVolumePartCounters
    {
        const TString DiskId;
        TPartitionDiskCountersPtr DiskCounters;
        ui64 VolumeSystemCpu;
        ui64 VolumeUserCpu;
        bool HasCheckpoint;
        NBlobMetrics::TBlobLoadMetrics BlobLoadMetrics;

        TVolumePartCounters(
                TString diskId,
                TPartitionDiskCountersPtr diskCounters,
                ui64 volumeSystemCpu,
                ui64 volumeUserCpu,
                bool hasCheckpoint,
                NBlobMetrics::TBlobLoadMetrics metrics)
            : DiskId(std::move(diskId))
            , DiskCounters(std::move(diskCounters))
            , VolumeSystemCpu(volumeSystemCpu)
            , VolumeUserCpu(volumeUserCpu)
            , HasCheckpoint(hasCheckpoint)
            , BlobLoadMetrics(std::move(metrics))
        {}
    };

    //
    // VolumeSelfCounters notification
    //

    struct TVolumeSelfCounters
    {
        const TString DiskId;
        const bool HasClients;
        const bool IsPreempted;
        TVolumeSelfCountersPtr VolumeSelfCounters;
        const ui32 FailedBoots;

        TVolumeSelfCounters(
                TString diskId,
                bool hasClients,
                bool isPreempted,
                TVolumeSelfCountersPtr volumeCounters,
                ui32 failedBoots)
            : DiskId(std::move(diskId))
            , HasClients(hasClients)
            , IsPreempted(isPreempted)
            , VolumeSelfCounters(std::move(volumeCounters))
            , FailedBoots(failedBoots)
        {}

        TVolumeSelfCounters(
                TString diskId,
                bool hasClients,
                bool isPreempted,
                TVolumeSelfCountersPtr volumeCounters)
            : TVolumeSelfCounters(
                std::move(diskId),
                hasClients,
                isPreempted,
                std::move(volumeCounters),
                0)
        {}
    };

    //
    // GetVolumeStats
    //

    struct TGetVolumeStatsRequest
    {
        TVector<NProto::TVolumeBalancerDiskStats> VolumeStats;

        TGetVolumeStatsRequest() = default;

        TGetVolumeStatsRequest(TVector<NProto::TVolumeBalancerDiskStats> volumeStats)
            : VolumeStats(std::move(volumeStats))
        {}
    };

    struct TGetVolumeStatsResponse
    {
        TVector<NProto::TVolumeBalancerDiskStats> VolumeStats;

        TGetVolumeStatsResponse() = default;

        TGetVolumeStatsResponse(TVector<NProto::TVolumeBalancerDiskStats> volumeStats)
            : VolumeStats(std::move(volumeStats))
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::STATS_SERVICE_START,

        EvRegisterVolume,
        EvUnregisterVolume,
        EvVolumeConfigUpdated,
        EvBootExternalResponse,
        EvVolumePartCounters,
        EvVolumeSelfCounters,
        EvGetVolumeStatsRequest,
        EvGetVolumeStatsResponse,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::STATS_SERVICE_END,
        "EvEnd expected to be < TBlockStoreEvents::STATS_SERVICE_END");

    BLOCKSTORE_STATS_SERVICE_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)

    using TEvRegisterVolume = TRequestEvent<
        TRegisterVolume,
        EvRegisterVolume
    >;

    using TEvUnregisterVolume = TRequestEvent<
        TUnregisterVolume,
        EvUnregisterVolume
    >;

    using TEvVolumeConfigUpdated = TRequestEvent<
        TVolumeConfigUpdated,
        EvVolumeConfigUpdated
    >;

    using TEvBootExternalResponse = TRequestEvent<
        TBootExternalResponse,
        EvBootExternalResponse
    >;

    using TEvVolumePartCounters = TRequestEvent<
        TVolumePartCounters,
        EvVolumePartCounters
    >;

    using TEvVolumeSelfCounters = TRequestEvent<
        TVolumeSelfCounters,
        EvVolumeSelfCounters
    >;

};


////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeStorageStatsServiceId();

}   // namespace NCloud::NBlockStore::NStorage
