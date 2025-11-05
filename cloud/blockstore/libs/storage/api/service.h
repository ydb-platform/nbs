#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/blockstore/libs/storage/protos/volume.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SERVICE_REQUESTS(xxx, ...)                                  \
    xxx(ChangeVolumeBinding,     __VA_ARGS__)                                  \
    xxx(GetVolumeStats,          __VA_ARGS__)                                  \
    xxx(RunVolumesLivenessCheck, __VA_ARGS__)                                  \
    xxx(AddTags,                 __VA_ARGS__)                                  \
// BLOCKSTORE_SERVICE_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvService
{
    //
    // ChangeVolumeBinding
    //

    struct TChangeVolumeBindingRequest
    {
        enum class EChangeBindingOp
        {
            RELEASE_TO_HIVE,
            ACQUIRE_FROM_HIVE
        };

        const TString DiskId;
        const EChangeBindingOp Action;
        const NProto::EPreemptionSource Source;

        TChangeVolumeBindingRequest(
                TString diskId,
                EChangeBindingOp action,
                NProto::EPreemptionSource source)
            : DiskId(std::move(diskId))
            , Action(action)
            , Source(source)
        {}
    };

    struct TChangeVolumeBindingResponse
    {
        const TString DiskId;

        TChangeVolumeBindingResponse() = default;

        TChangeVolumeBindingResponse(TString diskId)
            : DiskId(std::move(diskId))
        {}
    };

    //
    // GetVolumeStats
    //

    struct TGetVolumeStatsRequest
    {
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

        TUnregisterVolume(TString diskId, NProto::TError error = {})
            : DiskId(diskId)
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
    // RunVolumeLivenessCheckRequest
    //

    struct TRunVolumesLivenessCheckRequest
    {
    };

    //
    // RunVolumeLivenessCheckResponse
    //

    struct TRunVolumesLivenessCheckResponse
    {
        TVector<TString> DeletedVolumes;
        TVector<TString> LiveVolumes;

        TRunVolumesLivenessCheckResponse() = default;

        TRunVolumesLivenessCheckResponse(
                TVector<TString> deletedVolumes,
                TVector<TString> liveVolumes)
            : DeletedVolumes(std::move(deletedVolumes))
            , LiveVolumes(std::move(liveVolumes))
        {}
    };

    //
    // AddTags
    //

    struct TAddTagsRequest
    {
        const TString DiskId;
        const TVector<TString> Tags;

        TAddTagsRequest() = default;

        TAddTagsRequest(
                TString diskId,
                TVector<TString> tags)
            : DiskId(std::move(diskId))
            , Tags(std::move(tags))
        {}
    };

    struct TAddTagsResponse
    {};

    //
    // VolumeMountStateChanged
    //

    struct TVolumeMountStateChanged
    {
        const TString DiskId;
        const bool HasLocalMount = false;

        explicit TVolumeMountStateChanged(
                TString diskId,
                bool value)
            : DiskId(std::move(diskId))
            , HasLocalMount(value)
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::SERVICE_START,

        EvPingRequest = EvBegin + 1,
        EvPingResponse = EvBegin + 2,

        EvCreateVolumeRequest = EvBegin + 3,
        EvCreateVolumeResponse = EvBegin + 4,

        EvDestroyVolumeRequest = EvBegin + 5,
        EvDestroyVolumeResponse = EvBegin + 6,

        EvResizeVolumeRequest = EvBegin + 7,
        EvResizeVolumeResponse = EvBegin + 8,

        EvStatVolumeRequest = EvBegin + 9,
        EvStatVolumeResponse = EvBegin + 10,

        EvAssignVolumeRequest = EvBegin + 11,
        EvAssignVolumeResponse = EvBegin + 12,

        EvMountVolumeRequest = EvBegin + 13,
        EvMountVolumeResponse = EvBegin + 14,

        EvUnmountVolumeRequest = EvBegin + 15,
        EvUnmountVolumeResponse = EvBegin + 16,

        EvReadBlocksRequest = EvBegin + 17,
        EvReadBlocksResponse = EvBegin + 18,

        EvWriteBlocksRequest = EvBegin + 19,
        EvWriteBlocksResponse = EvBegin + 20,

        EvZeroBlocksRequest = EvBegin + 21,
        EvZeroBlocksResponse = EvBegin + 22,

        EvCreateCheckpointRequest = EvBegin + 23,
        EvCreateCheckpointResponse = EvBegin + 24,

        EvDeleteCheckpointRequest = EvBegin + 25,
        EvDeleteCheckpointResponse = EvBegin + 26,

        EvAlterVolumeRequest = EvBegin + 27,
        EvAlterVolumeResponse = EvBegin + 28,

        EvGetChangedBlocksRequest = EvBegin + 29,
        EvGetChangedBlocksResponse = EvBegin + 30,

        EvDescribeVolumeRequest = EvBegin + 31,
        EvDescribeVolumeResponse = EvBegin + 32,

        EvListVolumesRequest = EvBegin + 33,
        EvListVolumesResponse = EvBegin + 34,

        EvUploadClientMetricsRequest = EvBegin + 35,
        EvUploadClientMetricsResponse = EvBegin + 36,

        EvDiscoverInstancesRequest = EvBegin + 37,
        EvDiscoverInstancesResponse = EvBegin + 38,

        EvUpdateVolumeThrottlingConfigRequest = EvBegin + 39,
        EvUpdateVolumeThrottlingConfigResponse = EvBegin + 40,

        EvListDiskStatesRequest = EvBegin + 41,
        EvListDiskStatesResponse = EvBegin + 42,

        EvExecuteActionRequest = EvBegin + 43,
        EvExecuteActionResponse = EvBegin + 44,

        EvDescribeVolumeModelRequest = EvBegin + 45,
        EvDescribeVolumeModelResponse = EvBegin + 46,

        EvReadBlocksLocalRequest = EvBegin + 47,
        EvReadBlocksLocalResponse = EvBegin + 48,

        EvWriteBlocksLocalRequest = EvBegin + 49,
        EvWriteBlocksLocalResponse = EvBegin + 50,

        EvUpdateDiskRegistryConfigRequest = EvBegin + 53,
        EvUpdateDiskRegistryConfigResponse = EvBegin + 54,

        EvDescribeDiskRegistryConfigRequest = EvBegin + 55,
        EvDescribeDiskRegistryConfigResponse = EvBegin + 56,

        EvChangeVolumeBindingRequest = EvBegin + 57,
        EvChangeVolumeBindingResponse = EvBegin + 58,

        EvGetVolumeStatsRequest = EvBegin + 59,
        EvGetVolumeStatsResponse = EvBegin + 60,

        EvCreatePlacementGroupRequest = EvBegin + 61,
        EvCreatePlacementGroupResponse = EvBegin + 62,

        EvDestroyPlacementGroupRequest = EvBegin + 63,
        EvDestroyPlacementGroupResponse = EvBegin + 64,

        EvAlterPlacementGroupMembershipRequest = EvBegin + 65,
        EvAlterPlacementGroupMembershipResponse = EvBegin + 66,

        EvListPlacementGroupsRequest = EvBegin + 67,
        EvListPlacementGroupsResponse = EvBegin + 68,

        EvDescribePlacementGroupRequest = EvBegin + 69,
        EvDescribePlacementGroupResponse = EvBegin + 70,

        EvRemoveVolumeClientRequest = EvBegin + 71,
        EvRemoveVolumeClientResponse = EvBegin + 72,

        EvCmsActionRequest = EvBegin + 73,
        EvCmsActionResponse = EvBegin + 74,

        EvRegisterVolume = EvBegin + 75,
        EvUnregisterVolume = EvBegin + 76,
        EvVolumeConfigUpdated = EvBegin + 77,

        EvQueryAvailableStorageRequest = EvBegin + 78,
        EvQueryAvailableStorageResponse = EvBegin + 79,

        EvCreateVolumeFromDeviceRequest = EvBegin + 80,
        EvCreateVolumeFromDeviceResponse = EvBegin + 81,

        EvResumeDeviceRequest = EvBegin + 82,
        EvResumeDeviceResponse = EvBegin + 83,

        EvRunVolumesLivenessCheckRequest = EvBegin + 84,
        EvRunVolumesLivenessCheckResponse = EvBegin + 85,

        EvGetCheckpointStatusRequest = EvBegin + 86,
        EvGetCheckpointStatusResponse = EvBegin + 87,

        EvVolumeMountStateChanged = EvBegin + 88,

        EvQueryAgentsInfoRequest = EvBegin + 89,
        EvQueryAgentsInfoResponse = EvBegin + 90,

        EvAddTagsRequest = EvBegin + 91,
        EvAddTagsResponse = EvBegin + 92,

        EvCreateVolumeLinkRequest = EvBegin + 95,
        EvCreateVolumeLinkResponse = EvBegin + 96,

        EvDestroyVolumeLinkRequest = EvBegin + 97,
        EvDestroyVolumeLinkResponse = EvBegin + 98,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::SERVICE_END,
        "EvEnd expected to be < TBlockStoreEvents::SERVICE_END");

    BLOCKSTORE_STORAGE_SERVICE(BLOCKSTORE_DECLARE_PROTO_EVENTS)
    BLOCKSTORE_SERVICE_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)

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

    using TEvVolumeMountStateChanged = TRequestEvent<
        TVolumeMountStateChanged,
        EvVolumeMountStateChanged
    >;
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeStorageServiceId();

}   // namespace NCloud::NBlockStore::NStorage
