#pragma once

#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>

#include <cloud/blockstore/libs/storage/partition_common/actor_read_blob.h>
#include <cloud/blockstore/libs/storage/partition_common/actor_describe_base_disk_blocks.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/events.h>

#include <contrib/ydb/core/base/services/blobstorage_service_id.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

/**
 * \brief Class for reading data from a base and overlay disk at the same time
 * \details The incoming request is split into two TReadBlocksRequest requests
 *          - for base and overlay disk.
 *          Empty blocks from base disk are filled with zeros
 */
template <ReadRequest TMethod>
class TReadDiskRegistryBasedOverlayActor final
    : public NActors::TActorBootstrapped<TReadDiskRegistryBasedOverlayActor<TMethod>>
{
private:
    using TActorId = NActors::TActorId;
    using TActorContext = NActors::TActorContext;

    using TRequest = typename TMethod::TRequest::ProtoRecordType;
    using TBase = NActors::TActorBootstrapped<TReadDiskRegistryBasedOverlayActor<TMethod>>;

    const TRequestInfoPtr RequestInfo;
    const TRequest OriginalRequest;
    const TActorId VolumeActorId;
    const TActorId PartActorId;
    const ui64 VolumeTabletId;
    const TString BaseDiskId;
    const TString BaseDiskCheckpointId;
    const ui32 BlockSize;
    const TDuration LongRunningThreshold;
    TChildLogTitle LogTitle;
    const bool EnableChecksumValidation;
    const EStorageAccessMode Mode;

    // Initially, the block map is built on the basis of the usedBlocks of the
    // overlay disk.
    // Then we move it to TDescribeBaseDiskBlocksActor, which will
    // specify information about empty blocks that belong to the base disk.
    // The updated map will be returned in the TEvDescribeBlocksCompleted.
    NBlobMarkers::TBlockMarks BlockMarks;

    IReadBlocksHandlerPtr ReadHandler;
    NProto::TReadBlocksLocalRequest OverlayDiskRequest;
    NProto::TReadBlocksLocalRequest BaseDiskRequest;

    size_t RequestsInFlight = 0;

public:
    TReadDiskRegistryBasedOverlayActor(
        TRequestInfoPtr requestInfo,
        TRequest originalRequest,
        const TCompressedBitmap& usedBlocks,
        TActorId volumeActorId,
        TActorId partActorId,
        ui64 volumeTabletId,
        TString baseDiskId,
        TString baseDiskCheckpointId,
        ui32 blockSize,
        EStorageAccessMode mode,
        TDuration longRunningThreshold,
        TChildLogTitle logTitle,
        bool enableChecksumValidation);

    void Bootstrap(const TActorContext& ctx);

private:
    /**
     * \brief Initialization of requests to base and overlay disks
     * \details Requests are formed according to the following rules:
     *          Incoming request:        |--------------------------------|
     *          Data on base disk:       |11111---------1111---11---------|
     *          Data on overlay disk:    |--222222222---222222-11--2222222|
     *          Request to base disk:    |-------------------------|
     *          Request to overlay disk:   |------------------------------|
     *          Result:                  |11222222222000222222011002222222|
     */
    bool InitRequests();
    bool InitRequest(
        const TVector<ui64>& blockIndices,
        bool isBaseDisk,
        NProto::TReadBlocksLocalRequest& request);
    void SendOverlayDiskRequest(const TActorContext& ctx);
    void SendBaseDiskRequest(const TActorContext& ctx);

    void ReadBlocks(const TActorContext& ctx);
    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandleReadBlocksLocalResponse(
        const TEvService::TEvReadBlocksLocalResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeBlocksCompleted(
        const TEvPartitionCommonPrivate::TEvDescribeBlocksCompleted::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadBlobResponse(
        const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleLongRunningBlobOperation(
        const TEvPartitionCommonPrivate::TEvLongRunningOperation::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const NActors::TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
