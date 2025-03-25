#include "volume_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/volume/actors/forward_write_and_mark_used.h>
#include <cloud/blockstore/libs/storage/volume/actors/read_and_clear_empty_blocks_actor.h>
#include <cloud/blockstore/libs/storage/volume/actors/read_disk_registry_based_overlay.h>
#include <cloud/storage/core/libs/common/media.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
bool TVolumeActor::SendRequestToPartitionWithUsedBlockTracking(
    const TActorContext& ctx,
    const typename TMethod::TRequest::TPtr& ev,
    const TActorId& partActorId,
    const ui64 volumeRequestId)
{
    static_assert(IsReadMethod<TMethod> || IsWriteMethod<TMethod>);

    const auto* msg = ev->Get();

    const bool overlayDiskRegistryBasedDisk =
        State->IsDiskRegistryMediaKind() && !State->GetBaseDiskId().empty();

    if constexpr (IsWriteMethod<TMethod>) {
        if (State->GetTrackUsedBlocks() || State->HasCheckpointLight())
        {
            auto requestInfo =
                CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
            // TODO(drbasic)
            // For encrypted disk-registry based disks, we will continue to
            // write a map of encrypted blocks for a while.
            const bool encryptedDiskRegistryBasedDisk =
                State->IsDiskRegistryMediaKind() &&
                State->GetMeta()
                        .GetVolumeConfig()
                        .GetEncryptionDesc()
                        .GetMode() != NProto::NO_ENCRYPTION;

            // For overlay disks we need to ensure that the only bits that are
            // set in the used block map are the bits that correspond to the
            // blocks that have been successfully written. Therefore we must
            // update the map only after the block has been successfully
            // written.
            const bool needReliableUsedBlockTracking =
                encryptedDiskRegistryBasedDisk || overlayDiskRegistryBasedDisk;
            NCloud::Register<TWriteAndMarkUsedActor<TMethod>>(
                ctx,
                std::move(requestInfo),
                std::move(msg->Record),
                State->GetBlockSize(),
                needReliableUsedBlockTracking,
                volumeRequestId,
                partActorId,
                TabletID(),
                SelfId());

            return true;
        }
    }

    if constexpr (IsReadMethod<TMethod>) {
        if (State->GetTrackUsedBlocks()) {
            const bool needToZeroUnusedBlocks =
                State->GetMaskUnusedBlocks() || overlayDiskRegistryBasedDisk;
            if (!needToZeroUnusedBlocks) {
                // We don't need to mask unused blocks. Therefore, we can do
                // the usual reading.
                return false;
            }

            const bool readUsedBlocksOnly =
                State->GetUsedBlocks().Count(
                    msg->Record.GetStartIndex(),
                    msg->Record.GetStartIndex() +
                        msg->Record.GetBlocksCount()) ==
                msg->Record.GetBlocksCount();

            if (readUsedBlocksOnly) {
                // We don't need to look at the map of used blocks when
                // reading, because all the blocks have been used.
                // Therefore, we don't need to zero unused blocks and can do the
                // usual reading.
                return false;
            }

            if (overlayDiskRegistryBasedDisk) {
                NCloud::Register<TReadDiskRegistryBasedOverlayActor<TMethod>>(
                    ctx,
                    CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
                    std::move(msg->Record),
                    State->GetUsedBlocks(),
                    SelfId(),
                    partActorId,
                    TabletID(),
                    State->GetBaseDiskId(),
                    State->GetBaseDiskCheckpointId(),
                    State->GetBlockSize(),
                    State->GetStorageAccessMode(),
                    GetDowntimeThreshold(
                        *DiagnosticsConfig,
                        NProto::STORAGE_MEDIA_SSD));
            } else {
                NCloud::Register<TReadAndClearEmptyBlocksActor<TMethod>>(
                    ctx,
                    CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
                    std::move(msg->Record),
                    State->GetUsedBlocks(),
                    partActorId,
                    TabletID(),
                    SelfId());
            }

            return true;
        }
    }
    return false;
}

template <>
bool TVolumeActor::SendRequestToPartitionWithUsedBlockTracking<
    TEvService::TGetChangedBlocksMethod>(
    const TActorContext& ctx,
    const TEvService::TGetChangedBlocksMethod::TRequest::TPtr& ev,
    const TActorId& partActorId,
    const ui64 volumeRequestId)
{
    Y_UNUSED(partActorId);
    Y_UNUSED(volumeRequestId);

    if (!State->IsDiskRegistryMediaKind()) {
        // For BlobStorage-based disks, we return false to process the request
        // in the partition actor.
        return false;
    }

    const auto* msg = ev->Get();

    const auto highCheckpoint = State->GetCheckpointStore().GetCheckpoint(
        msg->Record.GetHighCheckpointId());
    const auto lowCheckpoint = State->GetCheckpointStore().GetCheckpoint(
        msg->Record.GetLowCheckpointId());

    if ((msg->Record.GetHighCheckpointId().empty() &&
         msg->Record.GetLowCheckpointId().empty()) ||
        highCheckpoint && highCheckpoint->Type == ECheckpointType::Light ||
        lowCheckpoint && lowCheckpoint->Type == ECheckpointType::Light)
    {
        GetChangedBlocksForLightCheckpoints(ev, ctx);
        return true;
    }

    // Expect that the name of the checkpoint will be passed through the
    // HighCheckpointId parameter.
    if (highCheckpoint && highCheckpoint->IsShadowDiskBased()) {
        // We need to forward the message to the shadow disk actor. Therefore,
        // we simply return false.
        return false;
    }

    ReplyErrorOnNormalGetChangedBlocksRequestForDiskRegistryBasedDisk(ev, ctx);
    return true;
}

////////////////////////////////////////////////////////////////////////////////

#define GENERATE_IMPL(name, ns)                                                \
template bool TVolumeActor::SendRequestToPartitionWithUsedBlockTracking<       \
    ns::T##name##Method>(                                                      \
        const TActorContext& ctx,                                              \
        const ns::TEv##name##Request::TPtr& ev,                                \
        const TActorId& partActorId,                                           \
        const ui64 volumeRequestId);                                           \
// GENERATE_IMPL

#define GENERATE_NO_IMPL(name, ns)                                           \
    template <>                                                              \
    bool TVolumeActor::SendRequestToPartitionWithUsedBlockTracking<          \
        ns::T##name##Method>(                                                \
        const TActorContext& ctx,                                            \
        const ns::T##name##Method::TRequest::TPtr& ev,                       \
        const TActorId& partActorId,                                         \
        const ui64 volumeRequestId)                                          \
    {                                                                        \
        Y_UNUSED(ctx);                                                       \
        Y_UNUSED(ev);                                                        \
        Y_UNUSED(partActorId);                                               \
        Y_UNUSED(volumeRequestId);                                           \
        return false;                                                        \
    }                                                                        \
// GENERATE_NO_IMPL

GENERATE_IMPL(ReadBlocks,         TEvService)
GENERATE_IMPL(WriteBlocks,        TEvService)
GENERATE_IMPL(ZeroBlocks,         TEvService)
GENERATE_IMPL(ReadBlocksLocal,    TEvService)
GENERATE_IMPL(WriteBlocksLocal,   TEvService)

GENERATE_NO_IMPL(CreateCheckpoint,    TEvService)
GENERATE_NO_IMPL(DeleteCheckpoint,    TEvService)
GENERATE_NO_IMPL(GetCheckpointStatus, TEvService)

GENERATE_NO_IMPL(DescribeBlocks,           TEvVolume)
GENERATE_NO_IMPL(GetUsedBlocks,            TEvVolume)
GENERATE_NO_IMPL(GetPartitionInfo,         TEvVolume)
GENERATE_NO_IMPL(CompactRange,             TEvVolume)
GENERATE_NO_IMPL(GetCompactionStatus,      TEvVolume)
GENERATE_NO_IMPL(DeleteCheckpointData,     TEvVolume)
GENERATE_NO_IMPL(RebuildMetadata,          TEvVolume)
GENERATE_NO_IMPL(GetRebuildMetadataStatus, TEvVolume)
GENERATE_NO_IMPL(ScanDisk,                 TEvVolume)
GENERATE_NO_IMPL(GetScanDiskStatus,        TEvVolume)
GENERATE_NO_IMPL(CheckRange,               TEvVolume)

#undef GENERATE_IMPL

}   // namespace NCloud::NBlockStore::NStorage
