#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/volume/actors/forward_read_marked.h>
#include <cloud/blockstore/libs/storage/volume/actors/forward_write_and_mark_used.h>
#include <cloud/blockstore/libs/storage/volume/actors/read_disk_registry_based_overlay.h>

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/forward_helpers.h>

#include <cloud/storage/core/libs/common/media.h>

#include <cloud/blockstore/libs/storage/core/probes.h>

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

    const auto& volumeConfig = State->GetMeta().GetVolumeConfig();
    const bool encryptedDiskRegistryBasedDisk =
        State->IsDiskRegistryMediaKind() &&
        volumeConfig.GetEncryptionDesc().GetMode() != NProto::NO_ENCRYPTION;
    const bool overlayDiskRegistryBasedDisk =
        State->IsDiskRegistryMediaKind() &&
        !State->GetBaseDiskId().Empty();

    if constexpr (IsWriteMethod<TMethod>) {
        if (State->GetTrackUsedBlocks() ||
            State->HasCheckpointLight() ||
            overlayDiskRegistryBasedDisk)
        {
            auto requestInfo =
                CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

            NCloud::Register<TWriteAndMarkUsedActor<TMethod>>(
                ctx,
                std::move(requestInfo),
                std::move(msg->Record),
                State->GetBlockSize(),
                encryptedDiskRegistryBasedDisk || overlayDiskRegistryBasedDisk,
                volumeRequestId,
                partActorId,
                TabletID(),
                SelfId());

            return true;
        }
    }

    if constexpr (IsReadMethod<TMethod>) {
        if (State->GetMaskUnusedBlocks() && State->GetUsedBlocks() ||
            encryptedDiskRegistryBasedDisk ||
            overlayDiskRegistryBasedDisk)
        {
            const TCompressedBitmap* usedBlocks = State->GetUsedBlocks();
            const bool isOnlyOverlayDisk = usedBlocks
                ? usedBlocks->Count(
                    msg->Record.GetStartIndex(),
                    msg->Record.GetStartIndex() + msg->Record.GetBlocksCount())
                        == msg->Record.GetBlocksCount()
                : false;

            if (isOnlyOverlayDisk) {
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
                    encryptedDiskRegistryBasedDisk,
                    GetDowntimeThreshold(
                        *DiagnosticsConfig,
                        NProto::STORAGE_MEDIA_SSD));

                return true;
            }

            NCloud::Register<TReadMarkedActor<TMethod>>(
                ctx,
                CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
                std::move(msg->Record),
                State->GetUsedBlocks(),
                State->GetMaskUnusedBlocks(),
                encryptedDiskRegistryBasedDisk,
                partActorId,
                TabletID(),
                SelfId());

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

    const auto* msg = ev->Get();

    const auto highCheckpointType =
        State->GetCheckpointStore().GetCheckpointType(
            msg->Record.GetHighCheckpointId());
    const auto lowCheckpointType =
        State->GetCheckpointStore().GetCheckpointType(
            msg->Record.GetLowCheckpointId());

    if (highCheckpointType && *highCheckpointType == ECheckpointType::Light ||
        lowCheckpointType && *lowCheckpointType == ECheckpointType::Light)
    {
        GetChangedBlocksForLightCheckpoints(ev, ctx);
        return true;
    }

    // TODO: NBS-3228: remove checks for disk registry based disks after normal
    // checkpoints for disk registry based disks are implemented completely.
    if (!State->IsDiskRegistryMediaKind()) {
        return false;
    }

    if (msg->Record.GetHighCheckpointId() == "" &&
        msg->Record.GetLowCheckpointId() == "")
    {
        GetChangedBlocksForLightCheckpoints(ev, ctx);
        return true;
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

#undef GENERATE_IMPL

}   // namespace NCloud::NBlockStore::NStorage
