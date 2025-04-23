#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/partition_common/model/blob_markers.h>
#include <cloud/storage/core/libs/common/compressed_bitmap.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool IsReadMethod =
    std::is_same_v<TMethod, TEvService::TReadBlocksMethod> ||
    std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>;

template <typename TMethod>
constexpr bool IsWriteMethod =
    std::is_same_v<TMethod, TEvService::TWriteBlocksMethod> ||
    std::is_same_v<TMethod, TEvService::TWriteBlocksLocalMethod> ||
    std::is_same_v<TMethod, TEvService::TZeroBlocksMethod>;

template <typename TMethod>
constexpr bool IsExactlyWriteMethod =
    std::is_same_v<TMethod, TEvService::TWriteBlocksMethod> ||
    std::is_same_v<TMethod, TEvService::TWriteBlocksLocalMethod>;

template <typename TMethod>
constexpr bool IsZeroMethod =
    std::is_same_v<TMethod, TEvService::TZeroBlocksMethod>;

template <typename TMethod>
constexpr bool IsReadOrWriteMethod =
    IsReadMethod<TMethod> || IsWriteMethod<TMethod>;

template <typename T>
constexpr bool IsDescribeBlocksMethod =
    std::is_same_v<T, TEvVolume::TDescribeBlocksMethod>;

template <typename TMethod>
constexpr bool IsCheckpointMethod =
    std::is_same_v<TMethod, TEvService::TCreateCheckpointMethod> ||
    std::is_same_v<TMethod, TEvService::TDeleteCheckpointMethod> ||
    std::is_same_v<TMethod, TEvVolume::TDeleteCheckpointDataMethod> ||
    std::is_same_v<TMethod, TEvService::TGetCheckpointStatusMethod>;

template <typename TMethod>
constexpr bool IsLocalMethod =
    std::is_same_v<TMethod, TEvService::TWriteBlocksLocalMethod> ||
    std::is_same_v<TMethod, TEvService::TReadBlocksLocalMethod>;

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool RequiresMount = IsReadOrWriteMethod<TMethod>;

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool RequiresReadWriteAccess = IsWriteMethod<TMethod>;

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool RequiresThrottling =
    IsReadOrWriteMethod<TMethod> ||
    std::is_same_v<TMethod, TEvVolume::TDescribeBlocksMethod>;

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool RejectRequestIfNotReady =
    !(std::is_same_v<TMethod, TEvService::TCreateCheckpointMethod> ||
      std::is_same_v<TMethod, TEvService::TDeleteCheckpointMethod> ||
      std::is_same_v<TMethod, TEvVolume::TDeleteCheckpointDataMethod> ||
      std::is_same_v<TMethod, TEvVolume::TDescribeBlocksMethod> ||
      std::is_same_v<TMethod, TEvVolume::TGetPartitionInfoMethod> ||
      std::is_same_v<TMethod, TEvVolume::TCompactRangeMethod> ||
      std::is_same_v<TMethod, TEvVolume::TGetCompactionStatusMethod>);

////////////////////////////////////////////////////////////////////////////////

template<class TMethod>
concept ReadRequest = IsReadMethod<TMethod>;

////////////////////////////////////////////////////////////////////////////////

template<class TMethod>
concept WriteRequest = IsWriteMethod<TMethod>;

////////////////////////////////////////////////////////////////////////////////

void ClearEmptyBlocks(
    const NBlobMarkers::TBlockMarks& usedBlocks,
    NProto::TReadBlocksResponse& response);
void ClearEmptyBlocks(
    const NBlobMarkers::TBlockMarks& usedBlocks,
    const TGuardedSgList& sglist);

////////////////////////////////////////////////////////////////////////////////

NBlobMarkers::TBlockMarks MakeUsedBlockMarks(
    const TCompressedBitmap& usedBlocks,
    TBlockRange64 range);

////////////////////////////////////////////////////////////////////////////////

inline TGuardedSgList GetSglist(const NProto::TReadBlocksLocalRequest& request)
{
    return request.Sglist;
}

inline TGuardedSgList GetSglist(const NProto::TWriteBlocksLocalRequest& request)
{
    return request.Sglist;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TEvent>
void ForwardMessageToActor(
    const TEvent& ev,
    const NActors::TActorContext& ctx,
    NActors::TActorId dstActor)
{
    NActors::TActorId nondeliveryActor = ev->GetForwardOnNondeliveryRecipient();
    auto message = std::make_unique<NActors::IEventHandle>(
        dstActor,
        ev->Sender,
        ev->ReleaseBase().Release(),
        ev->Flags,
        ev->Cookie,
        ev->Flags & NActors::IEventHandle::FlagForwardOnNondelivery
            ? &nondeliveryActor
            : nullptr);
    ctx.Send(std::move(message));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
