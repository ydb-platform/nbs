#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>

#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/hash_set.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool RequiresMount()
{
    return false;
}

template <>
constexpr bool RequiresMount<TEvService::TReadBlocksMethod>()
{
    return true;
}

template <>
constexpr bool RequiresMount<TEvService::TWriteBlocksMethod>()
{
    return true;
}

template <>
constexpr bool RequiresMount<TEvService::TZeroBlocksMethod>()
{
    return true;
}

template <>
constexpr bool RequiresMount<TEvService::TReadBlocksLocalMethod>()
{
    return true;
}

template <>
constexpr bool RequiresMount<TEvService::TWriteBlocksLocalMethod>()
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool RequiresReadWriteAccess()
{
    return false;
}

template <>
constexpr bool RequiresReadWriteAccess<TEvService::TWriteBlocksMethod>()
{
    return true;
}

template <>
constexpr bool RequiresReadWriteAccess<TEvService::TZeroBlocksMethod>()
{
    return true;
}

template <>
constexpr bool RequiresReadWriteAccess<TEvService::TWriteBlocksLocalMethod>()
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool RequiresThrottling()
{
    return false;
}

template <>
constexpr bool RequiresThrottling<TEvService::TReadBlocksMethod>()
{
    return true;
}

template <>
constexpr bool RequiresThrottling<TEvService::TWriteBlocksMethod>()
{
    return true;
}

template <>
constexpr bool RequiresThrottling<TEvService::TZeroBlocksMethod>()
{
    return true;
}

template <>
constexpr bool RequiresThrottling<TEvService::TReadBlocksLocalMethod>()
{
    return true;
}

template <>
constexpr bool RequiresThrottling<TEvService::TWriteBlocksLocalMethod>()
{
    return true;
}

template <>
constexpr bool RequiresThrottling<TEvVolume::TDescribeBlocksMethod>()
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool IsReadRequest()
{
    return false;
}

template <>
constexpr bool IsReadRequest<TEvService::TReadBlocksMethod>()
{
    return true;
}

template <>
constexpr bool IsReadRequest<TEvService::TReadBlocksLocalMethod>()
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool IsWriteRequest()
{
    return false;
}

template <>
constexpr bool IsWriteRequest<TEvService::TWriteBlocksMethod>()
{
    return true;
}

template <>
constexpr bool IsWriteRequest<TEvService::TWriteBlocksLocalMethod>()
{
    return true;
}

template <>
constexpr bool IsWriteRequest<TEvService::TZeroBlocksMethod>()
{
    return true;
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool IsReadWriteRequest()
{
    return IsReadRequest<TMethod>() || IsWriteRequest<TMethod>();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
constexpr bool RejectRequestIfNotReady()
{
    return true;
}

template <>
constexpr bool RejectRequestIfNotReady<TEvService::TCreateCheckpointMethod>()
{
    return false;
}

template <>
constexpr bool RejectRequestIfNotReady<TEvService::TDeleteCheckpointMethod>()
{
    return false;
}

template <>
constexpr bool RejectRequestIfNotReady<TEvVolume::TDeleteCheckpointDataMethod>()
{
    return false;
}

template <>
constexpr bool RejectRequestIfNotReady<TEvVolume::TDescribeBlocksMethod>()
{
    return false;
}

template <>
constexpr bool RejectRequestIfNotReady<TEvVolume::TGetPartitionInfoMethod>()
{
    return false;
}

template <>
constexpr bool RejectRequestIfNotReady<TEvVolume::TCompactRangeMethod>()
{
    return false;
}

template <>
constexpr bool RejectRequestIfNotReady<TEvVolume::TGetCompactionStatusMethod>()
{
    return false;
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void FillUnusedIndices(
    const T&,
    const TCompressedBitmap*,
    THashSet<ui64>*)
{
}

inline void FillUnusedIndices(
    const NProto::TReadBlocksRequest& request,
    const TCompressedBitmap* usedBlocks,
    THashSet<ui64>* unusedIndices)
{
    if (!usedBlocks) {
        for (ui64 i = 0; i < request.GetBlocksCount(); ++i) {
            unusedIndices->insert(request.GetStartIndex() + i);
        }
        return;
    }

    const auto b = request.GetStartIndex();
    const auto e = request.GetStartIndex() + request.GetBlocksCount();
    if (usedBlocks->Count(b, e) < e - b) {
        for (ui64 i = 0; i < request.GetBlocksCount(); ++i) {
            ui64 b = request.GetStartIndex() + i;
            if (!usedBlocks->Test(b)) {
                unusedIndices->insert(b);
            }
        }
    }
}

inline void FillUnusedIndices(
    const NProto::TReadBlocksLocalRequest& request,
    const TCompressedBitmap* usedBlocks,
    THashSet<ui64>* unusedIndices)
{
    FillUnusedIndices(
        static_cast<const NProto::TReadBlocksRequest&>(request),
        usedBlocks,
        unusedIndices);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void ApplyMask(const THashSet<ui64>&, const ui64, T&)
{
}

inline void ApplyMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksResponse& response)
{
    auto& buffers = *response.MutableBlocks()->MutableBuffers();
    for (int i = 0; i < buffers.size(); ++i) {
        ui64 b = startIndex + i;
        if (buffers[i].size() && unusedIndices.contains(b)) {
            memset(buffers[i].begin(), 0, buffers[i].size());
        }
    }
}

inline void ApplyMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksLocalRequest& request)
{
    auto& sglist = request.Sglist;
    auto guard = sglist.Acquire();
    if (!guard) {
        return;
    }

    auto& blockDatas = guard.Get();

    for (ui32 i = 0; i < blockDatas.size(); ++i) {
        auto& buffer = blockDatas[i];
        ui64 b = startIndex + i;
        if (buffer.Size() && unusedIndices.contains(b)) {
            memset(
                const_cast<char*>(buffer.Data()),
                0,
                buffer.Size()
            );
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void FillUnencryptedBlockMask(const THashSet<ui64>&, const ui64, T&)
{
}

inline void FillUnencryptedBlockMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksResponse& response)
{
    if (unusedIndices.empty()) {
        return;
    }

    TDynBitMap bitmap;
    for (auto unusedIndex: unusedIndices) {
        Y_VERIFY(unusedIndex >= startIndex);
        bitmap.Set(unusedIndex - startIndex);
    }

    auto& blockMask = *response.MutableUnencryptedBlockMask();
    blockMask.assign(TStringBuf{
        reinterpret_cast<const char*>(bitmap.GetChunks()),
        bitmap.Size() / 8
    });
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
TGuardedSgList GetSglist(const T&)
{
    return {};
}

inline TGuardedSgList GetSglist(const NProto::TReadBlocksLocalRequest& request)
{
    return request.Sglist;
}

inline TGuardedSgList GetSglist(const NProto::TWriteBlocksLocalRequest& request)
{
    return request.Sglist;
}

}   // namespace NCloud::NBlockStore::NStorage
