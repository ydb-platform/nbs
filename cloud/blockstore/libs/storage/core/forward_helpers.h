#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/storage/core/libs/common/compressed_bitmap.h>

#include <util/generic/hash_set.h>

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
constexpr bool IsReadOrWriteMethod =
    IsReadMethod<TMethod> || IsWriteMethod<TMethod>;

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

void ApplyMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksRequest& request);
void ApplyMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksResponse& response);
void ApplyMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksLocalRequest& request);

////////////////////////////////////////////////////////////////////////////////

void FillUnusedIndices(
    const NProto::TReadBlocksRequest& request,
    const TCompressedBitmap* usedBlocks,
    THashSet<ui64>* unusedIndices);
void FillUnusedIndices(
    const NProto::TReadBlocksLocalRequest& request,
    const TCompressedBitmap* usedBlocks,
    THashSet<ui64>* unusedIndices);

////////////////////////////////////////////////////////////////////////////////

void FillUnencryptedBlockMask(
    const THashSet<ui64>& unusedIndices,
    const ui64 startIndex,
    NProto::TReadBlocksResponse& response);

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
