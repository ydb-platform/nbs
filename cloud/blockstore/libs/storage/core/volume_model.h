#pragma once

#include "public.h"

// TODO: move enum definitions to a separate proto file to avoid the inclusion
// of big headers in other headers
#include <cloud/blockstore/libs/storage/model/channel_data_kind.h>
#include <cloud/blockstore/private/api/protos/volume.pb.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <util/generic/vector.h>

namespace NKikimrBlockStore {
    class TVolumeConfig;
}   // NKikimrBlockStore

namespace NCloud::NBlockStore::NProto {
    class TVolumePerformanceProfile;
    class TResizeVolumeRequestFlags;
}   // namespace NCloud::NBlockStore::NProto

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TChannelInfo
{
    TString PoolKind;
    EChannelDataKind DataKind = EChannelDataKind::Max;
};

struct TVolumeParams
{
    ui32 BlockSize = 0;
    ui64 BlocksCountPerPartition = 0;
    ui32 PartitionsCount = 0;
    TVector<TChannelInfo> DataChannels;
    ui64 MaxReadBandwidth = 0;
    ui64 MaxWriteBandwidth = 0;
    ui32 MaxReadIops = 0;
    ui32 MaxWriteIops = 0;
    NCloud::NProto::EStorageMediaKind MediaKind
        = NCloud::NProto::STORAGE_MEDIA_DEFAULT;
    NPrivateProto::TVolumeChannelsToPoolsKinds VolumeChannelsToPoolsKinds = {};

    ui64 GetBlocksCount() const
    {
        return BlocksCountPerPartition * PartitionsCount;
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TPartitionsInfo
{
    ui64 BlocksCountPerPartition = 0;
    ui32 PartitionsCount = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TCreateVolumeParamsCtx
{
    ui32 BlockSize;
    ui64 BlocksCount;
    NCloud::NProto::EStorageMediaKind MediaKind;
    ui32 PartitionsCount;
    TString CloudId;
    TString FolderId;
    TString DiskId;
    bool IsSystem;
    bool IsOverlayDisk;
};

ui64 ComputeBlocksCountPerPartition(
    const ui64 newBlocksCountPerVolume,
    const ui32 blocksPerStripe,
    const ui32 partitionsCount);

ui64 ComputeBlocksCountPerPartition(
    const ui64 newBlocksCountPerVolume,
    const ui32 bytesPerStripe,
    const ui32 blockSize,
    const ui32 partitionsCount);

TPartitionsInfo ComputePartitionsInfo(
    const TStorageConfig& config,
    const TString& cloudId,
    const TString& folderId,
    const TString& diskId,
    NCloud::NProto::EStorageMediaKind mediaKind,
    ui64 blocksCount,
    ui32 blockSize,
    bool isSystem,
    bool isOverlayDisk);

void ResizeVolume(
    const TStorageConfig& config,
    const TVolumeParams& volumeParams,
    const NProto::TResizeVolumeRequestFlags& flags,
    const NProto::TVolumePerformanceProfile& profile,
    NKikimrBlockStore::TVolumeConfig& volumeConfig);

bool SetMissingParams(
    const TVolumeParams& volumeParams,
    const NKikimrBlockStore::TVolumeConfig& prevConfig,
    NKikimrBlockStore::TVolumeConfig& update);

ui64 ComputeMaxBlocks(
    const TStorageConfig& config,
    const NCloud::NProto::EStorageMediaKind mediaKind,
    ui32 currentPartitions);

void ComputeChannelCountLimits(
    int freeChannelCount,
    int* wantToAddMerged,
    int* wantToAddMixed,
    int* wantToAddFresh);

TVolumeParams CreateVolumeParams(
    const TStorageConfig& config,
    const TCreateVolumeParamsCtx& ctx);

}   // namespace NCloud::NBlockStore::NStorage
