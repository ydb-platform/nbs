#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/block_range.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <util/generic/string.h>
#include <util/generic/typetraits.h>

namespace NCloud::NBlockStore::NStorage {

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

Y_HAS_MEMBER(GetSessionId);

template <typename T, typename = void>
struct TGetSessionIdSelector;

template <typename T>
struct TGetSessionIdSelector<T, std::enable_if_t<THasGetSessionId<T>::value>>
{
    static TString Get(const T& args)
    {
        return args.GetSessionId();
    }
};

template <typename T>
struct TGetSessionIdSelector<T, std::enable_if_t<!THasGetSessionId<T>::value>>
{
    static TString Get(const T& args)
    {
        Y_UNUSED(args);
        return {};
    }
};

template <typename T>
using TGetSessionId = TGetSessionIdSelector<T>;

}   // NImpl

////////////////////////////////////////////////////////////////////////////////

template <typename TArgs, ui32 EventId>
TString GetIdempotenceId(const TRequestEvent<TArgs, EventId>& request)
{
    return request.Headers.GetIdempotenceId();
}

template <typename TArgs, ui32 EventId>
TString GetIdempotenceId(const TProtoRequestEvent<TArgs, EventId>& request)
{
    return request.Record.GetHeaders().GetIdempotenceId();
}

template <typename TArgs, ui32 EventId>
TString GetClientId(const TRequestEvent<TArgs, EventId>& request)
{
    return request.Headers.GetClientId();
}

template <typename TArgs, ui32 EventId>
TString GetClientId(const TProtoRequestEvent<TArgs, EventId>& request)
{
    return request.Record.GetHeaders().GetClientId();
}

template <typename TArgs, ui32 EventId>
TString GetDiskId(const TRequestEvent<TArgs, EventId>& request)
{
    return request.DiskId;
}

template <typename TArgs, ui32 EventId>
TString GetDiskId(const TProtoRequestEvent<TArgs, EventId>& request)
{
    return request.Record.GetDiskId();
}

template <typename TArgs, ui32 EventId>
TString GetSessionId(const TRequestEvent<TArgs, EventId>& request)
{
    return request.SessionId;
}

template <typename TArgs, ui32 EventId>
TString GetSessionId(const TProtoRequestEvent<TArgs, EventId>& request)
{
    return NImpl::TGetSessionId<TArgs>::Get(request.Record);
}

void VolumeConfigToVolume(
    const NKikimrBlockStore::TVolumeConfig& volumeConfig,
    NProto::TVolume& volume);

void VolumeConfigToVolumeModel(
    const NKikimrBlockStore::TVolumeConfig& volumeConfig,
    NProto::TVolumeModel& volumeModel);

void FillDeviceInfo(
    const google::protobuf::RepeatedPtrField<NProto::TDeviceConfig>& devices,
    const google::protobuf::RepeatedPtrField<NProto::TDeviceMigration>& migrations,
    const google::protobuf::RepeatedPtrField<NProto::TReplica>& replicas,
    const google::protobuf::RepeatedPtrField<TString>& freshDeviceIds,
    NProto::TVolume& volume);

template <typename TArgs, ui32 EventId>
ui32 GetRequestGeneration(const TProtoRequestEvent<TArgs, EventId>& request)
{
    return request.Record.GetHeaders().GetRequestGeneration();
}

template <typename TArgs, ui32 EventId>
ui32 GetRequestGeneration(const TRequestEvent<TArgs, EventId>& request)
{
    return request.Headers.GetRequestGeneration();
}

template <typename TArgs, ui32 EventId>
void SetRequestGeneration(
    ui32 generation,
    TProtoRequestEvent<TArgs, EventId>& request)
{
    request.Record.MutableHeaders()->SetRequestGeneration(generation);
}

template <typename TArgs, ui32 EventId>
void SetRequestGeneration(
    ui32 generation,
    TRequestEvent<TArgs, EventId>& request)
{
    request.Headers.SetRequestGeneration(generation);
}

bool GetThrottlingEnabled(
    const TStorageConfig& config,
    const NProto::TPartitionConfig& partitionConfig);

bool GetThrottlingEnabledZeroBlocks(
    const TStorageConfig& config,
    const NProto::TPartitionConfig& partitionConfig);

bool CompareVolumeConfigs(
    const NKikimrBlockStore::TVolumeConfig& prevConfig,
    const NKikimrBlockStore::TVolumeConfig& newConfig);

ui32 GetWriteBlobThreshold(
    const TStorageConfig& config,
    const NCloud::NProto::EStorageMediaKind mediaKind);

ui32 GetWriteMixedBlobThreshold(
    const TStorageConfig& config,
    const NCloud::NProto::EStorageMediaKind mediaKind);

inline bool RequiresCheckpointSupport(const NProto::TReadBlocksRequest& request)
{
    return !request.GetCheckpointId().empty();
}

inline bool RequiresCheckpointSupport(const NProto::TWriteBlocksRequest&)
{
    return false;
}

inline bool RequiresCheckpointSupport(const NProto::TZeroBlocksRequest&)
{
    return false;
}

inline bool RequiresCheckpointSupport(const NProto::TChecksumDeviceBlocksRequest&)
{
    return false;
}

inline NProto::EStorageMediaKind GetCheckpointShadowDiskType(
    NProto::EStorageMediaKind srcMediaKind)
{
    return srcMediaKind == NProto::STORAGE_MEDIA_HDD_NONREPLICATED
               ? NProto::STORAGE_MEDIA_HDD_NONREPLICATED
               : NProto::STORAGE_MEDIA_SSD_NONREPLICATED;
}

////////////////////////////////////////////////////////////////////////////////

TBlockRange64 BuildRequestBlockRange(
    const TEvService::TEvReadBlocksRequest& request,
    const ui32 blockSize);

TBlockRange64 BuildRequestBlockRange(
    const TEvService::TEvReadBlocksLocalRequest& request,
    const ui32 blockSize);

TBlockRange64 BuildRequestBlockRange(
    const TEvService::TEvWriteBlocksRequest& request,
    const ui32 blockSize);

TBlockRange64 BuildRequestBlockRange(
    const TEvService::TEvWriteBlocksLocalRequest& request,
    const ui32 blockSize);

TBlockRange64 BuildRequestBlockRange(
    const TEvService::TEvZeroBlocksRequest& request,
    const ui32 blockSize);

TBlockRange64 BuildRequestBlockRange(
    const TEvVolume::TEvDescribeBlocksRequest& request,
    const ui32 blockSize);

TBlockRange64 BuildRequestBlockRange(
    const TEvService::TEvGetChangedBlocksRequest& request,
    const ui32 blockSize);

TBlockRange64 BuildRequestBlockRange(
    const TEvVolume::TEvCompactRangeRequest& request,
    const ui32 blockSize);

TBlockRange64 BuildRequestBlockRange(
    const TEvDiskAgent::TEvWriteDeviceBlocksRequest& request);

TBlockRange64 BuildRequestBlockRange(
    const NProto::TWriteDeviceBlocksRequest& request);

TBlockRange64 BuildRequestBlockRange(
    const TEvDiskAgent::TEvZeroDeviceBlocksRequest& request);

ui64 GetVolumeRequestId(const TEvDiskAgent::TEvWriteDeviceBlocksRequest& request);
ui64 GetVolumeRequestId(const NProto::TWriteDeviceBlocksRequest& request);
ui64 GetVolumeRequestId(const TEvDiskAgent::TEvZeroDeviceBlocksRequest& request);

ui64 GetVolumeRequestId(const TEvService::TEvWriteBlocksRequest& request);
ui64 GetVolumeRequestId(const TEvService::TEvWriteBlocksLocalRequest& request);
ui64 GetVolumeRequestId(const TEvService::TEvZeroBlocksRequest& request);
ui64 GetVolumeRequestId(
    const TEvDiskAgent::TEvDirectCopyBlocksRequest& request);

TString LogDevices(const TVector<NProto::TDeviceConfig>& devices);

NProto::TVolumePerformanceProfile VolumeConfigToVolumePerformanceProfile(
    const NKikimrBlockStore::TVolumeConfig& volumeConfig);
}   // namespace NCloud::NBlockStore::NStorage
