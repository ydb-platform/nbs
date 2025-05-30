#pragma once

#include "public.h"

#include <cloud/blockstore/public/api/protos/actions.pb.h>
#include <cloud/blockstore/public/api/protos/checkpoints.pb.h>
#include <cloud/blockstore/public/api/protos/cms.pb.h>
#include <cloud/blockstore/public/api/protos/discovery.pb.h>
#include <cloud/blockstore/public/api/protos/disk.pb.h>
#include <cloud/blockstore/public/api/protos/endpoints.pb.h>
#include <cloud/blockstore/public/api/protos/io.pb.h>
#include <cloud/blockstore/public/api/protos/local_ssd.pb.h>
#include <cloud/blockstore/public/api/protos/metrics.pb.h>
#include <cloud/blockstore/public/api/protos/mount.pb.h>
#include <cloud/blockstore/public/api/protos/ping.pb.h>
#include <cloud/blockstore/public/api/protos/placement.pb.h>
#include <cloud/blockstore/public/api/protos/volume.pb.h>

#include <cloud/storage/core/libs/common/guarded_sglist.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore {

namespace NProto {

////////////////////////////////////////////////////////////////////////////////

struct TReadBlocksLocalRequest
    : public TReadBlocksRequest
{
    TGuardedSgList Sglist;
    ui64 CommitId = 0;
    ui32 BlockSize = 0;
};

struct TReadBlocksLocalResponse: public TReadBlocksResponse
{
    TVector<TString> ScanDiskResults;

    TReadBlocksLocalResponse() = default;

    explicit TReadBlocksLocalResponse(const TReadBlocksResponse& base)
        : TReadBlocksResponse(base)
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TWriteBlocksLocalRequest
    : public TWriteBlocksRequest
{
    TGuardedSgList Sglist;
    ui32 BlocksCount = 0;
    ui32 BlockSize = 0;
};

using TWriteBlocksLocalResponse = TWriteBlocksResponse;

}   // namespace NProto

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_GRPC_STORAGE_SERVICE(xxx, ...)                              \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(CreateVolume,                       __VA_ARGS__)                       \
    xxx(DestroyVolume,                      __VA_ARGS__)                       \
    xxx(ResizeVolume,                       __VA_ARGS__)                       \
    xxx(StatVolume,                         __VA_ARGS__)                       \
    xxx(AssignVolume,                       __VA_ARGS__)                       \
    xxx(MountVolume,                        __VA_ARGS__)                       \
    xxx(UnmountVolume,                      __VA_ARGS__)                       \
    xxx(ReadBlocks,                         __VA_ARGS__)                       \
    xxx(WriteBlocks,                        __VA_ARGS__)                       \
    xxx(ZeroBlocks,                         __VA_ARGS__)                       \
    xxx(CreateCheckpoint,                   __VA_ARGS__)                       \
    xxx(DeleteCheckpoint,                   __VA_ARGS__)                       \
    xxx(AlterVolume,                        __VA_ARGS__)                       \
    xxx(GetChangedBlocks,                   __VA_ARGS__)                       \
    xxx(GetCheckpointStatus,                __VA_ARGS__)                       \
    xxx(DescribeVolume,                     __VA_ARGS__)                       \
    xxx(ListVolumes,                        __VA_ARGS__)                       \
    xxx(UploadClientMetrics,                __VA_ARGS__)                       \
    xxx(DiscoverInstances,                  __VA_ARGS__)                       \
    xxx(ExecuteAction,                      __VA_ARGS__)                       \
    xxx(DescribeVolumeModel,                __VA_ARGS__)                       \
    xxx(UpdateDiskRegistryConfig,           __VA_ARGS__)                       \
    xxx(DescribeDiskRegistryConfig,         __VA_ARGS__)                       \
    xxx(CreatePlacementGroup,               __VA_ARGS__)                       \
    xxx(DestroyPlacementGroup,              __VA_ARGS__)                       \
    xxx(AlterPlacementGroupMembership,      __VA_ARGS__)                       \
    xxx(DescribePlacementGroup,             __VA_ARGS__)                       \
    xxx(ListPlacementGroups,                __VA_ARGS__)                       \
    xxx(CmsAction,                          __VA_ARGS__)                       \
    xxx(QueryAvailableStorage,              __VA_ARGS__)                       \
    xxx(CreateVolumeFromDevice,             __VA_ARGS__)                       \
    xxx(ResumeDevice,                       __VA_ARGS__)                       \
    xxx(QueryAgentsInfo,                    __VA_ARGS__)                       \
    xxx(CreateVolumeLink,                   __VA_ARGS__)                       \
    xxx(DestroyVolumeLink,                  __VA_ARGS__)                       \
// BLOCKSTORE_GRPC_STORAGE_SERVICE

#define BLOCKSTORE_ENDPOINT_SERVICE(xxx, ...)                                  \
    xxx(StartEndpoint,                      __VA_ARGS__)                       \
    xxx(StopEndpoint,                       __VA_ARGS__)                       \
    xxx(ListEndpoints,                      __VA_ARGS__)                       \
    xxx(KickEndpoint,                       __VA_ARGS__)                       \
    xxx(ListKeyrings,                       __VA_ARGS__)                       \
    xxx(DescribeEndpoint,                   __VA_ARGS__)                       \
    xxx(RefreshEndpoint,                    __VA_ARGS__)                       \
// BLOCKSTORE_ENDPOINT_SERVICE

#define BLOCKSTORE_GRPC_SERVICE(xxx, ...)                                      \
    BLOCKSTORE_GRPC_STORAGE_SERVICE(xxx,    __VA_ARGS__)                       \
    BLOCKSTORE_ENDPOINT_SERVICE(xxx,        __VA_ARGS__)                       \
// BLOCKSTORE_GRPC_SERVICE

#define BLOCKSTORE_GRPC_DATA_SERVICE(xxx, ...)                                 \
    xxx(MountVolume,                        __VA_ARGS__)                       \
    xxx(UnmountVolume,                      __VA_ARGS__)                       \
    xxx(ReadBlocks,                         __VA_ARGS__)                       \
    xxx(WriteBlocks,                        __VA_ARGS__)                       \
    xxx(ZeroBlocks,                         __VA_ARGS__)                       \
    xxx(UploadClientMetrics,                __VA_ARGS__)                       \
// BLOCKSTORE_GRPC_DATA_SERVICE

#define BLOCKSTORE_LOCAL_SERVICE(xxx, ...)                                     \
    xxx(ReadBlocksLocal,                    __VA_ARGS__)                       \
    xxx(WriteBlocksLocal,                   __VA_ARGS__)                       \
// BLOCKSTORE_LOCAL_SERVICE

#define BLOCKSTORE_STORAGE_SERVICE(xxx, ...)                                   \
    BLOCKSTORE_GRPC_STORAGE_SERVICE(xxx,    __VA_ARGS__)                       \
    BLOCKSTORE_LOCAL_SERVICE(xxx,           __VA_ARGS__)                       \
// BLOCKSTORE_STORAGE_SERVICE

#define BLOCKSTORE_SERVICE(xxx, ...)                                           \
    BLOCKSTORE_STORAGE_SERVICE(xxx,         __VA_ARGS__)                       \
    BLOCKSTORE_ENDPOINT_SERVICE(xxx,        __VA_ARGS__)                       \
// BLOCKSTORE_SERVICE

#define BLOCKSTORE_DATA_SERVICE(xxx, ...)                                      \
    BLOCKSTORE_GRPC_DATA_SERVICE(xxx,       __VA_ARGS__)                       \
    BLOCKSTORE_LOCAL_SERVICE(xxx,           __VA_ARGS__)                       \
// BLOCKSTORE_DATA_SERVICE

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...) name,

enum class EBlockStoreRequest
{
    BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)
    MAX
};

#undef BLOCKSTORE_DECLARE_METHOD

constexpr size_t BlockStoreRequestsCount = (size_t)EBlockStoreRequest::MAX;

const TString& GetBlockStoreRequestName(EBlockStoreRequest requestType);

enum class ESysRequestType
{
    Compaction = 10000,
    Flush = 10001,
    ConvertToMixedIndex = 10002,
    ConvertToRangeMap = 10003,
    Cleanup = 10004,
    Migration = 10005,
    WriteDeviceBlocks = 10006,
    ZeroDeviceBlocks = 10007,
    Resync = 10008,
    ConfirmBlobs = 10009,
    MAX
};

TStringBuf GetSysRequestName(ESysRequestType requestType);

enum class EPrivateRequestType
{
    DescribeBlocks = 20000,
    MAX
};

TStringBuf GetPrivateRequestName(EPrivateRequestType requestType);

}   // namespace NCloud::NBlockStore
