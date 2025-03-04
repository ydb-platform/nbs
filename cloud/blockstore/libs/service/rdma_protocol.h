#pragma once

#include <cloud/blockstore/libs/rdma/iface/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Copy of BLOCKSTORE_SERVICE requests. Add my own macro to split control plane
// and data plane requests.
#define BLOCKSTORE_RDMA_STORAGE_SERVICE_CONTROL_PLANE(xxx, ...)                \
    xxx(Ping,                               __VA_ARGS__)                       \
    xxx(CreateVolume,                       __VA_ARGS__)                       \
    xxx(DestroyVolume,                      __VA_ARGS__)                       \
    xxx(ResizeVolume,                       __VA_ARGS__)                       \
    xxx(StatVolume,                         __VA_ARGS__)                       \
    xxx(AssignVolume,                       __VA_ARGS__)                       \
    xxx(MountVolume,                        __VA_ARGS__)                       \
    xxx(UnmountVolume,                      __VA_ARGS__)                       \
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
    xxx(CheckRange,                         __VA_ARGS__)                       \
    xxx(CreateVolumeLink,                   __VA_ARGS__)                       \
    xxx(DestroyVolumeLink,                  __VA_ARGS__)                       \
    xxx(StartEndpoint,                      __VA_ARGS__)                       \
    xxx(StopEndpoint,                       __VA_ARGS__)                       \
    xxx(ListEndpoints,                      __VA_ARGS__)                       \
    xxx(KickEndpoint,                       __VA_ARGS__)                       \
    xxx(ListKeyrings,                       __VA_ARGS__)                       \
    xxx(DescribeEndpoint,                   __VA_ARGS__)                       \
    xxx(RefreshEndpoint,                    __VA_ARGS__)                       \
// BLOCKSTORE_RDMA_STORAGE_SERVICE_CONTROL_PLANE

#define BLOCKSTORE_RDMA_STORAGE_SERVICE_DATA_PLANE(xxx, ...)                   \
    xxx(ReadBlocks,                         __VA_ARGS__)                       \
    xxx(WriteBlocks,                        __VA_ARGS__)                       \
    xxx(ZeroBlocks,                         __VA_ARGS__)                       \
// BLOCKSTORE_RDMA_STORAGE_SERVICE_DATA_PLANE

#define BLOCKSTORE_RDMA_STORAGE_SERVICE(xxx, ...)                              \
    BLOCKSTORE_RDMA_STORAGE_SERVICE_CONTROL_PLANE(xxx, __VA_ARGS__)            \
    BLOCKSTORE_RDMA_STORAGE_SERVICE_DATA_PLANE(xxx,    __VA_ARGS__)            \
// BLOCKSTORE_RDMA_STORAGE_SERVICE

struct TBlockStoreServerProtocol
{

#define BLOCKSTORE_DEFINE_E_MESSAGE_TYPE(name, ...) \
        Ev##name##Request,                          \
        Ev##name##Response,                         \
                                                    \

    enum EMessageType
    {
        BLOCKSTORE_RDMA_STORAGE_SERVICE(BLOCKSTORE_DEFINE_E_MESSAGE_TYPE)
    };

#undef BLOCKSTORE_DEFINE_E_MESSAGE_TYPE

    static NRdma::TProtoMessageSerializer* Serializer();
};

}   // namespace NCloud::NBlockStore::NStorage
