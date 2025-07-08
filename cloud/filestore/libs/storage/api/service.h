#pragma once

#include "public.h"

#include "components.h"
#include "events.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SERVICE_REQUESTS_FWD(xxx, ...)                               \
    xxx(AddClusterNode,                     __VA_ARGS__)                       \
    xxx(RemoveClusterNode,                  __VA_ARGS__)                       \
    xxx(ListClusterNodes,                   __VA_ARGS__)                       \
    xxx(AddClusterClients,                  __VA_ARGS__)                       \
    xxx(RemoveClusterClients,               __VA_ARGS__)                       \
    xxx(ListClusterClients,                 __VA_ARGS__)                       \
    xxx(UpdateCluster,                      __VA_ARGS__)                       \
                                                                               \
    xxx(ResetSession,                       __VA_ARGS__)                       \
                                                                               \
    xxx(SubscribeSession,                   __VA_ARGS__)                       \
                                                                               \
    xxx(CreateCheckpoint,                   __VA_ARGS__)                       \
    xxx(DestroyCheckpoint,                  __VA_ARGS__)                       \
                                                                               \
    xxx(ResolvePath,                        __VA_ARGS__)                       \
                                                                               \
    xxx(ReadNodeRefs,                       __VA_ARGS__)                       \
// FILESTORE_SERVICE_REQUESTS_FWD

#define FILESTORE_SERVICE_REQUESTS_FWD_TO_SHARD_BY_NODE_ID(xxx, ...)           \
    xxx(AccessNode,                         __VA_ARGS__)                       \
    xxx(SetNodeAttr,                        __VA_ARGS__)                       \
    xxx(GetNodeXAttr,                       __VA_ARGS__)                       \
    xxx(SetNodeXAttr,                       __VA_ARGS__)                       \
    xxx(ListNodeXAttr,                      __VA_ARGS__)                       \
    xxx(RemoveNodeXAttr,                    __VA_ARGS__)                       \
                                                                               \
    xxx(UnlinkNode,                         __VA_ARGS__)                       \
    xxx(RenameNode,                         __VA_ARGS__)                       \
    xxx(ReadLink,                           __VA_ARGS__)                       \
// FILESTORE_SERVICE_REQUESTS_FWD_TO_SHARD_BY_NODE_ID

#define FILESTORE_SERVICE_REQUESTS_FWD_TO_SHARD_BY_HANDLE(xxx, ...)            \
    xxx(DestroyHandle,                      __VA_ARGS__)                       \
    xxx(AllocateData,                       __VA_ARGS__)                       \
                                                                               \
    xxx(AcquireLock,                        __VA_ARGS__)                       \
    xxx(ReleaseLock,                        __VA_ARGS__)                       \
    xxx(TestLock,                           __VA_ARGS__)                       \
// FILESTORE_SERVICE_REQUESTS_FWD_TO_SHARD_BY_HANDLE

#define FILESTORE_SERVICE_REQUESTS_HANDLE(xxx, ...)                            \
    xxx(WriteData,                          __VA_ARGS__)                       \
    xxx(ReadData,                           __VA_ARGS__)                       \
    xxx(ListNodes,                          __VA_ARGS__)                       \
    xxx(GetNodeAttr,                        __VA_ARGS__)                       \
    xxx(CreateHandle,                       __VA_ARGS__)                       \
    xxx(CreateNode,                         __VA_ARGS__)                       \
// FILESTORE_SERVICE_REQUESTS_HANDLE

#define FILESTORE_SERVICE_REQUESTS(xxx, ...)                                   \
    FILESTORE_SERVICE_REQUESTS_HANDLE(xxx,   __VA_ARGS__)                      \
    FILESTORE_SERVICE_REQUESTS_FWD(xxx,      __VA_ARGS__)                      \
    FILESTORE_SERVICE_REQUESTS_FWD_TO_SHARD_BY_NODE_ID(xxx,  __VA_ARGS__)      \
    FILESTORE_SERVICE_REQUESTS_FWD_TO_SHARD_BY_HANDLE(xxx,   __VA_ARGS__)      \
// FILESTORE_SERVICE_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvService
{
    static const ui64 StreamCookie = -1;

    //
    // Register Local Filestore
    //

    struct TRegisterLocalFileStore
    {
        const TString FileStoreId;
        const ui64 TabletId;
        const ui64 Generation;
        const bool IsShard;
        NProtoPrivate::TFileSystemConfig Config;

        TRegisterLocalFileStore(
                TString fileStoreId,
                ui64 tablet,
                ui64 generation,
                bool isShard,
                NProtoPrivate::TFileSystemConfig config)
            : FileStoreId(std::move(fileStoreId))
            , TabletId(tablet)
            , Generation(generation)
            , IsShard(isShard)
            , Config(std::move(config))
        {}
    };

    struct TUnregisterLocalFileStore
    {
        const TString FileStoreId;
        const ui64 Generation;

        TUnregisterLocalFileStore(
                TString fileStoreId,
                ui64 generation)
            : FileStoreId(std::move(fileStoreId))
            , Generation(generation)
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TFileStoreEvents::SERVICE_START,

        EvPingRequest = EvBegin + 1,
        EvPingResponse,

        EvCreateFileStoreRequest = EvBegin + 3,
        EvCreateFileStoreResponse,

        EvDestroyFileStoreRequest = EvBegin + 5,
        EvDestroyFileStoreResponse,

        EvGetFileStoreInfoRequest = EvBegin + 7,
        EvGetFileStoreInfoResponse,

        EvCreateSessionRequest = EvBegin + 9,
        EvCreateSessionResponse,

        EvDestroySessionRequest = EvBegin + 11,
        EvDestroySessionResponse,

        EvPingSessionRequest = EvBegin + 13,
        EvPingSessionResponse,

        EvCreateCheckpointRequest = EvBegin + 15,
        EvCreateCheckpointResponse,

        EvDestroyCheckpointRequest = EvBegin + 17,
        EvDestroyCheckpointResponse,

        EvCreateNodeRequest = EvBegin + 19,
        EvCreateNodeResponse,

        EvUnlinkNodeRequest = EvBegin + 21,
        EvUnlinkNodeResponse,

        EvRenameNodeRequest = EvBegin + 23,
        EvRenameNodeResponse,

        EvAccessNodeRequest = EvBegin + 25,
        EvAccessNodeResponse,

        EvListNodesRequest = EvBegin + 27,
        EvListNodesResponse,

        EvSetNodeAttrRequest = EvBegin + 29,
        EvSetNodeAttrResponse,

        EvGetNodeAttrRequest = EvBegin + 31,
        EvGetNodeAttrResponse,

        EvSetNodeXAttrRequest = EvBegin + 33,
        EvSetNodeXAttrResponse,

        EvGetNodeXAttrRequest = EvBegin + 35,
        EvGetNodeXAttrResponse,

        EvListNodeXAttrRequest = EvBegin + 37,
        EvListNodeXAttrResponse,

        EvRemoveNodeXAttrRequest = EvBegin + 39,
        EvRemoveNodeXAttrResponse,

        EvCreateHandleRequest = EvBegin + 41,
        EvCreateHandleResponse,

        EvDestroyHandleRequest = EvBegin + 43,
        EvDestroyHandleResponse,

        EvAcquireLockRequest = EvBegin + 45,
        EvAcquireLockResponse,

        EvReleaseLockRequest = EvBegin + 47,
        EvReleaseLockResponse,

        EvTestLockRequest = EvBegin + 49,
        EvTestLockResponse,

        EvReadDataRequest = EvBegin + 51,
        EvReadDataResponse,

        EvWriteDataRequest = EvBegin + 53,
        EvWriteDataResponse,

        EvAllocateDataRequest = EvBegin + 55,
        EvAllocateDataResponse,

        EvResolvePathRequest = EvBegin + 57,
        EvResolvePathResponse,

        EvReadLinkRequest = EvBegin + 59,
        EvReadLinkResponse,

        EvAlterFileStoreRequest = EvBegin + 61,
        EvAlterFileStoreResponse,

        EvResizeFileStoreRequest = EvBegin + 63,
        EvResizeFileStoreResponse,

        EvDescribeFileStoreModelRequest = EvBegin + 65,
        EvDescribeFileStoreModelResponse,

        EvSubscribeSessionRequest = EvBegin + 67,
        EvSubscribeSessionResponse,

        EvGetSessionEventsRequest = EvBegin + 69,
        EvGetSessionEventsResponse,

        // Show local tablets in the service monitoring.
        EvRegisterLocalFileStore = EvBegin + 71,
        EvUnregisterLocalFileStore,

        // EvGetSessionAttrRequest
        EvUnused2 = EvBegin + 73,

        EvAddClusterNodeRequest = EvBegin + 75,
        EvAddClusterNodeResponse,

        EvRemoveClusterNodeRequest = EvBegin + 77,
        EvRemoveClusterNodeResponse,

        EvListClusterNodesRequest = EvBegin + 79,
        EvListClusterNodesResponse,

        EvAddClusterClientsRequest = EvBegin + 81,
        EvAddClusterClientsResponse,

        EvRemoveClusterClientsRequest = EvBegin + 83,
        EvRemoveClusterClientsResponse,

        EvListClusterClientsRequest = EvBegin + 85,
        EvListClusterClientsResponse,

        EvUpdateClusterRequest = EvBegin + 87,
        EvUpdateClusterResponse,

        EvStatFileStoreRequest = EvBegin + 89,
        EvStatFileStoreResponse,

        EvResetSessionRequest = EvBegin + 91,
        EvResetSessionResponse,

        EvListFileStoresRequest = EvBegin + 93,
        EvListFileStoresResponse,

        EvExecuteActionRequest = EvBegin + 95,
        EvExecuteActionResponse,

        EvReadNodeRefsRequest = EvBegin + 97,
        EvReadNodeRefsResponse,

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEvents::SERVICE_END,
        "EvEnd expected to be < TFileStoreEvents::SERVICE_END");

    FILESTORE_REMOTE_SERVICE(FILESTORE_DECLARE_PROTO_EVENTS, NProto)

    using TEvRegisterLocalFileStoreRequest = TRequestEvent<
        TRegisterLocalFileStore,
        EvRegisterLocalFileStore>;

    using TEvUnregisterLocalFileStoreRequest = TRequestEvent<
        TUnregisterLocalFileStore,
        EvUnregisterLocalFileStore>;
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeStorageServiceId();

}   // namespace NCloud::NFileStore::NStorage
