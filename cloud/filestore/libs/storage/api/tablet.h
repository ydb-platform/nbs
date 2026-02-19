#pragma once

#include "public.h"

#include "components.h"
#include "events.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_TABLET_REQUESTS(xxx, ...)                                    \
    xxx(WaitReady,                  __VA_ARGS__)                               \
    xxx(CreateSession,              __VA_ARGS__)                               \
    xxx(DestroySession,             __VA_ARGS__)                               \
    xxx(GetStorageStats,            __VA_ARGS__)                               \
    xxx(GetFileSystemConfig,        __VA_ARGS__)                               \
    xxx(GetStorageConfigFields,     __VA_ARGS__)                               \
    xxx(ChangeStorageConfig,        __VA_ARGS__)                               \
    xxx(DescribeData,               __VA_ARGS__)                               \
    xxx(DescribeSessions,           __VA_ARGS__)                               \
    xxx(GenerateBlobIds,            __VA_ARGS__)                               \
    xxx(AddData,                    __VA_ARGS__)                               \
    xxx(ForcedOperation,            __VA_ARGS__)                               \
    xxx(ConfigureShards,            __VA_ARGS__)                               \
    xxx(ConfigureAsShard,           __VA_ARGS__)                               \
    xxx(GetStorageConfig,           __VA_ARGS__)                               \
    xxx(GetNodeAttrBatch,           __VA_ARGS__)                               \
    xxx(WriteCompactionMap,         __VA_ARGS__)                               \
    xxx(UnsafeCreateNode,           __VA_ARGS__)                               \
    xxx(UnsafeDeleteNode,           __VA_ARGS__)                               \
    xxx(UnsafeUpdateNode,           __VA_ARGS__)                               \
    xxx(UnsafeGetNode,              __VA_ARGS__)                               \
    xxx(UnsafeCreateNodeRef,        __VA_ARGS__)                               \
    xxx(UnsafeDeleteNodeRef,        __VA_ARGS__)                               \
    xxx(UnsafeUpdateNodeRef,        __VA_ARGS__)                               \
    xxx(UnsafeGetNodeRef,           __VA_ARGS__)                               \
    xxx(ForcedOperationStatus,      __VA_ARGS__)                               \
    xxx(GetFileSystemTopology,      __VA_ARGS__)                               \
    xxx(RestartTablet,              __VA_ARGS__)                               \
                                                                               \
    xxx(RenameNodeInDestination,            __VA_ARGS__)                       \
    xxx(PrepareUnlinkDirectoryNodeInShard,  __VA_ARGS__)                       \
    xxx(AbortUnlinkDirectoryNodeInShard,    __VA_ARGS__)                       \
    xxx(DeleteResponseLogEntry,             __VA_ARGS__)                       \
    xxx(GetResponseLogEntry,                __VA_ARGS__)                       \
    xxx(WriteResponseLogEntry,              __VA_ARGS__)                       \
                                                                               \
    xxx(ReadNodeRefs,              __VA_ARGS__)                                \
                                                                               \
    xxx(SetHasXAttrs,               __VA_ARGS__)                               \
    xxx(MarkNodeRefsExhaustive,     __VA_ARGS__)                               \
                                                                               \
    xxx(UnsafeCreateHandle,         __VA_ARGS__)                               \
// FILESTORE_TABLET_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvIndexTablet
{
    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TFileStoreEvents::TABLET_START,

        EvWaitReadyRequest = EvBegin + 1,
        EvWaitReadyResponse,

        EvCreateSessionRequest = EvBegin + 3,
        EvCreateSessionResponse,

        EvDestroySessionRequest = EvBegin + 5,
        EvDestroySessionResponse,

        EvGetStorageStatsRequest = EvBegin + 7,
        EvGetStorageStatsResponse,

        EvGetFileSystemConfigRequest = EvBegin + 9,
        EvGetFileSystemConfigResponse,

        EvGetStorageConfigFieldsRequest = EvBegin + 11,
        EvGetStorageConfigFieldsResponse,

        EvChangeStorageConfigRequest = EvBegin + 13,
        EvChangeStorageConfigResponse,

        EvDescribeDataRequest = EvBegin + 15,
        EvDescribeDataResponse,

        EvDescribeSessionsRequest = EvBegin + 17,
        EvDescribeSessionsResponse,

        EvGenerateBlobIdsRequest = EvBegin + 19,
        EvGenerateBlobIdsResponse,

        EvAddDataRequest = EvBegin + 21,
        EvAddDataResponse,

        EvForcedOperationRequest = EvBegin + 23,
        EvForcedOperationResponse,

        EvConfigureShardsRequest = EvBegin + 25,
        EvConfigureShardsResponse,

        EvConfigureAsShardRequest = EvBegin + 27,
        EvConfigureAsShardResponse,

        EvGetStorageConfigRequest = EvBegin + 29,
        EvGetStorageConfigResponse,

        EvGetNodeAttrBatchRequest = EvBegin + 31,
        EvGetNodeAttrBatchResponse,

        EvWriteCompactionMapRequest = EvBegin + 33,
        EvWriteCompactionMapResponse,

        EvUnsafeDeleteNodeRequest = EvBegin + 35,
        EvUnsafeDeleteNodeResponse,

        EvUnsafeUpdateNodeRequest = EvBegin + 37,
        EvUnsafeUpdateNodeResponse,

        EvUnsafeGetNodeRequest = EvBegin + 39,
        EvUnsafeGetNodeResponse,

        EvForcedOperationStatusRequest = EvBegin + 41,
        EvForcedOperationStatusResponse,

        EvGetFileSystemTopologyRequest = EvBegin + 43,
        EvGetFileSystemTopologyResponse,

        EvRestartTabletRequest = EvBegin + 45,
        EvRestartTabletResponse,

        EvRenameNodeInDestinationRequest = EvBegin + 47,
        EvRenameNodeInDestinationResponse,

        EvReadNodeRefsRequest = EvBegin + 49,
        EvReadNodeRefsResponse,

        EvSetHasXAttrsRequest = EvBegin + 51,
        EvSetHasXAttrsResponse,

        EvMarkNodeRefsExhaustiveRequest = EvBegin + 53,
        EvMarkNodeRefsExhaustiveResponse,

        EvUnsafeCreateNodeRefRequest = EvBegin + 55,
        EvUnsafeCreateNodeRefResponse,

        EvUnsafeDeleteNodeRefRequest = EvBegin + 57,
        EvUnsafeDeleteNodeRefResponse,

        EvUnsafeUpdateNodeRefRequest = EvBegin + 59,
        EvUnsafeUpdateNodeRefResponse,

        EvUnsafeGetNodeRefRequest = EvBegin + 61,
        EvUnsafeGetNodeRefResponse,

        EvUnsafeCreateHandleRequest = EvBegin + 63,
        EvUnsafeCreateHandleResponse,

        EvPrepareUnlinkDirectoryNodeInShardRequest = EvBegin + 65,
        EvPrepareUnlinkDirectoryNodeInShardResponse,

        EvAbortUnlinkDirectoryNodeInShardRequest = EvBegin + 67,
        EvAbortUnlinkDirectoryNodeInShardResponse,

        EvUnsafeCreateNodeRequest = EvBegin + 69,
        EvUnsafeCreateNodeResponse,

        EvDeleteResponseLogEntryRequest = EvBegin + 71,
        EvDeleteResponseLogEntryResponse,

        EvGetResponseLogEntryRequest = EvBegin + 73,
        EvGetResponseLogEntryResponse,

        EvWriteResponseLogEntryRequest = EvBegin + 75,
        EvWriteResponseLogEntryResponse,

        // After the TABLET sub-namespace we have TABLET_WORKER and TABLET_PROXY
        // sub-namespaces which don't have any non-local events so if we run out
        // of event ids in the TABLET sub-namespace we can extend it by
        // moving TABLET_WORKER and TABLET_PROXY after SS_PROXY

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEvents::TABLET_END,
        "EvEnd expected to be < TFileStoreEvents::TABLET_END");

    FILESTORE_TABLET_REQUESTS(FILESTORE_DECLARE_PROTO_EVENTS, NProtoPrivate)
};

}   // namespace NCloud::NFileStore::NStorage
