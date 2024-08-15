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
    xxx(ConfigureFollowers,         __VA_ARGS__)                               \
    xxx(ConfigureAsFollower,        __VA_ARGS__)                               \
    xxx(GetStorageConfig,           __VA_ARGS__)                               \
    xxx(GetNodeAttrBatch,           __VA_ARGS__)                               \
    xxx(WriteCompactionMap,         __VA_ARGS__)                               \
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

        EvConfigureFollowersRequest = EvBegin + 25,
        EvConfigureFollowersResponse,

        EvConfigureAsFollowerRequest = EvBegin + 27,
        EvConfigureAsFollowerResponse,

        EvGetStorageConfigRequest = EvBegin + 29,
        EvGetStorageConfigResponse,

        EvGetNodeAttrBatchRequest = EvBegin + 31,
        EvGetNodeAttrBatchResponse,

        EvWriteCompactionMapRequest = EvBegin + 33,
        EvWriteCompactionMapResponse,

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEvents::TABLET_END,
        "EvEnd expected to be < TFileStoreEvents::TABLET_END");

    FILESTORE_TABLET_REQUESTS(FILESTORE_DECLARE_PROTO_EVENTS, NProtoPrivate)
};

}   // namespace NCloud::NFileStore::NStorage
