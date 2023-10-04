#pragma once

#include "public.h"

#include "components.h"
#include "events.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <library/cpp/actors/core/actorid.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_TABLET_REQUESTS(xxx, ...)                                    \
    xxx(WaitReady,                  __VA_ARGS__)                               \
    xxx(CreateSession,              __VA_ARGS__)                               \
    xxx(DestroySession,             __VA_ARGS__)                               \
    xxx(GetStorageStats,            __VA_ARGS__)                               \
    xxx(GetFileSystemConfig,        __VA_ARGS__)                               \
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

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEvents::TABLET_END,
        "EvEnd expected to be < TFileStoreEvents::TABLET_END");

    FILESTORE_TABLET_REQUESTS(FILESTORE_DECLARE_PROTO_EVENTS, NProtoPrivate)
};

}   // namespace NCloud::NFileStore::NStorage
