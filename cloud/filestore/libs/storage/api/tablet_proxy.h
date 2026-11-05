#pragma once

#include "public.h"

#include "components.h"
#include "events.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/public/api/protos/filesystem_event.pb.h>

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvIndexTabletProxy
{
    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TFileStoreEvents::TABLET_PROXY_START,

        EvFileSystemEvent = EvBegin + 1,

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEvents::TABLET_PROXY_END,
        "EvEnd expected to be < TFileStoreEvents::TABLET_PROXY_END");

    //
    // FileSystemEvent is sent by the tablet to the clients (IndexTabletProxy
    // actors) that hold pipes to it. It is not a response to any request.
    //

    struct TEvFileSystemEvent
        : public NActors::TEventPB<
              TEvFileSystemEvent,
              NProto::TFileSystemEvent,
              EvFileSystemEvent>
    {
    };
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeIndexTabletProxyServiceId();

}   // namespace NCloud::NFileStore::NStorage
