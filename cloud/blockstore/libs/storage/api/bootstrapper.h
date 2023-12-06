#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvBootstrapper
{
    //
    // Start
    //

    struct TStart
    {
    };

    //
    // Stop
    //

    struct TStop
    {
    };

    //
    // Status
    //

    enum ETabletStatus
    {
        STARTED,
        STOPPED,
        FAILED,
        RACE,
        SUGGEST_OUTDATED,
    };

    struct TStatus
    {
        const ui64 TabletId;
        const NActors::TActorId TabletSys;
        const NActors::TActorId TabletUser;
        const ETabletStatus Status;
        const TString Message;

        TStatus(ui64 tabletId,
                const NActors::TActorId& tabletSys,
                const NActors::TActorId& tabletUser,
                ETabletStatus status,
                TString message)
            : TabletId(tabletId)
            , TabletSys(tabletSys)
            , TabletUser(tabletUser)
            , Status(status)
            , Message(message)
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::BOOTSTRAPPER_START,

        EvStart,
        EvStop,
        EvStatus,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::BOOTSTRAPPER_END,
        "EvEnd expected to be < TBlockStoreEvents::BOOTSTRAPPER_END");

    using TEvStart = TRequestEvent<TStart, EvStart>;
    using TEvStop = TRequestEvent<TStop, EvStop>;
    using TEvStatus = TRequestEvent<TStatus, EvStatus>;
};

}   // namespace NCloud::NBlockStore::NStorage
