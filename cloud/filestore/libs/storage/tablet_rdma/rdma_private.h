#pragma once

#include <cloud/filestore/libs/storage/api/components.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/event_local.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaTabletPrivate
{
    enum EEvents
    {
        EvBegin = TFileStoreEventsPrivate::RDMA_START,

        EvTabletGone = EvBegin + 1,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TFileStoreEventsPrivate::RDMA_END,
        "EvEnd expected to be < TFileStoreEventsPrivate::RDMA_END");

    // The tablet actor left the RDMA tablet registry.
    struct TEvTabletGone
        : public NActors::TEventLocal<TEvTabletGone, EvTabletGone>
    {
        const NActors::TActorId TabletActorId;

        explicit TEvTabletGone(const NActors::TActorId& tabletActorId)
            : TabletActorId(tabletActorId)
        {}
    };
};

}   // namespace NCloud::NFileStore::NStorage
