#pragma once

#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/storage/api/components.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/event_local.h>

#include <library/cpp/threading/future/future.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaTabletPrivate
{
    enum EEvents
    {
        EvBegin = TFileStoreEventsPrivate::RDMA_START,

        EvTrackRequest = EvBegin + 1,
        EvTabletGone,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TFileStoreEventsPrivate::RDMA_END,
        "EvEnd expected to be < TFileStoreEventsPrivate::RDMA_END");

    // Precedes the request that the RDMA request handler sends to the tablet
    // with the same cookie; Promise gets the tablet's typed response.
    struct TEvTrackRequest
        : public NActors::TEventLocal<TEvTrackRequest, EvTrackRequest>
    {
        const NActors::TActorId TabletActorId;
        NThreading::TPromise<TTabletTransportResponse> Promise;

        TEvTrackRequest(
                const NActors::TActorId& tabletActorId,
                NThreading::TPromise<TTabletTransportResponse> promise)
            : TabletActorId(tabletActorId)
            , Promise(std::move(promise))
        {}

        ~TEvTrackRequest()
        {
            // the event was dropped before the forwarder took the promise
            if (Promise.Initialized() && !Promise.HasValue()) {
                Promise.SetValue(TTabletTransportResponse(
                    MakeError(E_REJECTED, "rdma forwarder unavailable")));
            }
        }
    };

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
