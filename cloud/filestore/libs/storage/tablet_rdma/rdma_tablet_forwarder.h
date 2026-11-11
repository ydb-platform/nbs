#pragma once

#include "rdma_counters.h"

#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/storage/api/components.h>

#include <contrib/ydb/library/actors/core/actor.h>
#include <contrib/ydb/library/actors/core/event_local.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvRdmaTabletForwarder
{
    enum EEvents
    {
        EvBegin = TFileStoreEventsPrivate::RDMA_START,

        EvForwardRequest = EvBegin + 1,

        EvEnd
    };

    static_assert(
        EvEnd < (int)TFileStoreEventsPrivate::RDMA_END,
        "EvEnd expected to be < TFileStoreEventsPrivate::RDMA_END");

    // A decoded RDMA request on its way to the tablet; Promise gets the
    // tablet's typed response.
    struct TEvForwardRequest
        : public NActors::TEventLocal<TEvForwardRequest, EvForwardRequest>
    {
        const ui64 TabletId;
        std::unique_ptr<NActors::IEventBase> Request;
        NThreading::TPromise<TTabletTransportResponse> Promise;

        TEvForwardRequest(
                ui64 tabletId,
                std::unique_ptr<NActors::IEventBase> request,
                NThreading::TPromise<TTabletTransportResponse> promise)
            : TabletId(tabletId)
            , Request(std::move(request))
            , Promise(std::move(promise))
        {}

        ~TEvForwardRequest()
        {
            // the event was dropped before the forwarder took the promise
            if (Promise.Initialized() && !Promise.HasValue()) {
                Promise.SetValue(TTabletTransportResponse(
                    MakeError(E_REJECTED, "rdma forwarder unavailable")));
            }
        }
    };
};

////////////////////////////////////////////////////////////////////////////////

// Per-node actor that pipes RDMA-injected requests straight to the addressed
// tablet (no tablet proxy hop) and resolves their promises with the typed
// responses. requestTimeout of zero disables the timeout.
std::unique_ptr<NActors::IActor> CreateRdmaTabletForwarderActor(
    TDuration requestTimeout,
    TRdmaForwarderCountersPtr counters);

}   // namespace NCloud::NFileStore::NStorage
