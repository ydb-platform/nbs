#pragma once

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/kikimr/components.h>
#include <cloud/storage/core/libs/kikimr/events.h>

#include <contrib/ydb/library/actors/core/actorid.h>

#include <util/system/types.h>

#include <utility>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TEvTabletProxy
{
    struct TForward
    {
        const ui64 TabletId;
        NActors::IEventHandlePtr Request;
        // Send TEvPipeClosed to the sender when the pipe goes away.
        const bool Subscribe;

        TForward(
                ui64 tabletId,
                NActors::IEventHandlePtr request,
                bool subscribe)
            : TabletId(tabletId)
            , Request(std::move(request))
            , Subscribe(subscribe)
        {}
    };

    // Notification that pipe broke before receiving reply.
    struct TDisconnected
    {
        const ui64 TabletId;

        explicit TDisconnected(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    // The pipe is gone; sent once to each subscriber of that pipe.
    struct TPipeClosed
    {
        const ui64 TabletId;

        explicit TPipeClosed(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    // Keeps an open tablet pipe active. Fails if there is no pipe: only a
    // forwarded request opens one.
    struct TKeepAliveRequest
    {
        const ui64 TabletId;

        explicit TKeepAliveRequest(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    struct TKeepAliveResponse
    {};

    enum EEvents
    {
        EvBegin = TStorageEvents::TABLET_PROXY_START,

        EvForward,
        EvDisconnected,
        EvKeepAliveRequest,
        EvKeepAliveResponse,
        EvPipeClosed,

        EvEnd
    };

    static_assert(EvEnd < TStorageEvents::TABLET_PROXY_END);

    using TEvForward = TRequestEvent<TForward, EvForward>;
    using TEvDisconnected = TResponseEvent<TDisconnected, EvDisconnected>;
    using TEvKeepAliveRequest =
        TRequestEvent<TKeepAliveRequest, EvKeepAliveRequest>;
    using TEvKeepAliveResponse =
        TResponseEvent<TKeepAliveResponse, EvKeepAliveResponse>;
    using TEvPipeClosed = TRequestEvent<TPipeClosed, EvPipeClosed>;
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeTabletProxyServiceId();

void ForwardToTablet(ui64 tabletId, NActors::IEventHandlePtr request);

// With @p subscribe the caller gets TEvPipeClosed when the pipe goes away.
void ForwardToTablet(
    ui64 tabletId,
    NActors::IEventHandlePtr request,
    bool subscribe);

}   // namespace NCloud::NStorage
