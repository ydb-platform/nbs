#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_REGISTRY_PROXY_REQUESTS(xxx, ...)                      \
    xxx(Subscribe,            __VA_ARGS__)                                     \
    xxx(Unsubscribe,          __VA_ARGS__)                                     \
    xxx(Reassign,             __VA_ARGS__)                                     \
    xxx(GetDrTabletInfo,      __VA_ARGS__)                                     \
// BLOCKSTORE_DISK_REGISTRY_PROXY_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvDiskRegistryProxy
{
    //
    // Subscribe
    //

    struct TSubscribeRequest
    {
        NActors::TActorId Subscriber;

        explicit TSubscribeRequest(NActors::TActorId subscriber)
            : Subscriber(std::move(subscriber))
        {}
    };

    struct TSubscribeResponse
    {
        const bool Discovered = false;

        TSubscribeResponse() = default;

        explicit TSubscribeResponse(bool discovered)
            : Discovered(discovered)
        {}
    };

    //
    // Unsubscribe
    //

    struct TUnsubscribeRequest
    {
        NActors::TActorId Subscriber;

        explicit TUnsubscribeRequest(NActors::TActorId subscriber)
            : Subscriber(std::move(subscriber))
        {}
    };

    struct TUnsubscribeResponse
    {
    };

    //
    // ConnectionEstablished notification
    //

    struct TConnectionEstablished
    {
    };

    //
    // ConnectionLost notification
    //

    struct TConnectionLost
    {
    };

    //
    // Reassign
    //

    struct TReassignRequest
    {
        TString SysKind;
        TString LogKind;
        TString IndexKind;

        TReassignRequest(
                TString sysKind,
                TString logKind,
                TString indexKind)
            : SysKind(std::move(sysKind))
            , LogKind(std::move(logKind))
            , IndexKind(std::move(indexKind))
        {}
    };

    struct TReassignResponse
    {
    };

    //
    // GetDrTabletId notification
    //

    struct TGetDrTabletInfoRequest
    {
    };

    struct TGetDrTabletInfoResponse
    {
        const ui64 TabletId = 0;

        TGetDrTabletInfoResponse() = default;

        explicit TGetDrTabletInfoResponse(ui64 tabletId)
            : TabletId(tabletId)
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::DISK_REGISTRY_PROXY_START,

        EvSubscribeRequest,
        EvSubscribeResponse,

        EvUnsubscribeRequest,
        EvUnsubscribeResponse,

        EvConnectionLost,
        EvConnectionEstablished,

        EvReassignRequest,
        EvReassignResponse,

        EvGetDrTabletInfoRequest,
        EvGetDrTabletInfoResponse,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::DISK_REGISTRY_PROXY_END,
        "EvEnd expected to be < TBlockStoreEvents::DISK_REGISTRY_PROXY_END");

    BLOCKSTORE_DISK_REGISTRY_PROXY_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)

    using TEvConnectionLost = TResponseEvent<TConnectionLost, EvConnectionLost>;
    using TEvConnectionEstablished =
        TResponseEvent<TConnectionEstablished, EvConnectionEstablished>;
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeDiskRegistryProxyServiceId();

}   // namespace NCloud::NBlockStore::NStorage
