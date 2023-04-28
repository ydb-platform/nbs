#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>
#include <cloud/blockstore/libs/service/auth_scheme.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_AUTH_REQUESTS(xxx, ...)                                     \
    xxx(Authorization,   __VA_ARGS__)                                          \
// BLOCKSTORE_AUTH_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvAuth
{
    //
    // Authorization
    //

    struct TAuthorizationRequest
    {
        const TString Token;
        const TPermissionList Permissions;

        TAuthorizationRequest(
                TString token,
                TPermissionList permissions)
            : Token(std::move(token))
            , Permissions(std::move(permissions))
        {}
    };

    struct TAuthorizationResponse
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::AUTH_START,

        EvAuthorizationRequest = EvBegin + 1,
        EvAuthorizationResponse = EvBegin + 2,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::AUTH_END,
        "EvEnd expected to be < TBlockStoreEvents::AUTH_END");

    BLOCKSTORE_AUTH_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeAuthorizerServiceId();

}   // namespace NCloud::NBlockStore::NStorage
