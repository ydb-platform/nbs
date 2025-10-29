#pragma once

#include "public.h"

#include <cloud/storage/core/libs/kikimr/components.h>
#include <cloud/storage/core/libs/kikimr/events.h>
#include <cloud/storage/core/libs/auth/auth_scheme.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_AUTH_REQUESTS(xxx, ...)                                        \
    xxx(Authorization,   __VA_ARGS__)                                          \
// STORAGE_AUTH_REQUESTS

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
        EvBegin = TStorageEvents::AUTH_START,

        EvAuthorizationRequest = EvBegin + 1,
        EvAuthorizationResponse = EvBegin + 2,

        EvEnd
    };

    static_assert(EvEnd < (int)TStorageEvents::AUTH_END,
        "EvEnd expected to be < TStorageEvents::AUTH_END");

    STORAGE_AUTH_REQUESTS(STORAGE_DECLARE_EVENTS)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeAuthorizerServiceId();

}   // namespace NCloud::NStorage
