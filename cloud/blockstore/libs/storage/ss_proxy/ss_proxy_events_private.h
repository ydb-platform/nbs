#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <library/cpp/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SS_PROXY_REQUESTS_PRIVATE(xxx, ...)                         \
    xxx(ReadPathDescriptionCache,   __VA_ARGS__)                               \
    xxx(UpdatePathDescriptionCache, __VA_ARGS__)                               \
// BLOCKSTORE_SS_PROXY_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvSSProxyPrivate
{
    //
    // ReadPathDescriptionCache
    //

    struct TReadPathDescriptionCacheRequest
    {
        const TString Path;

        explicit TReadPathDescriptionCacheRequest(TString path)
            : Path(std::move(path))
        {}
    };

    struct TReadPathDescriptionCacheResponse
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TReadPathDescriptionCacheResponse() = default;

        TReadPathDescriptionCacheResponse(
                TString path,
                NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}
    };

    //
    // UpdatePathDescriptionCache
    //

    struct TUpdatePathDescriptionCacheRequest
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TUpdatePathDescriptionCacheRequest(
                TString path,
                NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}
    };

    // unused
    struct TUpdatePathDescriptionCacheResponse
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStorePrivateEvents::SS_PROXY_START,

        BLOCKSTORE_SS_PROXY_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENT_IDS)

        EvEnd
    };

    BLOCKSTORE_SS_PROXY_REQUESTS_PRIVATE(BLOCKSTORE_DECLARE_EVENTS)
};

}   // namespace NCloud::NBlockStore::NStorage
