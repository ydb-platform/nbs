#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SS_PROXY_REQUESTS_PRIVATE(xxx, ...)                         \
    xxx(ReadPathDescriptionBackup,   __VA_ARGS__)                              \
    xxx(UpdatePathDescriptionBackup, __VA_ARGS__)                              \
// BLOCKSTORE_SS_PROXY_REQUESTS_PRIVATE

////////////////////////////////////////////////////////////////////////////////

struct TEvSSProxyPrivate
{
    //
    // ReadPathDescriptionBackup
    //

    struct TReadPathDescriptionBackupRequest
    {
        const TString Path;

        explicit TReadPathDescriptionBackupRequest(TString path)
            : Path(std::move(path))
        {}
    };

    struct TReadPathDescriptionBackupResponse
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TReadPathDescriptionBackupResponse() = default;

        TReadPathDescriptionBackupResponse(
                TString path,
                NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}
    };

    //
    // UpdatePathDescriptionBackup
    //

    struct TUpdatePathDescriptionBackupRequest
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TUpdatePathDescriptionBackupRequest(
                TString path,
                NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}
    };

    // unused
    struct TUpdatePathDescriptionBackupResponse
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
