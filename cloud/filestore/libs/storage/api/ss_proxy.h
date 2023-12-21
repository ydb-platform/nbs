#pragma once

#include "public.h"

#include "components.h"
#include "events.h"

#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <library/cpp/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SS_PROXY_REQUESTS(xxx, ...)                                  \
    xxx(DescribeScheme,     __VA_ARGS__)                                       \
    xxx(ModifyScheme,       __VA_ARGS__)                                       \
    xxx(WaitSchemeTx,       __VA_ARGS__)                                       \
    xxx(DescribeFileStore,  __VA_ARGS__)                                       \
    xxx(CreateFileStore,    __VA_ARGS__)                                       \
    xxx(AlterFileStore,     __VA_ARGS__)                                       \
    xxx(DestroyFileStore,   __VA_ARGS__)                                       \
// FILESTORE_SS_PROXY_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvSSProxy
{
    //
    // DescribeScheme
    //

    struct TDescribeSchemeRequest
    {
        const TString Path;

        TDescribeSchemeRequest(TString path)
            : Path(std::move(path))
        {}
    };

    struct TDescribeSchemeResponse
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TDescribeSchemeResponse() = default;

        TDescribeSchemeResponse(
                TString path,
                NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}
    };

    //
    // ModifyScheme
    //

    struct TModifySchemeRequest
    {
        const NKikimrSchemeOp::TModifyScheme ModifyScheme;

        TModifySchemeRequest(
                NKikimrSchemeOp::TModifyScheme modifyScheme)
            : ModifyScheme(std::move(modifyScheme))
        {}
    };

    struct TModifySchemeResponse
    {
        const ui64 SchemeShardTabletId;
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TModifySchemeResponse(
                ui64 schemeShardTabletId = 0,
                NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
                TString reason = TString())
            : SchemeShardTabletId(schemeShardTabletId)
            , Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // WaitSchemeTx
    //

    struct TWaitSchemeTxRequest
    {
        const ui64 SchemeShardTabletId;
        const ui64 TxId;

        TWaitSchemeTxRequest(
                ui64 schemeShardTabletId,
                ui64 txId)
            : SchemeShardTabletId(schemeShardTabletId)
            , TxId(txId)
        {}
    };

    struct TWaitSchemeTxResponse
    {
    };

    //
    // DescribeFileStore
    //

    struct TDescribeFileStoreRequest
    {
        const TString FileSystemId;

        TDescribeFileStoreRequest(TString fileSystemId)
            : FileSystemId(std::move(fileSystemId))
        {}
    };

    struct TDescribeFileStoreResponse
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TDescribeFileStoreResponse() = default;

        TDescribeFileStoreResponse(
                TString path,
                NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}
    };

    //
    // CreateFileStore
    //

    struct TCreateFileStoreRequest
    {
        const NKikimrFileStore::TConfig Config;

        TCreateFileStoreRequest(NKikimrFileStore::TConfig config)
            : Config(std::move(config))
        {}
    };

    struct TCreateFileStoreResponse
    {
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TCreateFileStoreResponse(
                NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
                TString reason = {})
            : Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // AlterFileStore
    //

    struct TAlterFileStoreRequest
    {
        const NKikimrFileStore::TConfig Config;

        TAlterFileStoreRequest(
                NKikimrFileStore::TConfig config)
            : Config(std::move(config))
        {}
    };

    struct TAlterFileStoreResponse
    {
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TAlterFileStoreResponse(
                NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
                TString reason = TString())
            : Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // DestroyFileStore
    //

    struct TDestroyFileStoreRequest
    {
        const TString FileSystemId;

        TDestroyFileStoreRequest(TString fileSystemId)
            : FileSystemId(std::move(fileSystemId))
        {}
    };

    struct TDestroyFileStoreResponse
    {
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TDestroyFileStoreResponse(
                NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
                TString reason = {})
            : Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TFileStoreEvents::SS_PROXY_START,

        EvDescribeSchemeRequest = EvBegin + 1,
        EvDescribeSchemeResponse = EvBegin + 2,

        EvModifySchemeRequest = EvBegin + 3,
        EvModifySchemeResponse = EvBegin + 4,

        EvWaitSchemeTxRequest = EvBegin + 5,
        EvWaitSchemeTxResponse = EvBegin + 6,

        EvDescribeFileStoreRequest = EvBegin + 7,
        EvDescribeFileStoreResponse = EvBegin + 8,

        EvCreateFileStoreRequest = EvBegin + 9,
        EvCreateFileStoreResponse = EvBegin + 10,

        EvAlterFileStoreRequest = EvBegin + 11,
        EvAlterFileStoreResponse = EvBegin + 12,

        EvDestroyFileStoreRequest = EvBegin + 13,
        EvDestroyFileStoreResponse = EvBegin + 14,

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEvents::SS_PROXY_END,
        "EvEnd expected to be < TFileStoreEvents::SS_PROXY_END");

    FILESTORE_SS_PROXY_REQUESTS(FILESTORE_DECLARE_EVENTS)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeSSProxyServiceId();

}   // namespace NCloud::NFileStore::NStorage
