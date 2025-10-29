#pragma once

#include "public.h"

#include <cloud/storage/core/libs/kikimr/components.h>
#include <cloud/storage/core/libs/kikimr/events.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define STORAGE_SS_PROXY_REQUESTS(xxx, ...)                                    \
    xxx(DescribeScheme,         __VA_ARGS__)                                   \
    xxx(ModifyScheme,           __VA_ARGS__)                                   \
    xxx(WaitSchemeTx,           __VA_ARGS__)                                   \
    xxx(BackupPathDescriptions, __VA_ARGS__)                                   \
// STORAGE_SS_PROXY_REQUESTS

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
    // BackupPathDescriptions
    //

    struct TBackupPathDescriptionsRequest
    {
    };

    struct TBackupPathDescriptionsResponse
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TStorageEvents::SS_PROXY_START,

        EvDescribeSchemeRequest = EvBegin + 1,
        EvDescribeSchemeResponse = EvBegin + 2,

        EvModifySchemeRequest = EvBegin + 3,
        EvModifySchemeResponse = EvBegin + 4,

        EvWaitSchemeTxRequest = EvBegin + 5,
        EvWaitSchemeTxResponse = EvBegin + 6,

        EvBackupPathDescriptionsRequest = EvBegin + 7,
        EvBackupPathDescriptionsResponse = EvBegin + 8,

        EvEnd
    };

    static_assert(EvEnd < (int)TStorageEvents::SS_PROXY_END,
        "EvEnd expected to be < TStorageEvents::SS_PROXY_END");

    STORAGE_SS_PROXY_REQUESTS(STORAGE_DECLARE_EVENTS)
};

}   // namespace NCloud::NStorage
