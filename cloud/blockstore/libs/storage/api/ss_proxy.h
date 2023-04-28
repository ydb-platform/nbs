#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>

#include <library/cpp/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SS_PROXY_REQUESTS(xxx, ...)                                 \
    xxx(CreateVolume,       __VA_ARGS__)                                       \
    xxx(ModifyScheme,       __VA_ARGS__)                                       \
    xxx(ModifyVolume,       __VA_ARGS__)                                       \
    xxx(DescribeScheme,     __VA_ARGS__)                                       \
    xxx(DescribeVolume,     __VA_ARGS__)                                       \
    xxx(WaitSchemeTx,       __VA_ARGS__)                                       \
                                                                               \
    xxx(SyncPathDescriptionCache, __VA_ARGS__)                                 \
// BLOCKSTORE_SS_PROXY_REQUESTS

////////////////////////////////////////////////////////////////////////////////

struct TEvSSProxy
{
    //
    // CreateVolume
    //

    struct TCreateVolumeRequest
    {
        const NKikimrBlockStore::TVolumeConfig VolumeConfig;

        explicit TCreateVolumeRequest(
                NKikimrBlockStore::TVolumeConfig config)
            : VolumeConfig(std::move(config))
        {}
    };

    struct TCreateVolumeResponse
    {
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TCreateVolumeResponse(
                NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
                TString reason = {})
            : Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // ModifyScheme
    //

    struct TModifySchemeRequest
    {
        const NKikimrSchemeOp::TModifyScheme ModifyScheme;

        explicit TModifySchemeRequest(
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
    // ModifyVolume
    //

    struct TModifyVolumeRequest
    {
        enum class EOpType
        {
            Assign,
            Destroy
        };

        const EOpType OpType;
        const TString DiskId;
        const TString NewMountToken;
        const ui64 TokenVersion;

        TModifyVolumeRequest(
                EOpType opType,
                TString diskId,
                TString newMountToken,
                ui64 tokenVersion)
            : OpType(opType)
            , DiskId(std::move(diskId))
            , NewMountToken(std::move(newMountToken))
            , TokenVersion(tokenVersion)
        {}
    };

    struct TModifyVolumeResponse
    {
        const ui64 SchemeShardTabletId;
        const NKikimrScheme::EStatus Status;
        const TString Reason;

        TModifyVolumeResponse(
                ui64 schemeShardTabletId = 0,
                NKikimrScheme::EStatus status = NKikimrScheme::StatusSuccess,
                TString reason = TString())
            : SchemeShardTabletId(schemeShardTabletId)
            , Status(status)
            , Reason(std::move(reason))
        {}
    };

    //
    // DescribeScheme
    //

    struct TDescribeSchemeRequest
    {
        const TString Path;

        explicit TDescribeSchemeRequest(TString path)
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
    // DescribeVolume
    //

    struct TDescribeVolumeRequest
    {
        const TString DiskId;

        explicit TDescribeVolumeRequest(TString diskId)
            : DiskId(std::move(diskId))
        {}
    };

    struct TDescribeVolumeResponse
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;

        TDescribeVolumeResponse() = default;

        TDescribeVolumeResponse(
                TString path,
                NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
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
    // SyncPathDescriptionCache
    //

    struct TSyncPathDescriptionCacheRequest
    {
    };

    struct TSyncPathDescriptionCacheResponse
    {
    };

    //
    // Events declaration
    //

    enum EEvents
    {
        EvBegin = TBlockStoreEvents::SS_PROXY_START,

        EvCreateVolumeRequest = EvBegin + 1,
        EvCreateVolumeResponse = EvBegin + 2,

        EvModifySchemeRequest = EvBegin + 3,
        EvModifySchemeResponse = EvBegin + 4,

        EvModifyVolumeRequest = EvBegin + 5,
        EvModifyVolumeResponse = EvBegin + 6,

        EvDescribeSchemeRequest = EvBegin + 7,
        EvDescribeSchemeResponse = EvBegin + 8,

        EvDescribeVolumeRequest = EvBegin + 9,
        EvDescribeVolumeResponse = EvBegin + 10,

        EvWaitSchemeTxRequest = EvBegin + 11,
        EvWaitSchemeTxResponse = EvBegin + 12,

        EvSyncPathDescriptionCacheRequest = EvBegin + 13,
        EvSyncPathDescriptionCacheResponse = EvBegin + 14,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::SS_PROXY_END,
        "EvEnd expected to be < TBlockStoreEvents::SS_PROXY_END");

    BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeSSProxyServiceId();

}   // namespace NCloud::NBlockStore::NStorage
