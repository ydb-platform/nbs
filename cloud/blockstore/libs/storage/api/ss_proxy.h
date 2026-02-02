#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/kikimr/events.h>

#include <cloud/storage/core/libs/api/ss_proxy.h>

#include <contrib/ydb/core/protos/flat_scheme_op.pb.h>
#include <contrib/ydb/core/protos/flat_tx_scheme.pb.h>
#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SS_PROXY_REQUESTS(xxx, ...)                                 \
    xxx(CreateVolume,       __VA_ARGS__)                                       \
    xxx(ModifyVolume,       __VA_ARGS__)                                       \
    xxx(DescribeVolume,     __VA_ARGS__)                                       \
                                                                               \
    xxx(BackupPathDescriptions, __VA_ARGS__)                                   \
// BLOCKSTORE_SS_PROXY_REQUESTS

////////////////////////////////////////////////////////////////////////////////

using TEvStorageSSProxy = ::NCloud::NStorage::TEvSSProxy;

////////////////////////////////////////////////////////////////////////////////
struct TEvSSProxy
{
    using TDescribeSchemeRequest =
        ::NCloud::NStorage::TEvSSProxy::TDescribeSchemeRequest;
    using TDescribeSchemeResponse =
        ::NCloud::NStorage::TEvSSProxy::TDescribeSchemeResponse;

    using TModifySchemeRequest =
        ::NCloud::NStorage::TEvSSProxy::TModifySchemeRequest;
    using TModifySchemeResponse =
        ::NCloud::NStorage::TEvSSProxy::TModifySchemeResponse;

    using TWaitSchemeTxRequest =
        ::NCloud::NStorage::TEvSSProxy::TWaitSchemeTxRequest;
    using TWaitSchemeTxResponse =
        ::NCloud::NStorage::TEvSSProxy::TWaitSchemeTxResponse;

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

        const ui64 FillGeneration;

        TModifyVolumeRequest(
                EOpType opType,
                TString diskId,
                TString newMountToken,
                ui64 tokenVersion,
                ui64 fillGeneration = 0)
            : OpType(opType)
            , DiskId(std::move(diskId))
            , NewMountToken(std::move(newMountToken))
            , TokenVersion(tokenVersion)
            , FillGeneration(fillGeneration)
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
    // DescribeVolume
    //

    struct TDescribeVolumeRequest
    {
        const TString DiskId;
        const bool ExactDiskIdMatch = false;

        explicit TDescribeVolumeRequest(TString diskId)
            : DiskId(std::move(diskId))
        {}

        TDescribeVolumeRequest(TString diskId, bool exactDiskIdMatch)
            : DiskId(std::move(diskId))
            , ExactDiskIdMatch(exactDiskIdMatch)
        {}
    };

    struct TDescribeVolumeResponse
    {
        const TString Path;
        const NKikimrSchemeOp::TPathDescription PathDescription;
        const TVector<TString> CheckedPaths;

        TDescribeVolumeResponse() = default;

        TDescribeVolumeResponse(
                TString path,
                NKikimrSchemeOp::TPathDescription pathDescription)
            : Path(std::move(path))
            , PathDescription(std::move(pathDescription))
        {}

        TDescribeVolumeResponse(TString path, TVector<TString> checkedPaths)
            : Path(std::move(path))
            , CheckedPaths(std::move(checkedPaths))
        {}

        TDescribeVolumeResponse(TString path)
            : Path(std::move(path))
        {}
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
        EvDescribeSchemeRequest =
            ::NCloud::NStorage::TEvSSProxy::EEvents::EvDescribeSchemeRequest,
        EvDescribeSchemeResponse =
            ::NCloud::NStorage::TEvSSProxy::EEvents::EvDescribeSchemeResponse,

        EvModifySchemeRequest =
            ::NCloud::NStorage::TEvSSProxy::EEvents::EvModifySchemeRequest,
        EvModifySchemeResponse =
            ::NCloud::NStorage::TEvSSProxy::EEvents::EvModifySchemeResponse,

        EvWaitSchemeTxRequest =
            ::NCloud::NStorage::TEvSSProxy::EEvents::EvWaitSchemeTxRequest,
        EvWaitSchemeTxResponse =
            ::NCloud::NStorage::TEvSSProxy::EEvents::EvWaitSchemeTxResponse,

        EvBegin = TBlockStoreEvents::SS_PROXY_START,

        EvCreateVolumeRequest = EvBegin + 1,
        EvCreateVolumeResponse = EvBegin + 2,

        EvModifyVolumeRequest = EvBegin + 3,
        EvModifyVolumeResponse = EvBegin + 4,

        EvDescribeVolumeRequest = EvBegin + 5,
        EvDescribeVolumeResponse = EvBegin + 6,

        EvBackupPathDescriptionsRequest = EvBegin + 7,
        EvBackupPathDescriptionsResponse = EvBegin + 8,

        EvEnd
    };

    static_assert(EvEnd < (int)TBlockStoreEvents::SS_PROXY_END,
        "EvEnd expected to be < TBlockStoreEvents::SS_PROXY_END");

    BLOCKSTORE_SS_PROXY_REQUESTS(BLOCKSTORE_DECLARE_EVENTS)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeSSProxyServiceId();

}   // namespace NCloud::NBlockStore::NStorage
