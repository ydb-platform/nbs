#pragma once

#include "public.h"

#include "components.h"
#include "events.h"

#include <cloud/storage/core/libs/api/ss_proxy.h>

#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/filestore_config.pb.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SS_PROXY_REQUESTS(xxx, ...)                                  \
    xxx(DescribeFileStore,  __VA_ARGS__)                                       \
    xxx(CreateFileStore,    __VA_ARGS__)                                       \
    xxx(AlterFileStore,     __VA_ARGS__)                                       \
    xxx(DestroyFileStore,   __VA_ARGS__)                                       \
// FILESTORE_SS_PROXY_REQUESTS

////////////////////////////////////////////////////////////////////////////////

using TEvStorageSSProxy = ::NCloud::NStorage::TEvSSProxy;

////////////////////////////////////////////////////////////////////////////////

struct TEvSSProxy
{
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

        EvDescribeFileStoreRequest = EvBegin + 1,
        EvDescribeFileStoreResponse = EvBegin + 2,

        EvCreateFileStoreRequest = EvBegin + 3,
        EvCreateFileStoreResponse = EvBegin + 4,

        EvAlterFileStoreRequest = EvBegin + 5,
        EvAlterFileStoreResponse = EvBegin + 6,

        EvDestroyFileStoreRequest = EvBegin + 7,
        EvDestroyFileStoreResponse = EvBegin + 8,

        EvEnd
    };

    static_assert(EvEnd < (int)TFileStoreEvents::SS_PROXY_END,
        "EvEnd expected to be < TFileStoreEvents::SS_PROXY_END");

    FILESTORE_SS_PROXY_REQUESTS(FILESTORE_DECLARE_EVENTS)
};

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeSSProxyServiceId();

}   // namespace NCloud::NFileStore::NStorage
