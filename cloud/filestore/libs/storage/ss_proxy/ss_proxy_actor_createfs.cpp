#include "ss_proxy_actor.h"

#include "path.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <contrib/ydb/core/base/path.h>
#include <contrib/ydb/core/protos/schemeshard/operations.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/vector.h>
#include <util/string/join.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCreateFileStoreActor final
    : public TActorBootstrapped<TCreateFileStoreActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TActorId StorageSSProxy;
    const NKikimrFileStore::TConfig FileStoreConfig;

    TVector<TString> FileStorePathItems;
    bool FirstCreateAttempt = true;
    size_t NextItemToCreate = 0;

public:
    TCreateFileStoreActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TActorId storageSSProxy,
        NKikimrFileStore::TConfig fileStoreConfig);

    void Bootstrap(const TActorContext& ctx);

private:
    void CreateFileStore(const TActorContext& ctx);
    void HandleCreateFileStoreResponse(
        const TEvStorageSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void EnsureDirs(const TActorContext& ctx);
    void CreateDir(const TActorContext& ctx);
    void HandleCreateDirResponse(
        const TEvStorageSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    STFUNC(StateCreateFileStore);
    STFUNC(StateCreateDir);
};

////////////////////////////////////////////////////////////////////////////////

TCreateFileStoreActor::TCreateFileStoreActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TActorId storageSSProxy,
        NKikimrFileStore::TConfig fileStoreConfig)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , StorageSSProxy(std::move(storageSSProxy))
    , FileStoreConfig(std::move(fileStoreConfig))
{}

void TCreateFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    auto path = GetFileSystemPath(
        Config->GetSchemeShardDir(),
        FileStoreConfig.GetFileSystemId());

    FileStorePathItems = SplitPath(std::move(path));
    Y_ABORT_UNLESS(FileStorePathItems);

    CreateFileStore(ctx);
}

void TCreateFileStoreActor::CreateFileStore(const TActorContext& ctx)
{
    Become(&TThis::StateCreateFileStore);

    auto name = FileStorePathItems.back();
    auto workingDir = JoinRange(
        "/",
        FileStorePathItems.begin(),
        FileStorePathItems.end() - 1);

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateFileStore);
    modifyScheme.SetWorkingDir(workingDir);

    auto* op = modifyScheme.MutableCreateFileStore();
    op->SetName(name);
    op->MutableConfig()->CopyFrom(FileStoreConfig);

    LOG_DEBUG(ctx, TFileStoreComponents::SS_PROXY,
        "FileStore %s: send create request (dir: %s, name: %s)",
        FileStoreConfig.GetFileSystemId().Quote().c_str(),
        workingDir.Quote().c_str(),
        name.Quote().c_str());

    auto request = std::make_unique<TEvStorageSSProxy::TEvModifySchemeRequest>(
        std::move(modifyScheme));

    NCloud::Send(ctx, StorageSSProxy, std::move(request));
}

void TCreateFileStoreActor::HandleCreateFileStoreResponse(
    const TEvStorageSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (FACILITY_FROM_CODE(error.GetCode()) == FACILITY_SCHEMESHARD) {
        auto status = (NKikimrScheme::EStatus)STATUS_FROM_CODE(error.GetCode());
        if (status == NKikimrScheme::StatusPathDoesNotExist) {
            if (FirstCreateAttempt) {
                FirstCreateAttempt = false;

                // Try creating intermediate directories
                EnsureDirs(ctx);
            }
        }
    }

    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TFileStoreComponents::SS_PROXY,
            "FileStore %s: create failed - %s",
            FileStoreConfig.GetFileSystemId().Quote().c_str(),
            FormatError(error).c_str());

        ReplyAndDie(ctx, error);
        return;
    }

    LOG_INFO(ctx, TFileStoreComponents::SS_PROXY,
        "FileStore %s: created successfully",
        FileStoreConfig.GetFileSystemId().Quote().c_str());

    ReplyAndDie(ctx);
}

void TCreateFileStoreActor::EnsureDirs(const TActorContext& ctx)
{
    if (NextItemToCreate < FileStorePathItems.size()) {
        CreateDir(ctx);
    } else {
        CreateFileStore(ctx);
    }
}

void TCreateFileStoreActor::CreateDir(const TActorContext& ctx)
{
    Become(&TThis::StateCreateDir);

    auto name = FileStorePathItems[NextItemToCreate];
    auto workingDir = JoinRange(
        "/",
        FileStorePathItems.begin(),
        FileStorePathItems.begin() + NextItemToCreate);

    LOG_DEBUG(ctx, TFileStoreComponents::SS_PROXY,
        "FileStore %s: send mkdir request (dir: %s, name: %s)",
        FileStoreConfig.GetFileSystemId().Quote().c_str(),
        workingDir.Quote().c_str(),
        name.Quote().c_str());

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpMkDir);
    modifyScheme.SetWorkingDir(workingDir);

    auto* op = modifyScheme.MutableMkDir();
    op->SetName(name);

    auto request = std::make_unique<TEvStorageSSProxy::TEvModifySchemeRequest>(
        std::move(modifyScheme));

    NCloud::Send(ctx, StorageSSProxy, std::move(request));
}

void TCreateFileStoreActor::HandleCreateDirResponse(
    const TEvStorageSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TFileStoreComponents::SS_PROXY,
            "FileStore %s: mkdir failed - ",
            FileStoreConfig.GetFileSystemId().Quote().c_str(),
            FormatError(error).c_str());

        ReplyAndDie(ctx, error);
        return;
    }

    ++NextItemToCreate;
    EnsureDirs(ctx);
}

void TCreateFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvSSProxy::TEvCreateFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

STFUNC(TCreateFileStoreActor::StateCreateFileStore)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStorageSSProxy::TEvModifySchemeResponse, HandleCreateFileStoreResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TCreateFileStoreActor::StateCreateDir)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStorageSSProxy::TEvModifySchemeResponse, HandleCreateDirResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SS_PROXY,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleCreateFileStore(
    const TEvSSProxy::TEvCreateFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto actor = std::make_unique<TCreateFileStoreActor>(
        std::move(requestInfo),
        Config,
        StorageSSProxy,
        msg->Config);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
