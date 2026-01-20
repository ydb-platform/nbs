#include "ss_proxy_actor.h"

#include "path.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <contrib/ydb/core/base/path.h>
#include <contrib/ydb/core/protos/schemeshard/operations.pb.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDestroyFileStoreActor final
    : public TActorBootstrapped<TDestroyFileStoreActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TActorId StorageSSProxy;
    const TString FileSystemId;

public:
    TDestroyFileStoreActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TActorId storageSSProxy,
        TString fileSystemId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ModifyScheme(const TActorContext& ctx);
    void HandleModifySchemeResponse(
        const TEvStorageSSProxy::TEvModifySchemeResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TDestroyFileStoreActor::TDestroyFileStoreActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TActorId storageSSProxy,
        TString fileSystemId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , StorageSSProxy(std::move(storageSSProxy))
    , FileSystemId(std::move(fileSystemId))
{}

void TDestroyFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    ModifyScheme(ctx);
    Become(&TThis::StateWork);
}

void TDestroyFileStoreActor::ModifyScheme(const TActorContext& ctx)
{
    auto path = GetFileSystemPath(
        Config->GetSchemeShardDir(),
        FileSystemId);

    auto pathItems = SplitPath(std::move(path));
    Y_ABORT_UNLESS(pathItems);

    auto name = pathItems.back();
    auto workingDir = JoinRange(
        "/",
        pathItems.begin(),
        pathItems.end() - 1);

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropFileStore);
    modifyScheme.SetWorkingDir(TString(workingDir));

    auto* op = modifyScheme.MutableDrop();
    op->SetName(TString(name));

    LOG_DEBUG(ctx, TFileStoreComponents::SS_PROXY,
        "FileStore %s: send drop request (dir: %s, name: %s)",
        FileSystemId.Quote().c_str(),
        workingDir.Quote().c_str(),
        name.Quote().c_str());

    auto request = std::make_unique<TEvStorageSSProxy::TEvModifySchemeRequest>(
        std::move(modifyScheme));

    NCloud::Send(ctx, StorageSSProxy, std::move(request));
}

void TDestroyFileStoreActor::HandleModifySchemeResponse(
    const TEvStorageSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    NProto::TError error = ev->Get()->GetError();
    if (error.GetCode() == MAKE_SCHEMESHARD_ERROR(NKikimrScheme::EStatus::StatusPathDoesNotExist)) {
        error = MakeError(S_FALSE, FileSystemId.Quote() + " does not exist");
    }

    ReplyAndDie(ctx, error);
}

void TDestroyFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TFileStoreComponents::SS_PROXY,
            "FileStore %s: drop failed - %s",
            FileSystemId.Quote().c_str(),
            FormatError(error).c_str());
    } else {
        LOG_DEBUG(ctx, TFileStoreComponents::SS_PROXY,
            "FileStore %s: dropped successfully",
            FileSystemId.Quote().c_str());
    }

    auto response = std::make_unique<TEvSSProxy::TEvDestroyFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

STFUNC(TDestroyFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStorageSSProxy::TEvModifySchemeResponse, HandleModifySchemeResponse);

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

void TSSProxyActor::HandleDestroyFileStore(
    const TEvSSProxy::TEvDestroyFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto actor = std::make_unique<TDestroyFileStoreActor>(
        std::move(requestInfo),
        Config,
        StorageSSProxy,
        msg->FileSystemId);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
