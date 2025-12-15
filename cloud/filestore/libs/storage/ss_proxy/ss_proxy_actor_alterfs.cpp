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

class TAlterFileStoreActor final
    : public TActorBootstrapped<TAlterFileStoreActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TActorId StorageSSProxy;
    const NKikimrFileStore::TConfig FileStoreConfig;

public:
    TAlterFileStoreActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TActorId storageSSProxy,
        NKikimrFileStore::TConfig fileStoreConfig);

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

TAlterFileStoreActor::TAlterFileStoreActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TActorId storageSSProxy,
        NKikimrFileStore::TConfig fileStoreConfig)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , StorageSSProxy(std::move(storageSSProxy))
    , FileStoreConfig(std::move(fileStoreConfig))
{}

void TAlterFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    ModifyScheme(ctx);
    Become(&TThis::StateWork);
}

void TAlterFileStoreActor::ModifyScheme(const TActorContext& ctx)
{
    auto path = GetFileSystemPath(
        Config->GetSchemeShardDir(),
        FileStoreConfig.GetFileSystemId());

    auto pathItems = SplitPath(std::move(path));
    Y_ABORT_UNLESS(pathItems);

    auto name = pathItems.back();
    auto workingDir = JoinRange(
        "/",
        pathItems.begin(),
        pathItems.end() - 1);

    NKikimrSchemeOp::TModifyScheme modifyScheme;
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterFileStore);
    modifyScheme.SetWorkingDir(workingDir);

    auto* op = modifyScheme.MutableAlterFileStore();
    op->SetName(name);
    op->MutableConfig()->CopyFrom(FileStoreConfig);

    LOG_DEBUG(ctx, TFileStoreComponents::SS_PROXY,
        "FileStore %s: send alter request (dir: %s, name: %s)",
        FileStoreConfig.GetFileSystemId().Quote().c_str(),
        workingDir.Quote().c_str(),
        name.Quote().c_str());

    auto request = std::make_unique<TEvStorageSSProxy::TEvModifySchemeRequest>(
        std::move(modifyScheme));

    NCloud::Send(ctx, StorageSSProxy, std::move(request));
}

void TAlterFileStoreActor::HandleModifySchemeResponse(
    const TEvStorageSSProxy::TEvModifySchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReplyAndDie(ctx, msg->GetError());
}

void TAlterFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        LOG_ERROR(ctx, TFileStoreComponents::SS_PROXY,
            "FileStore %s: alter failed - %s",
            FileStoreConfig.GetFileSystemId().Quote().c_str(),
            FormatError(error).c_str());
    } else {
        LOG_DEBUG(ctx, TFileStoreComponents::SS_PROXY,
            "FileStore %s: altered successfully",
            FileStoreConfig.GetFileSystemId().Quote().c_str());
    }

    auto response = std::make_unique<TEvSSProxy::TEvAlterFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

STFUNC(TAlterFileStoreActor::StateWork)
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

void TSSProxyActor::HandleAlterFileStore(
    const TEvSSProxy::TEvAlterFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto actor = std::make_unique<TAlterFileStoreActor>(
        std::move(requestInfo),
        Config,
        StorageSSProxy,
        msg->Config);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
