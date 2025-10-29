#include "ss_proxy_actor.h"

#include "path.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/core/config.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

const TDuration DescribeTimeout = TDuration::Seconds(20);

////////////////////////////////////////////////////////////////////////////////

class TDescribeFileStoreActor final
    : public TActorBootstrapped<TDescribeFileStoreActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TStorageConfigPtr Config;
    const TActorId StorageSSProxy;
    const TString FileSystemId;

public:
    TDescribeFileStoreActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TActorId storageSSProxy,
        TString fileSystemId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeScheme(const TActorContext& ctx);
    void HandleDescribeSchemeResponse(
        const TEvStorageSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
        const TActorContext& ctx);
    void HandleWakeup(
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TEvSSProxy::TEvDescribeFileStoreResponse> response);
};

////////////////////////////////////////////////////////////////////////////////

TDescribeFileStoreActor::TDescribeFileStoreActor(
        TRequestInfoPtr requestInfo,
        TStorageConfigPtr config,
        TActorId storageSSProxy,
        TString fileSystemId)
    : RequestInfo(std::move(requestInfo))
    , Config(std::move(config))
    , StorageSSProxy(std::move(storageSSProxy))
    , FileSystemId(std::move(fileSystemId))
{}

void TDescribeFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    ctx.Schedule(DescribeTimeout, new TEvents::TEvWakeup());

    DescribeScheme(ctx);
    Become(&TThis::StateWork);
}

void TDescribeFileStoreActor::DescribeScheme(const TActorContext& ctx)
{
    auto path = GetFileSystemPath(
        Config->GetSchemeShardDir(),
        FileSystemId);

    auto request = std::make_unique<TEvStorageSSProxy::TEvDescribeSchemeRequest>(path);
    NCloud::Send(ctx, StorageSSProxy, std::move(request));
}

void TDescribeFileStoreActor::HandleDescribeSchemeResponse(
    const TEvStorageSSProxy::TEvDescribeSchemeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& error = msg->GetError();

    if (FAILED(error.GetCode())) {
        ReplyAndDie(ctx, error);
        return;
    }

    const auto pathType = msg->PathDescription.GetSelf().GetPathType();

    auto path = GetFileSystemPath(
        Config->GetSchemeShardDir(),
        FileSystemId);

    if (pathType != NKikimrSchemeOp::EPathTypeFileStore) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_INVALID_STATE,
                TStringBuilder()
                    << "Described path is not a filestore: "
                    << path.Quote()));
        return;
    }

    // Zero IndexTabletId means that tablet is not configured by Hive yet.
    if (!msg->PathDescription.GetFileStoreDescription().GetIndexTabletId()) {
        ReplyAndDie(
            ctx,
            MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "Filestore volume " << path.Quote()
                    << " has zero IndexTabletId"));
        return;
    }

    auto response = std::make_unique<TEvSSProxy::TEvDescribeFileStoreResponse>(
        msg->Path,
        msg->PathDescription);

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeFileStoreActor::HandleWakeup(const TActorContext& ctx)
{
    LOG_ERROR(ctx, TFileStoreComponents::SS_PROXY,
        "Describe request timed out for filesystem %s",
        FileSystemId.c_str());

    auto response = std::make_unique<TEvSSProxy::TEvDescribeFileStoreResponse>(
        MakeError(E_TIMEOUT, "DescribeFileStore timeout for the filesystem"));

    ReplyAndDie(ctx, std::move(response));
}

void TDescribeFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response = std::make_unique<TEvSSProxy::TEvDescribeFileStoreResponse>(error);
    ReplyAndDie(ctx, std::move(response));
}

void TDescribeFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TEvSSProxy::TEvDescribeFileStoreResponse> response)
{
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

STFUNC(TDescribeFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvStorageSSProxy::TEvDescribeSchemeResponse, HandleDescribeSchemeResponse);

        CFunc(TEvents::TSystem::Wakeup, HandleWakeup)

            default
            : HandleUnexpectedEvent(
                  ev,
                  TFileStoreComponents::SS_PROXY,
                  __PRETTY_FUNCTION__);
        break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TSSProxyActor::HandleDescribeFileStore(
    const TEvSSProxy::TEvDescribeFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    auto actor = std::make_unique<TDescribeFileStoreActor>(
        std::move(requestInfo),
        Config,
        StorageSSProxy,
        msg->FileSystemId);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
