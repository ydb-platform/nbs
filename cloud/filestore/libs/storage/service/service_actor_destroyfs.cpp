#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

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
    const TString FileSystemId;
    const bool ForceDestroy;
    const bool AllowFileStoreDestroyWithOrphanSessions;
    TVector<TString> ShardIds;
    ui32 DestroyedShardCount = 0;

public:
    TDestroyFileStoreActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        bool forceDestroy,
        bool allowFileStoreDestroyWithOrphanSessions);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeSessions(const TActorContext& ctx);
    void GetFileSystemTopology(const TActorContext& ctx);
    void DestroyShards(const TActorContext& ctx);
    void DestroyFileStore(const TActorContext& ctx);

    void HandleDescribeSessionsResponse(
        const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetFileSystemTopologyResponse(
        const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDestroyFileStoreResponse(
        const TEvSSProxy::TEvDestroyFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TDestroyFileStoreActor::TDestroyFileStoreActor(
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        bool forceDestroy,
        bool allowFileStoreDestroyWithOrphanSessions)
    : RequestInfo(std::move(requestInfo))
    , FileSystemId(std::move(fileSystemId))
    , ForceDestroy(forceDestroy)
    , AllowFileStoreDestroyWithOrphanSessions(
        allowFileStoreDestroyWithOrphanSessions)
{}

void TDestroyFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    if (ForceDestroy) {
        GetFileSystemTopology(ctx);
    } else {
        DescribeSessions(ctx);
    }
    Become(&TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

void TDestroyFileStoreActor::DescribeSessions(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvDescribeSessionsRequest>();
    request->Record.SetFileSystemId(FileSystemId);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

void TDestroyFileStoreActor::HandleDescribeSessionsResponse(
    const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        if (msg->GetStatus() ==
            MAKE_SCHEMESHARD_ERROR(
                NKikimrScheme::EStatus::StatusPathDoesNotExist))
        {
            ReplyAndDie(
                ctx,
                MakeError(S_FALSE, FileSystemId.Quote() + " does not exist"));
            return;
        }

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    bool haveSessions = msg->Record.SessionsSize() != 0;
    if (AllowFileStoreDestroyWithOrphanSessions) {
        haveSessions = false;
        for (const auto& s: msg->Record.GetSessions()) {
            if (!s.GetIsOrphan()) {
                haveSessions = true;
                break;
            }
        }
    }

    if (haveSessions) {
        TStringBuilder message;
        message << "FileStore has active sessions with client ids:";
        for (const auto& sessionInfo: msg->Record.GetSessions()) {
            message << " " << sessionInfo.GetClientId();
        }
        ReplyAndDie(ctx, MakeError(E_REJECTED, message));
        return;
    }

    GetFileSystemTopology(ctx);
}

void TDestroyFileStoreActor::GetFileSystemTopology(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvGetFileSystemTopologyRequest>();
    request->Record.SetFileSystemId(FileSystemId);

    NCloud::Send(
        ctx,
        MakeIndexTabletProxyServiceId(),
        std::move(request));
}

void TDestroyFileStoreActor::HandleGetFileSystemTopologyResponse(
    const TEvIndexTablet::TEvGetFileSystemTopologyResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] GetFileSystemTopology error: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] DestroyFileStore GetFileSystemTopology response: %s",
        FileSystemId.c_str(),
        msg->Record.Utf8DebugString().Quote().c_str());

    for (auto& shardId: *msg->Record.MutableShardFileSystemIds()) {
        ShardIds.push_back(std::move(shardId));
    }

    if (ShardIds) {
        DestroyShards(ctx);
    } else {
        DestroyFileStore(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TDestroyFileStoreActor::DestroyShards(const TActorContext& ctx)
{
    for (ui32 i = 0; i < ShardIds.size(); ++i) {
        auto request = std::make_unique<TEvSSProxy::TEvDestroyFileStoreRequest>(
            ShardIds[i]);

        NCloud::Send(
            ctx,
            MakeSSProxyServiceId(),
            std::move(request),
            i + 1);
    }
}

void TDestroyFileStoreActor::DestroyFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvDestroyFileStoreRequest>(
        FileSystemId);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TDestroyFileStoreActor::HandleDestroyFileStoreResponse(
    const TEvSSProxy::TEvDestroyFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    Y_DEBUG_ABORT_UNLESS(ev->Cookie <= ShardIds.size());

    const auto* msg = ev->Get();
    TString fsId = "<undefined>";
    if (ev->Cookie == 0) {
        fsId = FileSystemId;
    } else if (ev->Cookie <= ShardIds.size()) {
        fsId = ShardIds[ev->Cookie - 1];
    }

    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] DestroyFileStore error for %s, %s",
            FileSystemId.c_str(),
            fsId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] DestroyFileStore success for %s",
        FileSystemId.c_str(),
        fsId.c_str());

    if (ev->Cookie) {
        ++DestroyedShardCount;
        Y_DEBUG_ABORT_UNLESS(DestroyedShardCount <= ShardIds.size());

        if (DestroyedShardCount == ShardIds.size()) {
            DestroyFileStore(ctx);
        }

        return;
    }

    ReplyAndDie(ctx, msg->GetError());
}

////////////////////////////////////////////////////////////////////////////////

void TDestroyFileStoreActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TDestroyFileStoreActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvService::TEvDestroyFileStoreResponse>(error);
    NCloud::Reply(ctx, *RequestInfo, std::move(response));

    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TDestroyFileStoreActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvDescribeSessionsResponse,
            HandleDescribeSessionsResponse);

        HFunc(
            TEvIndexTablet::TEvGetFileSystemTopologyResponse,
            HandleGetFileSystemTopologyResponse);

        HFunc(
            TEvSSProxy::TEvDestroyFileStoreResponse,
            HandleDestroyFileStoreResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleDestroyFileStore(
    const TEvService::TEvDestroyFileStoreRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        NProto::EStorageMediaKind::STORAGE_MEDIA_DEFAULT,
        StatsRegistry->GetRequestStats(),
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(
        SelfId(),
        cookie,
        msg->CallContext);

    if (Count(
            StorageConfig->GetDestroyFilestoreDenyList(),
            msg->Record.GetFileSystemId()))
    {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "FileStore %s is in deny list, responding with S_FALSE",
            msg->Record.GetFileSystemId().c_str());
        auto response =
            std::make_unique<TEvService::TEvDestroyFileStoreResponse>(MakeError(
                S_FALSE,
                Sprintf(
                    "FileStore %s is in deny list",
                    msg->Record.GetFileSystemId().c_str())));
        NCloud::Reply(ctx, *requestInfo, std::move(response));
        return;
    }

    bool forceDestroy = msg->Record.GetForceDestroy() &&
                        StorageConfig->GetAllowFileStoreForceDestroy();
    auto actor = std::make_unique<TDestroyFileStoreActor>(
        std::move(requestInfo),
        msg->Record.GetFileSystemId(),
        forceDestroy,
        StorageConfig->GetAllowFileStoreDestroyWithOrphanSessions());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
