#include "service_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

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
    const TStorageConfigPtr StorageConfig;
    const TRequestInfoPtr RequestInfo;
    const TString FileSystemId;
    const bool ForceDestroy;
    const bool AllowFileStoreDestroyWithOrphanSessions;
    TDuration RestartTabletUptimeThresholdDuringDestroy;
    TVector<TString> ShardIds;
    ui32 NextShardToDestroy = 0;
    ui32 DestroyedShardCount = 0;
    ui64 ForceDestroySizeThreshold = 0;
    NProto::TFileStore FileStore;
    TDuration TabletUptime;
    NProto::TError ActiveSessionsError;

public:
    TDestroyFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        bool forceDestroy,
        bool allowFileStoreDestroyWithOrphanSessions,
        ui64 forceDestroySizeThreshold,
        TDuration restartTabletUptimeThresholdDuringDestroy);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void DescribeFileStore(const TActorContext& ctx);
    void GetStorageStats(const TActorContext& ctx);
    void DescribeSessions(const TActorContext& ctx);
    void RestartTablet(const TActorContext& ctx);
    void GetFileSystemTopology(const TActorContext& ctx);
    void DestroyShards(const TActorContext& ctx);
    void DestroyShard(const TActorContext& ctx, const ui32 shardIndex);
    void DestroyFileStore(const TActorContext& ctx);

    void HandleDescribeFileStoreResponse(
        const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleGetStorageStatsResponse(
        const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleDescribeSessionsResponse(
        const TEvIndexTablet::TEvDescribeSessionsResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleRestartTabletResponse(
        const TEvIndexTablet::TEvRestartTabletResponse::TPtr& ev,
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

    ui32 GetShardManagementRequestsInFlightLimit() const
    {
        ui32 limit =
            !StorageConfig->GetShardManagementRequestThrottlingEnabled()
                ? Max<ui32>()
                : StorageConfig->GetMaxShardManagementRequestsInFlight();
        Y_ABORT_UNLESS(limit > 0);
        return limit;
    }
};

////////////////////////////////////////////////////////////////////////////////

TDestroyFileStoreActor::TDestroyFileStoreActor(
        TStorageConfigPtr storageConfig,
        TRequestInfoPtr requestInfo,
        TString fileSystemId,
        bool forceDestroy,
        bool allowFileStoreDestroyWithOrphanSessions,
        ui64 forceDestroySizeThreshold,
        TDuration restartTabletUptimeThresholdDuringDestroy)
    : StorageConfig(std::move(storageConfig))
    , RequestInfo(std::move(requestInfo))
    , FileSystemId(std::move(fileSystemId))
    , ForceDestroy(forceDestroy)
    , AllowFileStoreDestroyWithOrphanSessions(
        allowFileStoreDestroyWithOrphanSessions)
    , RestartTabletUptimeThresholdDuringDestroy(
        restartTabletUptimeThresholdDuringDestroy)
    , ForceDestroySizeThreshold(forceDestroySizeThreshold)
{}

void TDestroyFileStoreActor::Bootstrap(const TActorContext& ctx)
{
    DescribeFileStore(ctx);
    Become(&TThis::StateWork);
}

////////////////////////////////////////////////////////////////////////////////

void TDestroyFileStoreActor::DescribeFileStore(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvSSProxy::TEvDescribeFileStoreRequest>(
        FileSystemId);

    NCloud::Send(ctx, MakeSSProxyServiceId(), std::move(request));
}

void TDestroyFileStoreActor::HandleDescribeFileStoreResponse(
    const TEvSSProxy::TEvDescribeFileStoreResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        if (msg->GetStatus() == E_NOT_FOUND) {
            ReplyAndDie(
                ctx,
                MakeError(S_FALSE, FileSystemId.Quote() + " does not exist"));
            return;
        }

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    const auto& fileStore = msg->PathDescription.GetFileStoreDescription();
    const auto& config = fileStore.GetConfig();

    Convert(config, FileStore);

    auto bytesSize = FileStore.GetBlockSize() * FileStore.GetBlocksCount();
    bool isBelowForceDestroyThreshold = bytesSize <= ForceDestroySizeThreshold;
    if (isBelowForceDestroyThreshold) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] filestore size(%u bytes) less than "
            "ForceDestroySizeThreshold(%u bytes), force destroying",
            FileSystemId.c_str(),
            bytesSize,
            ForceDestroySizeThreshold);
    }

    if (ForceDestroy || isBelowForceDestroyThreshold) {
        GetFileSystemTopology(ctx);
    } else {
        GetStorageStats(ctx);
    }
}

void TDestroyFileStoreActor::GetStorageStats(const TActorContext& ctx)
{
    auto request =
        std::make_unique<TEvIndexTablet::TEvGetStorageStatsRequest>();
    request->Record.SetAllowCache(true);
    request->Record.SetFileSystemId(FileSystemId);
    request->Record.SetMode(NProtoPrivate::STATS_REQUEST_MODE_GET_ONLY_SELF);

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TDestroyFileStoreActor::HandleGetStorageStatsResponse(
    const TEvIndexTablet::TEvGetStorageStatsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        if (msg->GetStatus() == E_NOT_FOUND) {
            ReplyAndDie(
                ctx,
                MakeError(S_FALSE, FileSystemId.Quote() + " does not exist"));
            return;
        }

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    TabletUptime = TDuration::MilliSeconds(msg->Record.GetTabletUptimeMs());
    DescribeSessions(ctx);
}

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
        if (msg->GetStatus() == E_NOT_FOUND) {
            ReplyAndDie(
                ctx,
                MakeError(S_FALSE, FileSystemId.Quote() + " does not exist"));
            return;
        }

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    bool haveSessions = msg->Record.SessionsSize() != 0;
    bool haveNonOrphanSessions = false;
    for (const auto& s: msg->Record.GetSessions()) {
        if (!s.GetIsOrphan()) {
            haveNonOrphanSessions = true;
            break;
        }
    }

    if (AllowFileStoreDestroyWithOrphanSessions) {
        haveSessions = haveNonOrphanSessions;
    }

    if (haveSessions) {
        TStringBuilder message;
        message << "FileStore " << FileSystemId.Quote()
                << " has active sessions with client ids:";
        for (const auto& sessionInfo: msg->Record.GetSessions()) {
            message << " " << sessionInfo.GetClientId();
        }

        if (RestartTabletUptimeThresholdDuringDestroy &&
            TabletUptime > RestartTabletUptimeThresholdDuringDestroy &&
            haveNonOrphanSessions)
        {
            ActiveSessionsError = MakeError(E_REJECTED, message);
            RestartTablet(ctx);
            return;
        }

        ReplyAndDie(ctx, MakeError(E_REJECTED, message));
        return;
    }

    GetFileSystemTopology(ctx);
}

void TDestroyFileStoreActor::RestartTablet(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTablet::TEvRestartTabletRequest>();
    request->Record.SetFileSystemId(FileSystemId);

    LOG_INFO(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Restarting the index tablet before destroying the filesystem"
        " because the tablet may have false active sessions,"
        " uptime is greater than %lu ms",
        FileSystemId.c_str(),
        RestartTabletUptimeThresholdDuringDestroy.MilliSeconds());

    NCloud::Send(ctx, MakeIndexTabletProxyServiceId(), std::move(request));
}

void TDestroyFileStoreActor::HandleRestartTabletResponse(
    const TEvIndexTablet::TEvRestartTabletResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    if (HasError(msg->GetError())) {
        LOG_WARN(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] RestartTablet error: %s",
            FileSystemId.c_str(),
            FormatError(msg->GetError()).Quote().c_str());

        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    ReplyAndDie(ctx, ActiveSessionsError);
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
    const ui32 endShardIndex = std::min<ui32>(
        GetShardManagementRequestsInFlightLimit(),
        ShardIds.size());
    for (ui32 i = 0; i < endShardIndex; ++i) {
        DestroyShard(ctx, i);
        NextShardToDestroy = i + 1;
    }
}

void TDestroyFileStoreActor::DestroyShard(
    const TActorContext& ctx,
    const ui32 shardIndex)
{
    auto request = std::make_unique<TEvSSProxy::TEvDestroyFileStoreRequest>(
        ShardIds[shardIndex]);

    NCloud::Send(
        ctx,
        MakeSSProxyServiceId(),
        std::move(request),
        shardIndex + 1);
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
        } else if (StorageConfig->GetShardManagementRequestThrottlingEnabled())
        {
            if (NextShardToDestroy < ShardIds.size()) {
                DestroyShard(ctx, NextShardToDestroy);
                ++NextShardToDestroy;
            }
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
            TEvSSProxy::TEvDescribeFileStoreResponse,
            HandleDescribeFileStoreResponse);

        HFunc(
            TEvIndexTablet::TEvGetStorageStatsResponse,
            HandleGetStorageStatsResponse);

        HFunc(
            TEvIndexTablet::TEvDescribeSessionsResponse,
            HandleDescribeSessionsResponse);

        HFunc(
            TEvIndexTablet::TEvRestartTabletResponse,
            HandleRestartTabletResponse);

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

    InitProfileLogRequestInfo(inflight->AccessProfileLogRequest(), msg->Record);

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
    bool allowDestroyWithOrphanSessions =
        StorageConfig->GetAllowFileStoreDestroyWithOrphanSessions();
    if (StorageConfig->GetRestartTabletUptimeThresholdDuringDestroy()) {
        allowDestroyWithOrphanSessions = false;
    }
    auto actor = std::make_unique<TDestroyFileStoreActor>(
        StorageConfig,
        std::move(requestInfo),
        msg->Record.GetFileSystemId(),
        forceDestroy,
        allowDestroyWithOrphanSessions,
        StorageConfig->GetForceDestroySizeThreshold(),
        StorageConfig->GetRestartTabletUptimeThresholdDuringDestroy());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
