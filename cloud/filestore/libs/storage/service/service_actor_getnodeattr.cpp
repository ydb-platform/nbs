#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TGetNodeAttrActor final: public TActorBootstrapped<TGetNodeAttrActor>
{
private:
    // Original request
    const TRequestInfoPtr RequestInfo;
    NProto::TGetNodeAttrRequest GetNodeAttrRequest;

    // Filesystem-specific params
    const TString LogTag;

    // Response data
    NProto::TGetNodeAttrResponse LeaderResponse;
    bool LeaderResponded = false;

    // Stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;

    const bool MultiTabletForwardingEnabled;

public:
    TGetNodeAttrActor(
        TRequestInfoPtr requestInfo,
        NProto::TGetNodeAttrRequest getNodeAttrRequest,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        bool multiTabletForwardingEnabled);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void GetNodeAttrInLeader(const TActorContext& ctx);

    void HandleGetNodeAttrResponse(
        const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
        const TActorContext& ctx);

    void GetNodeAttrInShard(const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TGetNodeAttrResponse shardResponse);
    void HandleError(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TGetNodeAttrActor::TGetNodeAttrActor(
        TRequestInfoPtr requestInfo,
        NProto::TGetNodeAttrRequest getNodeAttrRequest,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        bool multiTabletForwardingEnabled)
    : RequestInfo(std::move(requestInfo))
    , GetNodeAttrRequest(std::move(getNodeAttrRequest))
    , LogTag(GetNodeAttrRequest.GetFileSystemId())
    , RequestStats(std::move(requestStats))
    , ProfileLog(std::move(profileLog))
    , MultiTabletForwardingEnabled(multiTabletForwardingEnabled)
{
}

void TGetNodeAttrActor::Bootstrap(const TActorContext& ctx)
{
    GetNodeAttrInLeader(ctx);
    Become(&TThis::StateWork);
}

void TGetNodeAttrActor::GetNodeAttrInLeader(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Executing GetNodeAttr in leader for %lu, %s",
        LogTag.c_str(),
        GetNodeAttrRequest.GetNodeId(),
        GetNodeAttrRequest.GetName().Quote().c_str());

    auto request = std::make_unique<TEvService::TEvGetNodeAttrRequest>();
    request->Record = GetNodeAttrRequest;

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TGetNodeAttrActor::GetNodeAttrInShard(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Executing GetNodeAttr in shard for %lu, %s",
        LogTag.c_str(),
        GetNodeAttrRequest.GetNodeId(),
        GetNodeAttrRequest.GetName().Quote().c_str());

    auto request = std::make_unique<TEvService::TEvGetNodeAttrRequest>();
    request->Record = GetNodeAttrRequest;
    request->Record.SetFileSystemId(
        LeaderResponse.GetNode().GetShardFileSystemId());
    request->Record.SetNodeId(RootNodeId);
    request->Record.SetName(LeaderResponse.GetNode().GetShardNodeName());

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

////////////////////////////////////////////////////////////////////////////////

void TGetNodeAttrActor::HandleGetNodeAttrResponse(
    const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, *msg->Record.MutableError());
        return;
    }

    if (LeaderResponded) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "GetNodeAttr succeeded in shard: %lu",
            msg->Record.GetNode().GetId());

        ReplyAndDie(ctx, std::move(msg->Record));
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "GetNodeAttr succeeded in leader: %s %s",
        msg->Record.GetNode().GetShardFileSystemId().c_str(),
        msg->Record.GetNode().GetShardNodeName().Quote().c_str());

    if (!MultiTabletForwardingEnabled
            || msg->Record.GetNode().GetShardFileSystemId().empty())
    {
        ReplyAndDie(ctx, std::move(msg->Record));
        return;
    }

    LeaderResponded = true;
    LeaderResponse = std::move(msg->Record);
    GetNodeAttrInShard(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TGetNodeAttrActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TGetNodeAttrActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TGetNodeAttrResponse shardResponse)
{
    auto response = std::make_unique<TEvService::TEvGetNodeAttrResponse>();
    response->Record = std::move(shardResponse);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TGetNodeAttrActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvGetNodeAttrResponse>(
        std::move(error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TGetNodeAttrActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvService::TEvGetNodeAttrResponse,
            HandleGetNodeAttrResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::SERVICE_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TStorageServiceActor::HandleGetNodeAttr(
    const TEvService::TEvGetNodeAttrRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (msg->Record.GetName().empty()) {
        // handle creation by NodeId can be handled directly by the shard
        ForwardRequestToShard<TEvService::TGetNodeAttrMethod>(
            ctx,
            ev,
            ExtractShardNo(msg->Record.GetNodeId()));
        return;
    }

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<TEvService::TEvGetNodeAttrResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    const NProto::TFileStore& filestore = session->FileStore;

    auto& headers = *msg->Record.MutableHeaders();
    headers.SetBehaveAsDirectoryTablet(
        filestore.GetFeatures().GetDirectoryCreationInShardsEnabled());
    if (auto shardNo = ExtractShardNo(msg->Record.GetNodeId())) {
        // parent directory is managed by a shard
        auto [shardId, error] = SelectShard(
            ctx,
            sessionId,
            seqNo,
            headers.GetDisableMultiTabletForwarding(),
            TEvService::TGetNodeAttrMethod::Name,
            msg->CallContext->RequestId,
            filestore,
            shardNo);
        if (HasError(error)) {
            auto response =
                std::make_unique<TEvService::TEvGetNodeAttrResponse>(
                    std::move(error));
            return NCloud::Reply(ctx, *ev, std::move(response));
        }
        msg->Record.SetFileSystemId(shardId);
    }

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        session->MediaKind,
        session->RequestStats,
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(SelfId(), cookie, msg->CallContext);

    const bool multiTabletForwardingEnabled =
        StorageConfig->GetMultiTabletForwardingEnabled()
        && !msg->Record.GetHeaders().GetDisableMultiTabletForwarding();
    auto actor = std::make_unique<TGetNodeAttrActor>(
        std::move(requestInfo),
        std::move(msg->Record),
        session->RequestStats,
        ProfileLog,
        multiTabletForwardingEnabled);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
