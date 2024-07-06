#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TListNodesActor final: public TActorBootstrapped<TListNodesActor>
{
private:
    // Original request
    const TRequestInfoPtr RequestInfo;
    NProto::TListNodesRequest ListNodesRequest;

    // Filesystem-specific params
    const TString LogTag;

    // Response data
    NProto::TListNodesResponse Response;
    ui32 GetNodeAttrResponses = 0;

    // Stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;

    const bool MultiTabletForwardingEnabled;

public:
    TListNodesActor(
        TRequestInfoPtr requestInfo,
        NProto::TListNodesRequest listNodesRequest,
        TString logTag,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        bool multiTabletForwardingEnabled);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ListNodes(const TActorContext& ctx);

    void HandleListNodesResponse(
        const TEvService::TEvListNodesResponse::TPtr& ev,
        const TActorContext& ctx);

    void GetNodeAttrs(const TActorContext& ctx);

    void HandleGetNodeAttrResponse(
        const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TListNodesActor::TListNodesActor(
        TRequestInfoPtr requestInfo,
        NProto::TListNodesRequest listNodesRequest,
        TString logTag,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        bool multiTabletForwardingEnabled)
    : RequestInfo(std::move(requestInfo))
    , ListNodesRequest(std::move(listNodesRequest))
    , LogTag(std::move(logTag))
    , RequestStats(std::move(requestStats))
    , ProfileLog(std::move(profileLog))
    , MultiTabletForwardingEnabled(multiTabletForwardingEnabled)
{
}

void TListNodesActor::Bootstrap(const TActorContext& ctx)
{
    ListNodes(ctx);
    Become(&TThis::StateWork);
}

void TListNodesActor::ListNodes(const TActorContext& ctx)
{
    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Executing ListNodes in leader for %lu",
        LogTag.c_str(),
        ListNodesRequest.GetNodeId());

    auto request = std::make_unique<TEvService::TEvListNodesRequest>();
    request->Record = ListNodesRequest;

    // forward request through tablet proxy
    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TListNodesActor::GetNodeAttrs(const TActorContext& ctx)
{
    if (!MultiTabletForwardingEnabled) {
        GetNodeAttrResponses = Response.NodesSize();
        return;
    }

    // TODO(#1350): register inflight requests for these GetNodeAttr requests
    // TODO(#1350): batch GetNodeAttr requests by FollowerFileSystemId

    for (ui64 cookie = 0; cookie < Response.NodesSize(); ++cookie) {
        const auto& node = Response.GetNodes(cookie);
        if (node.GetFollowerFileSystemId()) {
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "[%s] Executing GetNodeAttr in follower for %s, %s",
                LogTag.c_str(),
                node.GetFollowerFileSystemId().c_str(),
                node.GetFollowerNodeName().Quote().c_str());

            auto request =
                std::make_unique<TEvService::TEvGetNodeAttrRequest>();
            request->Record.MutableHeaders()->CopyFrom(
                ListNodesRequest.GetHeaders());
            request->Record.SetFileSystemId(node.GetFollowerFileSystemId());
            request->Record.SetNodeId(RootNodeId);
            request->Record.SetName(node.GetFollowerNodeName());

            // forward request through tablet proxy
            ctx.Send(
                MakeIndexTabletProxyServiceId(),
                request.release(),
                0, // flags
                cookie);
        } else {
            ++GetNodeAttrResponses;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListNodesActor::HandleListNodesResponse(
    const TEvService::TEvListNodesResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        HandleError(ctx, *msg->Record.MutableError());
        return;
    }

    Response = std::move(msg->Record);
    GetNodeAttrs(ctx);

    if (GetNodeAttrResponses == Response.NodesSize()) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "No nodes at followers for parent %lu",
            ListNodesRequest.GetNodeId());

        ReplyAndDie(ctx);
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListNodesActor::HandleGetNodeAttrResponse(
    const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        const auto noent = MAKE_FILESTORE_ERROR(NProto::E_FS_NOENT);
        if (msg->GetError().GetCode() == noent) {
            ReportNodeNotFoundInFollower();

            LOG_ERROR(
                ctx,
                TFileStoreComponents::SERVICE,
                "Node not found in follower: %s, %s",
                FormatError(msg->GetError()).Quote().c_str(),
                Response.GetNames(ev->Cookie).c_str());
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to GetNodeAttr from follower: %s",
                FormatError(msg->GetError()).Quote().c_str());

            HandleError(ctx, *msg->Record.MutableError());
            return;
        }
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "GetNodeAttrResponse from follower: %s",
        msg->Record.GetNode().DebugString().Quote().c_str());

    TABLET_VERIFY(ev->Cookie < Response.NodesSize());
    auto* node = Response.MutableNodes(ev->Cookie);
    *node = std::move(*msg->Record.MutableNode());
    ++GetNodeAttrResponses;

    if (GetNodeAttrResponses == Response.NodesSize()) {
        ReplyAndDie(ctx);
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListNodesActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TListNodesActor::ReplyAndDie(const TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvListNodesResponse>();
    response->Record = std::move(Response);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TListNodesActor::HandleError(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto response = std::make_unique<TEvService::TEvListNodesResponse>(
        std::move(error));
    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TListNodesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvService::TEvListNodesResponse,
            HandleListNodesResponse);
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

void TStorageServiceActor::HandleListNodes(
    const TEvService::TEvListNodesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<TEvService::TEvListNodesResponse>(
            ErrorInvalidSession(clientId, sessionId, seqNo));
        return NCloud::Reply(ctx, *ev, std::move(response));
    }

    auto [cookie, inflight] = CreateInFlightRequest(
        TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
        session->MediaKind,
        session->RequestStats,
        ctx.Now());

    InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

    auto requestInfo = CreateRequestInfo(SelfId(), cookie, msg->CallContext);

    auto actor = std::make_unique<TListNodesActor>(
        std::move(requestInfo),
        std::move(msg->Record),
        msg->Record.GetFileSystemId(),
        session->RequestStats,
        ProfileLog,
        StorageConfig->GetMultiTabletForwardingEnabled());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
