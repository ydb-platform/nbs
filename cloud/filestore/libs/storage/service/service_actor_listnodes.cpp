#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

#include <util/string/join.h>

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

    TVector<TVector<ui32>> Cookie2NodeIndices;
    TVector<TString> Cookie2FollowerId;

    // Stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;

    const bool MultiTabletForwardingEnabled;
    const bool GetNodeAttrBatchEnabled;

public:
    TListNodesActor(
        TRequestInfoPtr requestInfo,
        NProto::TListNodesRequest listNodesRequest,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        bool multiTabletForwardingEnabled,
        bool getNodeAttrBatchEnabled);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ListNodes(const TActorContext& ctx);

    void HandleListNodesResponse(
        const TEvService::TEvListNodesResponse::TPtr& ev,
        const TActorContext& ctx);

    void GetNodeAttrsBatch(const TActorContext& ctx);

    void HandleGetNodeAttrBatchResponse(
        const TEvIndexTablet::TEvGetNodeAttrBatchResponse::TPtr& ev,
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
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog,
        bool multiTabletForwardingEnabled,
        bool getNodeAttrBatchEnabled)
    : RequestInfo(std::move(requestInfo))
    , ListNodesRequest(std::move(listNodesRequest))
    , LogTag(ListNodesRequest.GetFileSystemId())
    , RequestStats(std::move(requestStats))
    , ProfileLog(std::move(profileLog))
    , MultiTabletForwardingEnabled(multiTabletForwardingEnabled)
    , GetNodeAttrBatchEnabled(getNodeAttrBatchEnabled)
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

////////////////////////////////////////////////////////////////////////////////

void TListNodesActor::GetNodeAttrsBatch(const TActorContext& ctx)
{
    if (!MultiTabletForwardingEnabled) {
        GetNodeAttrResponses = Response.NodesSize();
        return;
    }

    // TODO(#1350): register inflight requests for these GetNodeAttr requests

    struct TBatch
    {
        NProtoPrivate::TGetNodeAttrBatchRequest Record;
        TVector<ui32> NodeIndices;
    };
    THashMap<TString, TBatch> batches;

    for (ui32 i = 0; i < Response.NodesSize(); ++i) {
        const auto& node = Response.GetNodes(i);
        if (node.GetFollowerFileSystemId()) {
            auto& batch = batches[node.GetFollowerFileSystemId()];
            if (batch.Record.GetHeaders().GetSessionId().Empty()) {
                batch.Record.MutableHeaders()->CopyFrom(ListNodesRequest.GetHeaders());
                batch.Record.SetFileSystemId(node.GetFollowerFileSystemId());
                batch.Record.SetNodeId(RootNodeId);
            }

            batch.Record.AddNames(node.GetFollowerNodeName());
            batch.NodeIndices.push_back(i);
        } else {
            ++GetNodeAttrResponses;
        }
    }

    ui64 cookie = 0;
    Cookie2NodeIndices.resize(batches.size());
    Cookie2FollowerId.resize(batches.size());
    for (auto& [_, batch]: batches) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Executing GetNodeAttrBatch in follower for %s, %s",
            LogTag.c_str(),
            batch.Record.GetFileSystemId().c_str(),
            JoinSeq(", ", batch.Record.GetNames()).c_str());

        auto request =
            std::make_unique<TEvIndexTablet::TEvGetNodeAttrBatchRequest>();
        request->Record = std::move(batch.Record);

        Cookie2NodeIndices[cookie] = std::move(batch.NodeIndices);
        Cookie2FollowerId[cookie] = request->Record.GetFileSystemId();

        // forward request through tablet proxy
        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            0, // flags
            cookie);

        ++cookie;
    }
}

void TListNodesActor::GetNodeAttrs(const TActorContext& ctx)
{
    if (!MultiTabletForwardingEnabled) {
        GetNodeAttrResponses = Response.NodesSize();
        return;
    }

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
    if (GetNodeAttrBatchEnabled) {
        GetNodeAttrsBatch(ctx);
    } else {
        GetNodeAttrs(ctx);
    }

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

void TListNodesActor::HandleGetNodeAttrBatchResponse(
    const TEvIndexTablet::TEvGetNodeAttrBatchResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TABLET_VERIFY(ev->Cookie < Cookie2NodeIndices.size());
    const auto& nodeIndices = Cookie2NodeIndices[ev->Cookie];
    TABLET_VERIFY(ev->Cookie < Cookie2FollowerId.size());
    const auto& followerId = Cookie2FollowerId[ev->Cookie];

    const auto noent = MAKE_FILESTORE_ERROR(NProto::E_FS_NOENT);
    if (HasError(msg->GetError())) {
        if (msg->GetError().GetCode() == noent) {
            ReportNodeNotFoundInFollower();

            TStringBuilder names;
            for (auto i: nodeIndices) {
                if (names) {
                    names << ", ";
                }
                names << Response.GetNames(i).Quote();
            }

            LOG_ERROR(
                ctx,
                TFileStoreComponents::SERVICE,
                "Nodes not found in follower: %s, %s, %s",
                followerId.c_str(),
                FormatError(msg->GetError()).Quote().c_str(),
                names.c_str());
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to GetNodeAttrBatch from follower: %s, %s",
                followerId.c_str(),
                FormatError(msg->GetError()).Quote().c_str());

            HandleError(ctx, *msg->Record.MutableError());
            return;
        }
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "GetNodeAttrBatchResponse from follower: %s",
        msg->Record.DebugString().Quote().c_str());

    auto& responses = *msg->Record.MutableResponses();
    auto responseIter = responses.begin();
    for (const auto i: nodeIndices) {
        if (i >= Response.NodesSize()) {
            const auto message = TStringBuilder() << "NodeIndex " << i
                << " >= " << Response.NodesSize() << ", FollowerId: "
                << followerId;
            ReportIndexOutOfBounds(message);
            LOG_ERROR(ctx, TFileStoreComponents::SERVICE, message);
            continue;
        }

        if (responseIter == responses.end()) {
            const auto message = TStringBuilder() << "NodeIndex " << i
                << " >= " << responses.size() << ", FollowerId: " << followerId;
            ReportNotEnoughResultsInGetNodeAttrBatchResponses(message);
            LOG_ERROR(ctx, TFileStoreComponents::SERVICE, message);
            continue;
        }

        if (responseIter->GetError().GetCode() == noent) {
            // TODO(#1350): mb this shouldn't be a critical event - list ->
            // unlink -> get attr race may lead to such cases
            // nodes and names for which getnodeattr returns an error should be
            // removed from the response
            const auto message = TStringBuilder() << "Node not found in follower: "
                << Response.GetNames(i).Quote() << ", FollowerId: "
                << followerId << ", Error: "
                << FormatError(responseIter->GetError()).Quote();
            ReportNodeNotFoundInFollower(message);
            LOG_ERROR(ctx, TFileStoreComponents::SERVICE, message);
            ++responseIter;
            continue;
        }

        if (HasError(responseIter->GetError())) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to GetNodeAttr from follower: %s, %s, %s",
                Response.GetNames(i).Quote().c_str(),
                followerId.c_str(),
                FormatError(responseIter->GetError()).Quote().c_str());
            ++responseIter;
            continue;
        }

        auto* node = Response.MutableNodes(i);
        *node = std::move(*responseIter->MutableNode());

        ++responseIter;
    }

    GetNodeAttrResponses += nodeIndices.size();
    if (GetNodeAttrResponses == Response.NodesSize()) {
        ReplyAndDie(ctx);
        return;
    }
}

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
        HFunc(
            TEvIndexTablet::TEvGetNodeAttrBatchResponse,
            HandleGetNodeAttrBatchResponse);

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

    const bool multiTabletForwardingEnabled =
        StorageConfig->GetMultiTabletForwardingEnabled()
        && !msg->Record.GetHeaders().GetDisableMultiTabletForwarding();
    auto actor = std::make_unique<TListNodesActor>(
        std::move(requestInfo),
        std::move(msg->Record),
        session->RequestStats,
        ProfileLog,
        multiTabletForwardingEnabled,
        StorageConfig->GetGetNodeAttrBatchEnabled());

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
