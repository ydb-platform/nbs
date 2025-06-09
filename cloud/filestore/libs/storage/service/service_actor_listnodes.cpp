#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/model/utils.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <util/string/join.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

const auto NoEnt = MAKE_FILESTORE_ERROR(NProto::E_FS_NOENT);

////////////////////////////////////////////////////////////////////////////////

class TListNodesActor final: public TActorBootstrapped<TListNodesActor>
{
private:
    // Original request
    const TRequestInfoPtr RequestInfo;
    NProto::TListNodesRequest ListNodesRequest;

    // Filesystem-specific params
    const TString LogTag;

    // Control flags
    const bool MultiTabletForwardingEnabled;
    const bool GetNodeAttrBatchEnabled;

    // Response data
    NProto::TListNodesResponse Response;
    ui32 GetNodeAttrResponses = 0;

    TVector<ui32> MissingNodeIndices;
    ui32 LostNodeCount = 0;
    ui32 CheckedNodeCount = 0;

    TVector<TVector<ui32>> Cookie2NodeIndices;
    TVector<TString> Cookie2ShardId;

    // Stats for reporting
    IRequestStatsPtr RequestStats;
    IProfileLogPtr ProfileLog;

public:
    TListNodesActor(
        TRequestInfoPtr requestInfo,
        NProto::TListNodesRequest listNodesRequest,
        bool multiTabletForwardingEnabled,
        bool getNodeAttrBatchEnabled,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);
    STFUNC(StateCheck);

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

    void CheckNodeAttrs(const TActorContext& ctx);

    void HandleGetNodeAttrResponseCheck(
        const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void CheckResponseAndReply(const TActorContext& ctx);
    void ReplyAndDie(const TActorContext& ctx);
    void HandleError(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TListNodesActor::TListNodesActor(
        TRequestInfoPtr requestInfo,
        NProto::TListNodesRequest listNodesRequest,
        bool multiTabletForwardingEnabled,
        bool getNodeAttrBatchEnabled,
        IRequestStatsPtr requestStats,
        IProfileLogPtr profileLog)
    : RequestInfo(std::move(requestInfo))
    , ListNodesRequest(std::move(listNodesRequest))
    , LogTag(ListNodesRequest.GetFileSystemId())
    , MultiTabletForwardingEnabled(multiTabletForwardingEnabled)
    , GetNodeAttrBatchEnabled(getNodeAttrBatchEnabled)
    , RequestStats(std::move(requestStats))
    , ProfileLog(std::move(profileLog))
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
        if (node.GetShardFileSystemId()) {
            auto& batch = batches[node.GetShardFileSystemId()];
            if (batch.Record.GetHeaders().GetSessionId().empty()) {
                batch.Record.MutableHeaders()->CopyFrom(ListNodesRequest.GetHeaders());
                batch.Record.SetFileSystemId(node.GetShardFileSystemId());
                batch.Record.SetNodeId(RootNodeId);
            }

            batch.Record.AddNames(node.GetShardNodeName());
            batch.NodeIndices.push_back(i);
        } else {
            ++GetNodeAttrResponses;
        }
    }

    ui64 cookie = 0;
    Cookie2NodeIndices.resize(batches.size());
    Cookie2ShardId.resize(batches.size());
    for (auto& [_, batch]: batches) {
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Executing GetNodeAttrBatch in shard for %s, %s",
            LogTag.c_str(),
            batch.Record.GetFileSystemId().c_str(),
            JoinSeq(", ", batch.Record.GetNames()).c_str());

        auto request =
            std::make_unique<TEvIndexTablet::TEvGetNodeAttrBatchRequest>();
        request->Record = std::move(batch.Record);

        Cookie2NodeIndices[cookie] = std::move(batch.NodeIndices);
        Cookie2ShardId[cookie] = request->Record.GetFileSystemId();

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
        if (node.GetShardFileSystemId()) {
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "[%s] Executing GetNodeAttr in shard for %s, %s",
                LogTag.c_str(),
                node.GetShardFileSystemId().c_str(),
                node.GetShardNodeName().Quote().c_str());

            auto request =
                std::make_unique<TEvService::TEvGetNodeAttrRequest>();
            request->Record.MutableHeaders()->CopyFrom(
                ListNodesRequest.GetHeaders());
            request->Record.SetFileSystemId(node.GetShardFileSystemId());
            request->Record.SetNodeId(RootNodeId);
            request->Record.SetName(node.GetShardNodeName());

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
            "No nodes at shards for parent %lu",
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
    TABLET_VERIFY(ev->Cookie < Cookie2ShardId.size());
    const auto& shardId = Cookie2ShardId[ev->Cookie];

    if (HasError(msg->GetError())) {
        if (msg->GetError().GetCode() == NoEnt) {
            TStringBuilder names;
            for (auto i: nodeIndices) {
                if (names) {
                    names << ", ";
                }
                names << Response.GetNames(i).Quote();
                MissingNodeIndices.push_back(i);
            }

            LOG_ERROR(
                ctx,
                TFileStoreComponents::SERVICE,
                "Nodes not found in shard (invalid parent?): %s, %s, %s",
                shardId.c_str(),
                FormatError(msg->GetError()).Quote().c_str(),
                names.c_str());
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to GetNodeAttrBatch from shard: %s, %s",
                shardId.c_str(),
                FormatError(msg->GetError()).Quote().c_str());

            HandleError(ctx, *msg->Record.MutableError());
            return;
        }
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "GetNodeAttrBatchResponse from shard: %s",
        msg->Record.DebugString().Quote().c_str());

    auto& responses = *msg->Record.MutableResponses();
    auto responseIter = responses.begin();
    for (const auto i: nodeIndices) {
        if (i >= Response.NodesSize()) {
            const auto message = TStringBuilder() << "NodeIndex " << i
                << " >= " << Response.NodesSize() << ", ShardId: "
                << shardId;
            ReportIndexOutOfBounds(message);
            LOG_ERROR(ctx, TFileStoreComponents::SERVICE, message);
            continue;
        }

        if (responseIter == responses.end()) {
            const auto message = TStringBuilder() << "NodeIndex " << i
                << " >= " << responses.size() << ", ShardId: " << shardId;
            ReportNotEnoughResultsInGetNodeAttrBatchResponses(message);
            LOG_ERROR(ctx, TFileStoreComponents::SERVICE, message);
            continue;
        }

        if (responseIter->GetError().GetCode() == NoEnt) {
            MissingNodeIndices.push_back(i);

            LOG_WARN(ctx, TFileStoreComponents::SERVICE, TStringBuilder()
                << "Node not found in shard: "
                << Response.GetNames(i).Quote() << ", ShardId: "
                << shardId << ", Error: "
                << FormatError(responseIter->GetError()).Quote());
            ++responseIter;
            continue;
        }

        if (HasError(responseIter->GetError())) {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to GetNodeAttr from shard: %s, %s, %s",
                Response.GetNames(i).Quote().c_str(),
                shardId.c_str(),
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
        CheckResponseAndReply(ctx);
        return;
    }
}

void TListNodesActor::HandleGetNodeAttrResponse(
    const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "GetNodeAttrResponse from shard: %s",
        msg->Record.GetNode().DebugString().Quote().c_str());

    if (HasError(msg->GetError())) {
        if (msg->GetError().GetCode() == NoEnt) {
            MissingNodeIndices.push_back(ev->Cookie);

            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "Node not found in shard: %s, %s",
                FormatError(msg->GetError()).Quote().c_str(),
                Response.GetNames(ev->Cookie).c_str());
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "Failed to GetNodeAttr from shard: %s",
                FormatError(msg->GetError()).Quote().c_str());

            HandleError(ctx, std::move(*msg->Record.MutableError()));
            return;
        }
    } else {
        TABLET_VERIFY(ev->Cookie < Response.NodesSize());
        auto* node = Response.MutableNodes(ev->Cookie);
        *node = std::move(*msg->Record.MutableNode());
    }

    if (++GetNodeAttrResponses == Response.NodesSize()) {
        CheckResponseAndReply(ctx);
        return;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListNodesActor::CheckNodeAttrs(const TActorContext& ctx)
{
    for (ui64 cookie: MissingNodeIndices) {
        const auto& name = Response.GetNames(cookie);
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Checking NodeAttr in leader for %lu, %s",
            LogTag.c_str(),
            ListNodesRequest.GetNodeId(),
            name.Quote().c_str());

        auto request =
            std::make_unique<TEvService::TEvGetNodeAttrRequest>();
        request->Record.MutableHeaders()->CopyFrom(
            ListNodesRequest.GetHeaders());
        request->Record.SetFileSystemId(ListNodesRequest.GetFileSystemId());
        request->Record.SetNodeId(ListNodesRequest.GetNodeId());
        request->Record.SetName(name);

        // forward request through tablet proxy
        ctx.Send(
            MakeIndexTabletProxyServiceId(),
            request.release(),
            0, // flags
            cookie);
    }

    Become(&TThis::StateCheck);
}

////////////////////////////////////////////////////////////////////////////////

void TListNodesActor::HandleGetNodeAttrResponseCheck(
    const TEvService::TEvGetNodeAttrResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto& node = Response.GetNodes(ev->Cookie);
    const auto& name = Response.GetNames(ev->Cookie);

    bool exists = true;
    if (HasError(msg->GetError())) {
        if (msg->GetError().GetCode() == NoEnt) {
            exists = false;
        } else {
            LOG_WARN(
                ctx,
                TFileStoreComponents::SERVICE,
                "Node check in leader failed with error: %s, %s",
                FormatError(msg->GetError()).Quote().c_str(),
                name.Quote().c_str());
        }
    } else {
        const auto& attr = msg->Record.GetNode();
        exists = attr.GetShardNodeName() == node.GetShardNodeName();
    }

    if (exists) {
        ++LostNodeCount;
        ReportNodeNotFoundInShard();
    }

    if (++CheckedNodeCount == MissingNodeIndices.size()) {
        // If all response nodes are missing, we can't respond with an ok
        // status because the client will think that it's the end of the
        // directory stream (recall that directory listing is done in windows
        // specified by an offset + size pair which is converted to a "Cookie"
        // + size pair by our vfs_fuse layer)
        // If all missing nodes are permanently lost, retrying this ListNodes
        // request won't lead to a different result => E_IO
        // If some of the missing nodes are missing because they were supposedly
        // unlinked after the ListNodes call to the leader and before the
        // GetNodeAttr[Batch] calls to the shards, a retry of such a request
        // will cause a different set of nodes to be read from the leader =>
        // E_REJECTED.
        if (MissingNodeIndices.size() == Response.NodesSize()) {
            NProto::TError error;
            if (MissingNodeIndices.size() == LostNodeCount) {
                error = MakeError(E_IO, TStringBuilder()
                    << "lost some nodes in shards for request: "
                    << ListNodesRequest.ShortDebugString().Quote());
            } else {
                error = MakeError(E_REJECTED, TStringBuilder()
                    << "concurrent directory modifications for request: "
                    << ListNodesRequest.ShortDebugString().Quote());
            }

            HandleError(ctx, std::move(error));
            return;
        }

        // Here we need to remove the Names and Nodes corresponding to the
        // missing nodes from the response. Plain Erase/EraseIf don't seem to
        // be applicable since we need to do this operation over 2 repeated
        // fields - not over a single stl-like container.
        RemoveByIndices(*Response.MutableNodes(), MissingNodeIndices);
        RemoveByIndices(*Response.MutableNames(), MissingNodeIndices);

        ReplyAndDie(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TListNodesActor::CheckResponseAndReply(const TActorContext& ctx)
{
    if (MissingNodeIndices.empty()) {
        ReplyAndDie(ctx);
        return;
    }

    // Some of the nodes are missing - i.e. they are present in the leader's
    // ListNodesResponse but absent from the corresponding shards. We need
    // to check whether they are still present in leader (they might've been
    // deleted after we got the ListNodesResponse from the leader). If they are,
    // it's an error which we should report (log + critical events). And in some
    // cases it affects the error code that we return to the client.
    CheckNodeAttrs(ctx);
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
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::SERVICE_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

STFUNC(TListNodesActor::StateCheck)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvService::TEvGetNodeAttrResponse,
            HandleGetNodeAttrResponseCheck);

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
            TEvService::TListNodesMethod::Name,
            msg->CallContext->RequestId,
            filestore,
            shardNo);
        if (HasError(error)) {
            auto response =
                std::make_unique<TEvService::TEvListNodesResponse>(
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
    auto actor = std::make_unique<TListNodesActor>(
        std::move(requestInfo),
        std::move(msg->Record),
        multiTabletForwardingEnabled,
        StorageConfig->GetGetNodeAttrBatchEnabled(),
        session->RequestStats,
        ProfileLog);

    NCloud::Register(ctx, std::move(actor));
}

}   // namespace NCloud::NFileStore::NStorage
