#include "service_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log_events.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <util/generic/guid.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

/**
 * @brief This actor is used only for creating hard links in multi-tablet mode.
 * First it creates a hardlink in the shard, then it creates a nodeRef in the
 * leader, pointing to the hardlink in the shard.
 */
class TLinkActor final: public TActorBootstrapped<TLinkActor>
{
private:
    // Original request
    const TRequestInfoPtr RequestInfo;
    NProto::TCreateNodeRequest CreateNodeRequest;
    const TString ShardId;
    const TString ShardNodeName;

    // Filesystem-specific params
    const TString LogTag;

    // Response data
    bool ShardResponded = false;
    NProto::TCreateNodeResponse ShardResponse;

    // Stats for reporting
    IProfileLogPtr ProfileLog;

public:
    TLinkActor(
        TRequestInfoPtr requestInfo,
        NProto::TCreateNodeRequest createNodeRequest,
        TString shardId,
        TString logTag,
        IProfileLogPtr profileLog);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateNodeInShard(const TActorContext& ctx);

    void HandleCreateResponse(
        const TEvService::TEvCreateNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleLeaderResponse(
        const TEvService::TEvCreateNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleShardResponse(
        const TEvService::TEvCreateNodeResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        NProto::TCreateNodeResponse shardResponse);

    void HandleError(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TLinkActor::TLinkActor(
        TRequestInfoPtr requestInfo,
        NProto::TCreateNodeRequest createNodeRequest,
        TString shardId,
        TString logTag,
        IProfileLogPtr profileLog)
    : RequestInfo(std::move(requestInfo))
    , CreateNodeRequest(std::move(createNodeRequest))
    , ShardId(std::move(shardId))
    , ShardNodeName(CreateGuidAsString())
    , LogTag(std::move(logTag))
    , ProfileLog(std::move(profileLog))
{}

void TLinkActor::Bootstrap(const TActorContext& ctx)
{
    CreateNodeInShard(ctx);
    Become(&TThis::StateWork);
}

void TLinkActor::CreateNodeInShard(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvCreateNodeRequest>();

    request->Record.CopyFrom(CreateNodeRequest);
    // hard links should be located at the root node of the shard
    request->Record.SetFileSystemId(ShardId);
    request->Record.SetNodeId(RootNodeId);
    // Explicitly pick the shard name to reuse afterwards in the leader
    request->Record.SetName(ShardNodeName);

    // The `CreateNodeInShard` is sent to the shard, and thus there is no need
    // to consider this shard as a standalone filesystem
    request->Record.MutableHeaders()->ClearBehaveAsDirectoryTablet();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Creating node in shard for %lu, %s",
        LogTag.c_str(),
        CreateNodeRequest.GetLink().GetTargetNode(),
        ShardId.c_str());

    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TLinkActor::HandleShardResponse(
    const TEvService::TEvCreateNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Error creating link in shard for %lu, with error %s",
            LogTag.c_str(),
            CreateNodeRequest.GetLink().GetTargetNode(),
            FormatError(msg->GetError()).Quote().c_str());

        HandleError(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Link created in shard %s with nodeId %s",
        LogTag.c_str(),
        ShardId.c_str(),
        msg->Record.GetNode().DebugString().Quote().c_str());

    auto request = std::make_unique<TEvService::TEvCreateNodeRequest>();
    // By setting the shard filesystem id, we let the filesystem know that we
    // should not verify the target node existence and just create a nodeRef
    CreateNodeRequest.SetShardFileSystemId(ShardId);
    CreateNodeRequest.MutableLink()->SetTargetNode(
        msg->Record.GetNode().GetId());
    CreateNodeRequest.MutableLink()->SetShardNodeName(ShardNodeName);
    ShardResponse = std::move(msg->Record);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] Creating nodeRef in leader for %s, %lu",
        LogTag.c_str(),
        ShardResponse.ShortDebugString().Quote().c_str(),
        CreateNodeRequest.GetLink().GetTargetNode());

    request->Record = std::move(CreateNodeRequest);

    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());

    // TODO(#2667): add proper mechanism for handling dangling links for the
    // cases, when the hardlink has been created in the shard, but the
    // nodeRef creation in the leader was not completed or failed
}

void TLinkActor::HandleLeaderResponse(
    const TEvService::TEvCreateNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::SERVICE,
            "[%s] Error creating nodeRef in leader for %lu, with error %s",
            LogTag.c_str(),
            CreateNodeRequest.GetLink().GetTargetNode(),
            FormatError(msg->GetError()).Quote().c_str());

        HandleError(ctx, msg->GetError());
        return;
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::SERVICE,
        "[%s] NodeRef created in leader for %lu",
        LogTag.c_str(),
        msg->Record.GetNode().GetId());

    // TODO(#2667): some attributes from the shard response could be invalid
    // by the time the leader response is received
    msg->Record.MutableNode()->Swap(ShardResponse.MutableNode());

    ReplyAndDie(ctx, std::move(msg->Record));
}

void TLinkActor::HandleCreateResponse(
    const TEvService::TEvCreateNodeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    if (!ShardResponded) {
        ShardResponded = true;
        HandleShardResponse(ev, ctx);
    } else {
        HandleLeaderResponse(ev, ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLinkActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    HandleError(ctx, MakeError(E_REJECTED, "request cancelled"));
}

////////////////////////////////////////////////////////////////////////////////

void TLinkActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TCreateNodeResponse shardResponse)
{
    auto response = std::make_unique<TEvService::TEvCreateNodeResponse>();
    response->Record = std::move(shardResponse);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TLinkActor::HandleError(const TActorContext& ctx, NProto::TError error)
{
    auto response =
        std::make_unique<TEvService::TEvCreateNodeResponse>(std::move(error));

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TLinkActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvService::TEvCreateNodeResponse, HandleCreateResponse);

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

void TStorageServiceActor::HandleCreateNode(
    const TEvService::TEvCreateNodeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& clientId = GetClientId(msg->Record);
    const auto& sessionId = GetSessionId(msg->Record);
    const ui64 seqNo = GetSessionSeqNo(msg->Record);

    auto* session = State->FindSession(sessionId, seqNo);
    if (!session || session->ClientId != clientId || !session->SessionActor) {
        auto response = std::make_unique<TEvService::TEvCreateNodeResponse>(
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
            TEvService::TCreateNodeMethod::Name,
            msg->CallContext->RequestId,
            filestore,
            shardNo);
        if (HasError(error)) {
            auto response = std::make_unique<TEvService::TEvCreateNodeResponse>(
                std::move(error));
            return NCloud::Reply(ctx, *ev, std::move(response));
        }
        msg->Record.SetFileSystemId(shardId);
    }

    if (filestore.GetFeatures().GetParentlessFilesOnly()) {
        if (msg->Record.HasFile()) {
            // If the filestore supports parentless files, we should create a
            // file without a parent directory. Meaning that the file should be
            // created directly in one of the shards if the filestore is
            // sharded.
            if (const auto& shardId = session->SelectShard()) {
                msg->Record.SetFileSystemId(shardId);
                msg->Record.SetNodeId(RootNodeId);

                msg->Record.SetName(CreateGuidAsString());
                msg->Record.ClearShardFileSystemId();
            }
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::SERVICE,
                "Creating parentless file %s in filestore %s",
                msg->Record.GetName().c_str(),
                msg->Record.GetFileSystemId().c_str());
        } else {
            return NCloud::Reply(
                ctx,
                *ev,
                std::make_unique<TEvService::TEvCreateNodeResponse>(
                    ErrorNotSupported(
                        "Parentless filestore only supports creating files")));
        }
    } else if (msg->Record.HasLink()) {
        auto shardNo = ExtractShardNo(msg->Record.GetLink().GetTargetNode());

        auto [shardId, error] = SelectShard(
            ctx,
            sessionId,
            seqNo,
            headers.GetDisableMultiTabletForwarding(),
            TEvService::TCreateNodeMethod::Name,
            msg->CallContext->RequestId,
            filestore,
            shardNo);
        if (HasError(error)) {
            auto response = std::make_unique<TEvService::TEvCreateNodeResponse>(
                std::move(error));
            return NCloud::Reply(ctx, *ev, std::move(response));
        }
        if (shardId) {
            // If the target node is located on a shard, start a worker actor
            // to separately increment the link count in the shard and insert
            // the node in the leader.

            auto [cookie, inflight] = CreateInFlightRequest(
                TRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
                session->MediaKind,
                session->RequestStats,
                ctx.Now());

            InitProfileLogRequestInfo(inflight->ProfileLogRequest, msg->Record);

            auto requestInfo =
                CreateRequestInfo(SelfId(), cookie, msg->CallContext);

            auto actor = std::make_unique<TLinkActor>(
                std::move(requestInfo),
                std::move(msg->Record),
                shardId,
                filestore.GetFileSystemId(),
                ProfileLog);

            NCloud::Register(ctx, std::move(actor));

            return;
        }
    } else {
        const bool multiTabletForwardingEnabled =
            StorageConfig->GetMultiTabletForwardingEnabled() &&
            !headers.GetDisableMultiTabletForwarding() &&
            (msg->Record.HasFile() ||
             filestore.GetFeatures().GetDirectoryCreationInShardsEnabled());

        if (multiTabletForwardingEnabled) {
            if (const auto& shardId = session->SelectShard()) {
                msg->Record.SetShardFileSystemId(shardId);
            }
        }
    }

    ForwardRequest<TEvService::TCreateNodeMethod>(ctx, ev);
}

}   // namespace NCloud::NFileStore::NStorage
