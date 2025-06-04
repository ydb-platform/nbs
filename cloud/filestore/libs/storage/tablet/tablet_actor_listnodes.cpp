#include "tablet_actor.h"

#include "helpers.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

void AddNode(
    NProto::TListNodesResponse& record,
    TString name,
    ui64 id,
    const NProto::TNode& attrs)
{
    record.AddNames(std::move(name));
    ConvertNodeFromAttrs(*record.AddNodes(), id, attrs);
}

void AddExternalNode(
    NProto::TListNodesResponse& record,
    TString name,
    const TString& shardId,
    const TString& shardNodeName)
{
    record.AddNames(std::move(name));
    auto* node = record.AddNodes();
    node->SetShardFileSystemId(shardId);
    node->SetShardNodeName(shardNodeName);
}

NProto::TError ValidateRequest(const NProto::TListNodesRequest& request)
{
    if (request.GetNodeId() == InvalidNodeId) {
        return ErrorInvalidArgument();
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleListNodes(
    const TEvService::TEvListNodesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    if (!AcceptRequest<TEvService::TListNodesMethod>(ev, ctx, ValidateRequest)) {
        return;
    }

    auto* msg = ev->Get();
    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvService::TListNodesMethod>(*requestInfo);

    auto maxBytes = Min(
        Config->GetMaxResponseEntries() * MaxName,
        Config->GetMaxResponseBytes());
    if (auto bytes = msg->Record.GetMaxBytes()) {
        maxBytes = Min(bytes, maxBytes);
    }

    ExecuteTx<TListNodes>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        maxBytes);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_ListNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TListNodes& args)
{
    Y_UNUSED(ctx);

    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);
    if (!session) {
        args.Error = ErrorInvalidSession(
            args.ClientId,
            args.SessionId,
            args.SessionSeqNo);
        return false;
    }

    args.CommitId = GetReadCommitId(session->GetCheckpointId());
    if (args.CommitId == InvalidCommitId) {
        args.Error = ErrorInvalidCheckpoint(session->GetCheckpointId());
        return false;
    }

    return true;
}

bool TIndexTabletActor::PrepareTx_ListNodes(
    const NActors::TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TListNodes& args)
{
    Y_UNUSED(ctx);

    // validate target node exists
    if (!ReadNode(db, args.NodeId, args.CommitId, args.Node)) {
        return false;   // not ready
    }

    if (!args.Node) {
        args.Error = ErrorInvalidTarget(args.NodeId);
        return true;
    } else if (args.Node->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
        args.Error = ErrorIsNotDirectory(args.NodeId);
        return true;
    }

    // TODO: AccessCheck
    TABLET_VERIFY(args.Node);

    if (!PrechargeNodeRefs(
        db,
        args.NodeId,
        args.Cookie,
        args.BytesToPrecharge))
    {
        return false; // not ready
    }

    bool ready = true;
    // list children refs
    if (!ReadNodeRefs(
        db,
        args.NodeId,
        args.CommitId,
        args.Cookie,
        args.ChildRefs,
        args.MaxBytes,
        &args.Next))
    {
        ready = false;
    }

    // get actual nodes
    args.ChildNodes.reserve(args.ChildRefs.size());
    for (const auto& ref: args.ChildRefs) {
        if (ref.ShardId) {
            continue;
        }

        TMaybe<TIndexTabletDatabase::TNode> childNode;
        if (!ReadNode(db, ref.ChildNodeId, args.CommitId, childNode)) {
            ready = false;   // not ready
        }

        if (ready) {
            // TODO: AccessCheck
            TABLET_VERIFY(childNode);
            args.ChildNodes.emplace_back(std::move(childNode.GetRef()));
        }
    }

    return ready;
}

void TIndexTabletActor::CompleteTx_ListNodes(
    const TActorContext& ctx,
    TTxIndexTablet::TListNodes& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvListNodesResponse>(args.Error);
    if (SUCCEEDED(args.Error.GetCode())) {
        auto& record = response->Record;
        record.MutableNames()->Reserve(args.ChildRefs.size());
        record.MutableNodes()->Reserve(args.ChildRefs.size());

        ui64 requestBytes = 0;

        size_t j = 0;
        for (size_t i = 0; i < args.ChildRefs.size(); ++i) {
            const auto& ref = args.ChildRefs[i];
            requestBytes += ref.Name.size();
            if (ref.ShardId) {
                if (!HasPendingNodeCreateInShard(ref.ShardNodeName)) {
                    AddExternalNode(
                        record,
                        ref.Name,
                        ref.ShardId,
                        ref.ShardNodeName);

                }

                continue;
            }

            AddNode(
                record,
                ref.Name,
                ref.ChildNodeId,
                args.ChildNodes[j].Attrs);
            ++j;
        }

        if (args.Next) {
            record.SetCookie(args.Next);
        }

        Metrics.ListNodes.Update(
            1,
            requestBytes,
            ctx.Now() - args.RequestInfo->StartedTs);
    }

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
