#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAbortUnlinkDirectoryInShardActor final
    : public TActorBootstrapped<TAbortUnlinkDirectoryInShardActor>
{
private:
    const TString LogTag;
    const TActorId ParentId;
    const TString ShardId;
    const ui64 NodeId;
    TEvIndexTabletPrivate::TUnlinkDirectoryNodeAbortedInShard Result;

    using TEvAbortUnlinkRequest =
        TEvIndexTablet::TEvAbortUnlinkDirectoryNodeInShardRequest;
    using TEvAbortUnlinkResponse =
        TEvIndexTablet::TEvAbortUnlinkDirectoryNodeInShardResponse;

public:
    TAbortUnlinkDirectoryInShardActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        NProto::TError originalError,
        TString shardId,
        ui64 nodeId,
        ui64 opLogEntryId);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void AbortUnlink(const TActorContext& ctx);

    void HandleAbortUnlinkResponse(
        const TEvAbortUnlinkResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);
};

////////////////////////////////////////////////////////////////////////////////

TAbortUnlinkDirectoryInShardActor::TAbortUnlinkDirectoryInShardActor(
        TString logTag,
        TRequestInfoPtr requestInfo,
        const TActorId& parentId,
        NProtoPrivate::TRenameNodeInDestinationRequest request,
        NProto::TError originalError,
        TString shardId,
        ui64 nodeId,
        ui64 opLogEntryId)
    : LogTag(std::move(logTag))
    , ParentId(parentId)
    , ShardId(std::move(shardId))
    , NodeId(nodeId)
    , Result(
        std::move(requestInfo),
        std::move(request),
        std::move(originalError),
        opLogEntryId)
{}

void TAbortUnlinkDirectoryInShardActor::Bootstrap(const TActorContext& ctx)
{
    AbortUnlink(ctx);
    Become(&TThis::StateWork);
}

void TAbortUnlinkDirectoryInShardActor::AbortUnlink(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvAbortUnlinkRequest>();
    request->Record.MutableHeaders()->CopyFrom(Result.Request.GetHeaders());
    request->Record.SetFileSystemId(ShardId);
    request->Record.SetNodeId(NodeId);
    request->Record.MutableOriginalRequest()->CopyFrom(Result.Request);

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Sending AbortUnlinkRequest to shard %s",
        LogTag.c_str(),
        request->Record.ShortUtf8DebugString().Quote().c_str());

    ctx.Send(MakeIndexTabletProxyServiceId(), request.release());
}

void TAbortUnlinkDirectoryInShardActor::HandleAbortUnlinkResponse(
    const TEvAbortUnlinkResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET_WORKER,
        "%s Got AbortUnlinkResponse %s",
        LogTag.c_str(),
        msg->Record.ShortUtf8DebugString().Quote().c_str());

    ReplyAndDie(ctx, msg->GetError());
}

void TAbortUnlinkDirectoryInShardActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TAbortUnlinkDirectoryInShardActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    using TResponse =
        TEvIndexTabletPrivate::TEvUnlinkDirectoryNodeAbortedInShard;
    Result.Error = std::move(error);
    ctx.Send(
        ParentId,
        std::make_unique<TResponse>(std::move(Result)));

    Die(ctx);
}

STFUNC(TAbortUnlinkDirectoryInShardActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTablet::TEvAbortUnlinkDirectoryNodeInShardResponse,
            HandleAbortUnlinkResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAbortUnlinkDirectoryNodeInShard(
    const TEvIndexTablet::TEvAbortUnlinkDirectoryNodeInShardRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvIndexTablet::TAbortUnlinkDirectoryNodeInShardMethod>(
        *requestInfo);

    ExecuteTx<TAbortUnlinkDirectoryNode>(
        ctx,
        std::move(requestInfo),
        std::move(msg->Record));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AbortUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAbortUnlinkDirectoryNode& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    bool ready = ReadNode(
        db,
        args.Request.GetNodeId(),
        args.CommitId,
        args.Node);
    if (!ready) {
        return false;
    }

    if (!args.Node) {
        //
        // We can have this situation upon retry.
        //

        auto message = TStringBuilder()
            << "AbortUnlinkDirectoryNode - node not found: "
            << args.Request.ShortDebugString();

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s[%s] %s",
            LogTag.c_str(),
            args.SessionId.c_str(),
            message.Quote().c_str());

        args.Error = MakeError(S_ALREADY);
        return true;
    }

    if (args.Node->Attrs.GetType() != NProto::E_DIRECTORY_NODE) {
        auto message = ReportIsNotDirectory(TStringBuilder()
            << "AbortUnlinkDirectoryNode: "
            << args.Request.ShortDebugString());
        args.Error = ErrorIsNotDirectory(args.Request.GetNodeId());
        return true;
    }

    if (!args.Node->Attrs.GetIsPreparedForUnlink()) {
        //
        // This is also possible upon retry.
        //

        auto message = TStringBuilder()
            << "AbortUnlinkDirectoryNode - not prepared for unlink: "
            << args.Request.ShortDebugString();

        LOG_INFO(ctx, TFileStoreComponents::TABLET,
            "%s[%s] %s",
            LogTag.c_str(),
            args.SessionId.c_str(),
            message.Quote().c_str());
        args.Error = MakeError(S_ALREADY);
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_AbortUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAbortUnlinkDirectoryNode& args)
{
    Y_UNUSED(ctx);

    FILESTORE_VALIDATE_TX_ERROR(AbortUnlinkDirectoryNode, args);
    if (args.Error.GetCode() == S_ALREADY) {
        return;
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        args.OnCommitIdOverflow();
        return;
    }

    NProto::TNode attrs = CopyAttrs(args.Node->Attrs, E_CM_CTIME);
    attrs.SetIsPreparedForUnlink(false);
    UpdateNode(
        db,
        args.Request.GetNodeId(),
        args.Node->MinCommitId,
        args.CommitId,
        attrs,
        args.Node->Attrs);
}

void TIndexTabletActor::CompleteTx_AbortUnlinkDirectoryNode(
    const TActorContext& ctx,
    TTxIndexTablet::TAbortUnlinkDirectoryNode& args)
{
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s[%s] AbortUnlinkDirectoryNode completed (%s)",
        LogTag.c_str(),
        args.SessionId.c_str(),
        FormatError(args.Error).c_str());

    InvalidateNodeCaches(args.Request.GetNodeId());

    RemoveTransaction(*args.RequestInfo);
    EnqueueBlobIndexOpIfNeeded(ctx);

    using TResponse =
        TEvIndexTablet::TEvAbortUnlinkDirectoryNodeInShardResponse;
    auto response = std::make_unique<TResponse>(args.Error);
    CompleteResponse<TEvIndexTablet::TAbortUnlinkDirectoryNodeInShardMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::RegisterAbortUnlinkDirectoryInShardActor(
    const NActors::TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    NProtoPrivate::TRenameNodeInDestinationRequest request,
    NProto::TError originalError,
    TString shardId,
    ui64 nodeId,
    ui64 opLogEntryId)
{
    auto actor = std::make_unique<TAbortUnlinkDirectoryInShardActor>(
        LogTag,
        std::move(requestInfo),
        ctx.SelfID,
        std::move(request),
        std::move(originalError),
        std::move(shardId),
        nodeId,
        opLogEntryId);

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
