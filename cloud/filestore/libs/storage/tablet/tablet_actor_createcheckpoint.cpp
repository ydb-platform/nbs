#include "tablet_actor.h"

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCreateCheckpoint(
    const TEvService::TEvCreateCheckpointRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    const auto& checkpointId = msg->Record.GetCheckpointId();
    const auto nodeId = msg->Record.GetNodeId();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s CreateCheckpoint started (checkpointId: %s, nodeId: %lu)",
        LogTag.c_str(),
        checkpointId.c_str(),
        nodeId);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    AddTransaction<TEvService::TCreateCheckpointMethod>(*requestInfo);

    ExecuteTx<TCreateCheckpoint>(
        ctx,
        std::move(requestInfo),
        checkpointId,
        nodeId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_CreateCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateCheckpoint& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TIndexTabletActor::ExecuteTx_CreateCheckpoint(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCreateCheckpoint& args)
{
    FILESTORE_VALIDATE_TX_ERROR(CreateCheckpoint, args);

    TIndexTabletDatabaseProxy db(tx.DB, &args.IndexStateRequests);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "CreateCheckpoint");
    }

    auto* checkpoint = CreateCheckpoint(
        db,
        args.CheckpointId,
        args.NodeId,
        args.CommitId);
    Y_ABORT_UNLESS(checkpoint);
}

void TIndexTabletActor::CompleteTx_CreateCheckpoint(
    const TActorContext& ctx,
    TTxIndexTablet::TCreateCheckpoint& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto response = std::make_unique<TEvService::TEvCreateCheckpointResponse>(args.Error);
    CompleteResponse<TEvService::TCreateCheckpointMethod>(
        response->Record,
        args.RequestInfo->CallContext,
        ctx);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
}

}   // namespace NCloud::NFileStore::NStorage
