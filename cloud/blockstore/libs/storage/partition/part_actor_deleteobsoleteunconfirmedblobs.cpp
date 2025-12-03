#include "part_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleDeleteObsoleteUnconfirmedBlobs(
    const TEvPartitionPrivate::TEvDeleteObsoleteUnconfirmedBlobsRequest::TPtr&
        ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "DeleteObsoleteUnconfirmedBlobs",
        requestInfo->CallContext->RequestId);

    AddTransaction<TEvPartitionPrivate::TDeleteObsoleteUnconfirmedBlobsMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TDeleteObsoleteUnconfirmedBlobs>(
            requestInfo,
            msg->CommitId,
            std::move(msg->Blobs)));
}

bool TPartitionActor::PrepareDeleteObsoleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteObsoleteUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteDeleteObsoleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteObsoleteUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    State->DeleteUnconfirmedBlobs(db, args.CommitId, args.Blobs);

    State->GetGarbageQueue().ReleaseBarrier(args.CommitId);
    State->GetCommitQueue().ReleaseBarrier(args.CommitId);
}

void TPartitionActor::CompleteDeleteObsoleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTxPartition::TDeleteObsoleteUnconfirmedBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);

    auto response = std::make_unique<
        TEvPartitionPrivate::TEvDeleteObsoleteUnconfirmedBlobsResponse>();
    response->ExecCycles = args.RequestInfo->GetExecCycles();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "DeleteObsoleteUnconfirmedBlobs",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);

    auto time =
        CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.DeleteObsoleteUnconfirmedBlobs.AddRequest(
        time);

    ProcessCommitQueue(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
