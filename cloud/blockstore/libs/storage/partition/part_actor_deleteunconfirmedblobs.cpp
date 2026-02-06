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

void TPartitionActor::HandleDeleteUnconfirmedBlobs(
    const TEvPartitionPrivate::TEvDeleteUnconfirmedBlobsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "DeleteUnconfirmedBlobs",
        requestInfo->CallContext->RequestId);

    AddTransaction<TEvPartitionPrivate::TDeleteUnconfirmedBlobsMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TDeleteUnconfirmedBlobs>(
            requestInfo,
            msg->CommitId));
}

bool TPartitionActor::PrepareDeleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteDeleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDeleteUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    State->DeleteUnconfirmedBlobs(db, args.CommitId);

    State->GetGarbageQueue().ReleaseBarrier(args.CommitId);
    State->AccessCommitQueue().ReleaseBarrier(args.CommitId);
}

void TPartitionActor::CompleteDeleteUnconfirmedBlobs(
    const TActorContext& ctx,
    TTxPartition::TDeleteUnconfirmedBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);

    auto response = std::make_unique<
        TEvPartitionPrivate::TEvDeleteUnconfirmedBlobsResponse>();
    response->ExecCycles = args.RequestInfo->GetExecCycles();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "DeleteUnconfirmedBlobs",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);

    auto time =
        CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.DeleteUnconfirmedBlobs.AddRequest(time);

    ProcessCommitQueue(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
