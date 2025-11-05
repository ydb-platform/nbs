#include "part_actor.h"

#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/storage/core/public.h>

#include <cloud/storage/core/libs/common/alloc.h>
#include <cloud/storage/core/libs/tablet/blob_id.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleAddUnconfirmedBlobs(
    const TEvPartitionPrivate::TEvAddUnconfirmedBlobsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "AddUnconfirmedBlobs",
        requestInfo->CallContext->RequestId);

    AddTransaction<TEvPartitionPrivate::TAddUnconfirmedBlobsMethod>(
        *requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TAddUnconfirmedBlobs>(
            requestInfo,
            msg->CommitId,
            std::move(msg->Blobs)));
}

bool TPartitionActor::PrepareAddUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    return true;
}

void TPartitionActor::ExecuteAddUnconfirmedBlobs(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TAddUnconfirmedBlobs& args)
{
    Y_UNUSED(ctx);

    TPartitionDatabase db(tx.DB);

    for (const auto& blob: args.Blobs) {
        State->WriteUnconfirmedBlob(db, args.CommitId, blob);
    }
}

void TPartitionActor::CompleteAddUnconfirmedBlobs(
    const TActorContext& ctx,
    TTxPartition::TAddUnconfirmedBlobs& args)
{
    TRequestScope timer(*args.RequestInfo);

    auto response =
        std::make_unique<TEvPartitionPrivate::TEvAddUnconfirmedBlobsResponse>();
    response->ExecCycles = args.RequestInfo->GetExecCycles();

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "AddUnconfirmedBlobs",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    RemoveTransaction(*args.RequestInfo);

    auto time =
        CyclesToDurationSafe(args.RequestInfo->GetTotalCycles()).MicroSeconds();
    PartCounters->RequestCounters.AddUnconfirmedBlobs.AddRequest(time);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
