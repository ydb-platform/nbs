#include "part2_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueUpdateIndexStructuresIfNeeded(
    const TActorContext& ctx)
{
    if (!Config->GetEnableConversionIntoMixedIndexV2()) {
        return;
    }

    const auto now = ctx.Now();

    if (now - LastUpdateIndexStructuresTs
            < Config->GetIndexStructuresConversionAttemptInterval())
    {
        return;
    }

    auto request =
        std::make_unique<TEvPartitionPrivate::TEvUpdateIndexStructuresRequest>(
            MakeIntrusive<TCallContext>(CreateRequestId()),
            TBlockRange32::WithLength(0, PartitionConfig.GetBlocksCount())
        );

    LWTRACK(
        BackgroundTaskStarted_Partition,
        request->CallContext->LWOrbit,
        "UpdateIndexStructures",
        static_cast<ui32>(PartitionConfig.GetStorageMediaKind()),
        request->CallContext->RequestId,
        PartitionConfig.GetDiskId());

    NCloud::Send(
        ctx,
        SelfId(),
        std::move(request));
}

void TPartitionActor::HandleUpdateIndexStructures(
    const TEvPartitionPrivate::TEvUpdateIndexStructuresRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo =
        CreateRequestInfo(
            ev->Sender,
            ev->Cookie,
            msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "UpdateIndexStructures",
        requestInfo->CallContext->RequestId);

    auto replyError = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 errorCode,
        TString errorReason)
    {
        using TResponse = TEvPartitionPrivate::TEvUpdateIndexStructuresResponse;
        auto response = std::make_unique<TResponse>(
            MakeError(errorCode, std::move(errorReason)));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "UpdateIndexStructures",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (!Config->GetEnableConversionIntoMixedIndexV2()) {
        replyError(
            ctx,
            *requestInfo,
            E_INVALID_STATE,
            "conversion into mixed index is disabled"
        );
        return;
    }

    auto tx = CreateTx<TUpdateIndexStructures>(
        requestInfo,
        msg->BlockRange);

    AddTransaction<TEvPartitionPrivate::TUpdateIndexStructuresMethod>(*requestInfo);

    ExecuteTx(ctx, std::move(tx));
}

////////////////////////////////////////////////////////////////////////////////

bool TPartitionActor::PrepareUpdateIndexStructures(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateIndexStructures& args)
{
    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    if (!State->IsIndexInitialized(args.BlockRange)) {
        return true;
    }

    return State->UpdateIndexStructures(
        db,
        ctx.Now(),
        args.BlockRange,
        &args.ConvertedToMixedIndex,
        &args.ConvertedToRangeMap
    );
}

void TPartitionActor::ExecuteUpdateIndexStructures(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TUpdateIndexStructures& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteUpdateIndexStructures(
    const TActorContext& ctx,
    TTxPartition::TUpdateIndexStructures& args)
{
    LastUpdateIndexStructuresTs = ctx.Now();

    NCloud::Reply(
        ctx,
        *args.RequestInfo,
        std::make_unique<TEvPartitionPrivate::TEvUpdateIndexStructuresResponse>(
            State->GetMixedZoneCount()
        )
    );

    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    const auto d = CyclesToDurationSafe(args.RequestInfo->GetTotalCycles());

    if (args.ConvertedToMixedIndex) {
        IProfileLog::TSysReadWriteRequest request;
        request.RequestType = ESysRequestType::ConvertToMixedIndex;
        request.Duration = d;
        request.Ranges = MakeRangesInfo(args.ConvertedToMixedIndex);

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ctx.Now() - d;
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }

    if (args.ConvertedToRangeMap) {
        IProfileLog::TSysReadWriteRequest request;
        request.RequestType = ESysRequestType::ConvertToRangeMap;
        request.Duration = d;
        request.Ranges = MakeRangesInfo(args.ConvertedToRangeMap);

        IProfileLog::TRecord record;
        record.DiskId = State->GetConfig().GetDiskId();
        record.Ts = ctx.Now() - d;
        record.Request = std::move(request);

        ProfileLog->Write(std::move(record));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
