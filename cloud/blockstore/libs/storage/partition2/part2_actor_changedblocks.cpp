#include "part2_actor.h"

#include <cloud/blockstore/libs/storage/api/volume_proxy.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

IEventBasePtr CreateGetChangedBlocksResponse(const TVector<ui8>& changedBlocks)
{
    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>();

    for (const auto& b: changedBlocks) {
        response->Record.MutableMask()->push_back(b);
    }

    return response;
}

void FillOperationCompleted(
    TEvPartitionPrivate::TOperationCompleted& operation,
    TRequestInfoPtr requestInfo)
{
    operation.ExecCycles = requestInfo->GetExecCycles();
    operation.TotalCycles = requestInfo->GetTotalCycles();

    auto execTime = CyclesToDurationSafe(requestInfo->GetExecCycles());
    auto waitTime = CyclesToDurationSafe(requestInfo->GetWaitCycles());

    auto& counters = *operation.Stats.MutableUserReadCounters();
    counters.SetRequestsCount(1);
    counters.SetExecTime(execTime.MicroSeconds());
    counters.SetWaitTime(waitTime.MicroSeconds());
}

////////////////////////////////////////////////////////////////////////////////


class TGetChangedBlocksActor final
    : public TActorBootstrapped<TGetChangedBlocksActor>
{
private:
    const TRequestInfoPtr RequestInfo;
    const TActorId Tablet;

    const TBlockRange32 ReadRange;
    const TString BaseDiskId;
    const TString BaseDiskCheckpointId;

    TVector<ui8> ChangedBlocks;

public:
    TGetChangedBlocksActor(
        TRequestInfoPtr requestInfo,
        const TActorId& tablet,
        TBlockRange32 readRange,
        const TString& baseDiskId,
        const TString& baseDiskCheckpointId,
        TVector<ui8> changedBlocks);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendGetChangedBlocksFromBaseDisk(const TActorContext& ctx);

    void HandleGetChangedBlocksResponse(
        const TEvService::TEvGetChangedBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);
    bool HandleError(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        IEventBasePtr response,
        const NProto::TError& error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TGetChangedBlocksActor::TGetChangedBlocksActor(
    TRequestInfoPtr requestInfo,
    const TActorId& tablet,
    TBlockRange32 readRange,
    const TString& baseDiskId,
    const TString& baseDiskCheckpointId,
    TVector<ui8> changedBlocks)
    : RequestInfo(std::move(requestInfo))
    , Tablet(tablet)
    , ReadRange(readRange)
    , BaseDiskId(baseDiskId)
    , BaseDiskCheckpointId(baseDiskCheckpointId)
    , ChangedBlocks(std::move(changedBlocks))
{}

void TGetChangedBlocksActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "GetChangedBlocks",
        RequestInfo->CallContext->RequestId);

    SendGetChangedBlocksFromBaseDisk(ctx);
}

void TGetChangedBlocksActor::SendGetChangedBlocksFromBaseDisk(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvGetChangedBlocksRequest>();

    request->Record.SetDiskId(BaseDiskId);
    request->Record.SetStartIndex(ReadRange.Start);
    request->Record.SetBlocksCount(ReadRange.Size());
    request->Record.SetHighCheckpointId(BaseDiskCheckpointId);

    auto self = SelfId();

    TAutoPtr<IEventHandle> event = new IEventHandle(
        MakeVolumeProxyServiceId(),
        self,
        request.release());

    ctx.Send(event);
}

void TGetChangedBlocksActor::HandleGetChangedBlocksResponse(
    const TEvService::TEvGetChangedBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HandleError(ctx, msg->GetError())) {
        return;
    }

    if (msg->Record.GetMask().size() > ChangedBlocks.size()) {
        HandleError(
            ctx,
            MakeError(E_FAIL, "Changed blocks mask size from the base disk should not be wider than from the overlay disk")
        );
        return;
    }

    const auto& mask =  msg->Record.GetMask();

    for (ui64 i = 0; i < mask.size(); i++) {
        ChangedBlocks[i] |= mask[i];
    }

    auto response = CreateGetChangedBlocksResponse(ChangedBlocks);
    ReplyAndDie(ctx, std::move(response), {});
}

void TGetChangedBlocksActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request =
        std::make_unique<TEvPartitionPrivate::TEvGetChangedBlocksCompleted>(error);

    FillOperationCompleted(*request, RequestInfo);

    NCloud::Send(ctx, Tablet, std::move(request));
}

bool TGetChangedBlocksActor::HandleError(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    if (FAILED(error.GetCode())) {
        auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>(error);
        ReplyAndDie(ctx, std::move(response), error);
        return true;
    }

    return false;
}

void TGetChangedBlocksActor::ReplyAndDie(
    const TActorContext& ctx,
    IEventBasePtr response,
    const NProto::TError& error)
{
    NotifyCompleted(ctx, error);

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "GetChangedBlocks",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

////////////////////////////////////////////////////////////////////////////////

void TGetChangedBlocksActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto error = MakeError(E_REJECTED, "tablet is shutting down");

    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>(error);

    ReplyAndDie(ctx, std::move(response), error);
}

STFUNC(TGetChangedBlocksActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvService::TEvGetChangedBlocksResponse, HandleGetChangedBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

class TChangedBlocksVisitor final
    : public IFreshBlockVisitor
    , public IMergedBlockVisitor
{
private:
    TTxPartition::TGetChangedBlocks& Args;
    bool Low = true;

public:
    TChangedBlocksVisitor(TTxPartition::TGetChangedBlocks& args)
        : Args(args)
    {}

    void Visit(
        const TBlock& block,
        TStringBuf blockContent) override
    {
        Y_UNUSED(blockContent);
        Args.MarkBlock(block, Low);
    }

    void Visit(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Y_UNUSED(blobId);
        Y_UNUSED(blobOffset);
        Args.MarkBlock(block, Low);
    }

    void GetHigh()
    {
        Low = false;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleGetChangedBlocksCompleted(
    const TEvPartitionPrivate::TEvGetChangedBlocksCompleted::TPtr& ev,
    const NActors::TActorContext &ctx)
{
    Actors.erase(ev->Sender);

    FinalizeGetChangedBlocks(ctx, std::move(*ev->Get()));
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleGetChangedBlocks(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
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
        "GetChangedBlocks",
        requestInfo->CallContext->RequestId);

    auto reply = [=] (
        const TActorContext& ctx,
        TRequestInfo& requestInfo,
        ui32 statusCode,
        TString reason,
        ui32 flags = 0)
    {
        auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>(
            MakeError(statusCode, std::move(reason), flags));

        LWTRACK(
            ResponseSent_Partition,
            requestInfo.CallContext->LWOrbit,
            "GetChangedBlocks",
            requestInfo.CallContext->RequestId);

        NCloud::Reply(ctx, requestInfo, std::move(response));
    };

    if (msg->Record.GetBlocksCount() == 0) {
        reply(ctx, *requestInfo, E_ARGUMENT, TStringBuilder()
            << "empty block range is forbidden for GetChangedBlocks: ["
            << "index: " << msg->Record.GetStartIndex()
            << ", count: " << msg->Record.GetBlocksCount()
            << "]");
        return;
    }

    auto readRange = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount()
    );
    auto bounds = TBlockRange64::WithLength(
        0,
        State->GetConfig().GetBlocksCount()
    );

    if (!bounds.Overlaps(readRange)) {
        // NBS-3085: GetChangedBlocks should return empty response for out of
        // bounds range
        reply(ctx, *requestInfo, S_OK, {});
        return;
    }

    readRange = bounds.Intersect(readRange);

    auto ok = InitChangedBlocksRange(
        readRange.Start,
        readRange.Size(),
        &readRange
    );

    if (!ok) {
        reply(ctx, *requestInfo, E_ARGUMENT, TStringBuilder()
            << "invalid block range ["
            << "index: " << msg->Record.GetStartIndex()
            << ", count: " << msg->Record.GetBlocksCount()
            << "]");
        return;
    }

    ui64 lowCommitId = 0;

    if (msg->Record.GetLowCheckpointId()) {
        lowCommitId = State->GetCheckpoints().GetCommitId(msg->Record.GetLowCheckpointId());
        if (!lowCommitId) {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_SILENT);
            reply(
                ctx,
                *requestInfo,
                E_NOT_FOUND,
                TStringBuilder()
                    << "Checkpoint not found ["
                    << "index: " << msg->Record.GetLowCheckpointId()
                    << "]",
                flags);
            return;
        }
    }

    ui64 highCommitId = State->GetLastCommitId();

    if (msg->Record.GetHighCheckpointId()) {
        highCommitId = State->GetCheckpoints().GetCommitId(msg->Record.GetHighCheckpointId());
        if (!highCommitId) {
            ui32 flags = 0;
            SetProtoFlag(flags, NProto::EF_SILENT);
            reply(
                ctx,
                *requestInfo,
                E_NOT_FOUND,
                TStringBuilder()
                    << "Checkpoint not found ["
                    << "index: " << msg->Record.GetHighCheckpointId()
                    << "]",
                flags);
            return;
        }
    }

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Start diffing blocks between @%lu and @%lu (range: %s)",
        TabletID(),
        lowCommitId,
        highCommitId,
        DescribeRange(readRange).data());

    AddTransaction<TEvService::TGetChangedBlocksMethod>(*requestInfo);

    ExecuteTx<TGetChangedBlocks>(
        ctx,
        requestInfo,
        ConvertRangeSafe(readRange),
        lowCommitId,
        highCommitId,
        msg->Record.GetIgnoreBaseDisk());
}

bool TPartitionActor::PrepareGetChangedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TGetChangedBlocks& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    if (!State->InitIndex(db, args.ReadRange)) {
        return false;
    }

    TChangedBlocksVisitor visitor(args);
    State->FindFreshBlocks(args.LowCommitId, args.ReadRange, visitor);

    bool result = State->FindMergedBlocks(
        db,
        args.LowCommitId,
        args.ReadRange,
        visitor
    );

    visitor.GetHigh();

    State->FindFreshBlocks(args.HighCommitId, args.ReadRange, visitor);

    result &= State->FindMergedBlocks(
        db,
        args.HighCommitId,
        args.ReadRange,
        visitor
    );

    if (result) {
        args.Finish();
        return true;
    }

    return false;
}

void TPartitionActor::ExecuteGetChangedBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TGetChangedBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteGetChangedBlocks(
    const TActorContext& ctx,
    TTxPartition::TGetChangedBlocks& args)
{
    TRequestScope timer(*args.RequestInfo);
    RemoveTransaction(*args.RequestInfo);

    LOG_DEBUG(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] Complete diff blocks between @%lu and @%lu (range: %s)",
        TabletID(),
        args.LowCommitId,
        args.HighCommitId,
        DescribeRange(args.ReadRange).data());

    if (!args.LowCommitId && State->GetBaseDiskId() && !args.IgnoreBaseDisk) {
        auto actor = NCloud::Register<TGetChangedBlocksActor>(
            ctx,
            args.RequestInfo,
            SelfId(),
            args.ReadRange,
            State->GetBaseDiskId(),
            State->GetBaseDiskCheckpointId(),
            std::move(args.ChangedBlocks));

        Actors.insert(actor);
        return;
    }

    auto response = CreateGetChangedBlocksResponse(args.ChangedBlocks);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "ChangedBlocks",
        args.RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    TEvPartitionPrivate::TOperationCompleted operation;
    FillOperationCompleted(operation, args.RequestInfo);

    FinalizeGetChangedBlocks(ctx, std::move(operation));
}

void TPartitionActor::FinalizeGetChangedBlocks(
    const NActors::TActorContext& ctx,
    TEvPartitionPrivate::TOperationCompleted operation)
{
    UpdateStats(operation.Stats);

    UpdateCPUUsageStat(ctx, operation.ExecCycles);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
