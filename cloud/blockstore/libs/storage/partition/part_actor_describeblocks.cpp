#include "part_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/helpers.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TDescribeBlocksVisitor final
    : public IFreshBlocksIndexVisitor
    , public IBlocksIndexVisitor
{
private:
    TTxPartition::TDescribeBlocks& Args;

public:
    TDescribeBlocksVisitor(TTxPartition::TDescribeBlocks& args)
        : Args(args)
    {}

    bool Visit(const TFreshBlock& block) override
    {
        Args.MarkBlock(
            block.Meta.BlockIndex,
            block.Meta.CommitId,
            block.Content);
        return true;
    }

    bool Visit(
        ui32 blockIndex,
        ui64 commitId,
        const TPartialBlobId& blobId,
        ui16 blobOffset) override
    {
        Args.MarkBlock(blockIndex, commitId, blobId, blobOffset);
        return true;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TMaybe<ui64> TPartitionActor::VerifyDescribeBlocksCheckpoint(
    const TActorContext& ctx,
    const TString& checkpointId,
    TRequestInfo& requestInfo)
{
    if (!checkpointId) {
        return State->GetLastCommitId();
    }

    const ui64 commitId = State->GetCheckpoints().GetCommitId(checkpointId, false);
    if (commitId) {
        return commitId;
    }

    ui32 flags = 0;
    SetProtoFlag(flags, NProto::EF_SILENT);
    auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>(
        MakeError(
            E_NOT_FOUND,
            TStringBuilder()
                << "checkpoint not found: " << checkpointId.Quote(),
            flags));

    LWTRACK(
        ResponseSent_Partition,
        requestInfo.CallContext->LWOrbit,
        "DescribeBlocks",
        requestInfo.CallContext->RequestId);

    NCloud::Reply(ctx, requestInfo, std::move(response));
    return {};
}

void TPartitionActor::DescribeBlocks(
    const TActorContext& ctx,
    TRequestInfoPtr requestInfo,
    ui64 commitId,
    const TBlockRange32& describeRange)
{
    State->GetCleanupQueue().AcquireBarrier(commitId);

    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Start describe blocks @%lu (range: %s)",
        LogTitle.GetWithTime().c_str(),
        commitId,
        DescribeRange(describeRange).c_str());

    AddTransaction<TEvVolume::TDescribeBlocksMethod>(*requestInfo);

    ExecuteTx(
        ctx,
        CreateTx<TDescribeBlocks>(requestInfo, commitId, describeRange));
}

void TPartitionActor::HandleDescribeBlocks(
    const TEvVolume::TEvDescribeBlocksRequest::TPtr& ev,
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
        "DescribeBlocks",
        requestInfo->CallContext->RequestId);

    auto reply = [&](auto response) {
        LWTRACK(
            ResponseSent_Partition,
            requestInfo->CallContext->LWOrbit,
            "DescribeBlocks",
            requestInfo->CallContext->RequestId);

        NCloud::Reply(ctx, *requestInfo, std::move(response));
    };

    if (State->GetBaseDiskId()) {
        auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>(
            MakeError(E_NOT_IMPLEMENTED, TStringBuilder()
                << "DescribeBlocks is not implemented for overlay disks"));
        reply(std::move(response));
        return;
    }

    if (msg->Record.GetBlocksCount() == 0) {
        auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>(
            MakeError(E_ARGUMENT, TStringBuilder()
                << "empty block range is forbidden for DescribeBlocks: ["
                << "index: " << msg->Record.GetStartIndex()
                << ", count: " << msg->Record.GetBlocksCount()
                << "]"));
        reply(std::move(response));
        return;
    }

    auto range = TBlockRange64::WithLength(
        msg->Record.GetStartIndex(),
        msg->Record.GetBlocksCount()
    );
    auto bounds = TBlockRange64::WithLength(
        0,
        State->GetConfig().GetBlocksCount()
    );

    if (!bounds.Overlaps(range)) {
        // describing out of bounds range should return empty response
        reply(std::make_unique<TEvVolume::TEvDescribeBlocksResponse>());
        return;
    }

    range = bounds.Intersect(range);

    const auto commitId = VerifyDescribeBlocksCheckpoint(
        ctx, msg->Record.GetCheckpointId(), *requestInfo);

    if (!commitId.Defined()) {
        return;
    }

    DescribeBlocks(ctx, requestInfo, *commitId, ConvertRangeSafe(range));
}

bool TPartitionActor::PrepareDescribeBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDescribeBlocks& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(tx.DB);

    ui64 commitId = args.CommitId;

    if (State->OverlapsUnconfirmedBlobs(0, commitId, args.DescribeRange)) {
        args.Interrupted = true;
        return true;
    }

    // NOTE: we should also look in confirmed blobs because they are not added
    // yet
    if (State->OverlapsConfirmedBlobs(0, commitId, args.DescribeRange)) {
        args.Interrupted = true;
        return true;
    }

    TDescribeBlocksVisitor visitor(args);
    State->FindFreshBlocks(visitor, args.DescribeRange, commitId);
    auto ready = db.FindMixedBlocks(
        visitor,
        args.DescribeRange,
        false,  // precharge
        commitId
    );
    ready &= db.FindMergedBlocks(
        visitor,
        args.DescribeRange,
        false,  // precharge
        State->GetMaxBlocksInBlob(),
        commitId
    );

    return ready;
}

void TPartitionActor::ExecuteDescribeBlocks(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TDescribeBlocks& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);
}

void TPartitionActor::CompleteDescribeBlocks(
    const TActorContext& ctx,
    TTxPartition::TDescribeBlocks& args)
{
    TRequestScope timer(*args.RequestInfo);

    RemoveTransaction(*args.RequestInfo);

    const ui64 commitId = args.CommitId;
    LOG_TRACE(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s [%lu][d:%s] Complete describe blocks @%lu",
        LogTitle.GetWithTime().c_str(),
        TabletID(),
        PartitionConfig.GetDiskId().c_str(),
        commitId);

    LWTRACK(
        ResponseSent_Partition,
        args.RequestInfo->CallContext->LWOrbit,
        "DescribeBlocks",
        args.RequestInfo->CallContext->RequestId);

    State->GetCleanupQueue().ReleaseBarrier(commitId);

    if (args.Interrupted) {
        auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>(
            MakeError(E_REJECTED, "DescribeBlocks transaction was interrupted")
        );
        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    auto response = std::make_unique<TEvVolume::TEvDescribeBlocksResponse>();
    FillDescribeBlocksResponse(args, response.get());

    const ui64 responseBytes = response->Record.ByteSizeLong();

    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    UpdateNetworkStat(ctx.Now(), responseBytes);
    UpdateCPUUsageStat(ctx.Now(), args.RequestInfo->GetExecCycles());

    const auto duration = CyclesToDurationSafe(args.RequestInfo->GetTotalCycles());
    const auto time = duration.MicroSeconds();
    const ui64 requestBytes = static_cast<ui64>(State->GetBlockSize()) * args.DescribeRange.Size();

    PartCounters->RequestCounters.DescribeBlocks.AddRequest(time, requestBytes);

    IProfileLog::TDescribeBlocksRequest request;
    request.RequestType = EPrivateRequestType::DescribeBlocks;
    request.Duration = duration;
    request.Range = ConvertRangeSafe(args.DescribeRange);
    IProfileLog::TRecord record;
    record.DiskId = State->GetConfig().GetDiskId();
    record.Ts = ctx.Now() - duration;
    record.Request = request;
    ProfileLog->Write(std::move(record));
}

void TPartitionActor::FillDescribeBlocksResponse(
    TTxPartition::TDescribeBlocks& args,
    TEvVolume::TEvDescribeBlocksResponse* response)
{
    for (auto& mark: args.Marks) {
        if (!mark.Content) {
            continue;
        }

        auto* range = response->Record.AddFreshBlockRanges();
        range->SetStartIndex(mark.BlockIndex);
        // TODO(svartmetal): should be optimized.
        range->SetBlocksCount(1);
        range->SetBlocksContent(std::move(mark.Content));
    }

    EraseIf(args.Marks, [] (const auto& m) { return IsDeletionMarker(m.BlobId); });
    Sort(args.Marks);

    auto iter = args.Marks.begin();
    while (iter != args.Marks.end()) {
        const auto& blobId = iter->BlobId;
        auto* blobPiece = response->Record.AddBlobPieces();

        LogoBlobIDFromLogoBlobID(
            MakeBlobId(TabletID(), blobId),
            blobPiece->MutableBlobId());

        blobPiece->SetBSGroupId(
            Info()->GroupFor(blobId.Channel(), blobId.Generation()));

        do {
            auto blobOffset = iter->BlobOffset;
            auto blockIndex = iter->BlockIndex;

            auto* range = blobPiece->AddRanges();
            range->SetBlobOffset(blobOffset);
            range->SetBlockIndex(blockIndex);
            ui32 blocksCount = 1;

            ++iter;
            while (
                iter != args.Marks.end() &&
                iter->BlobId == blobId &&
                iter->BlobOffset == blobOffset + 1 &&
                iter->BlockIndex == blockIndex + 1
            ) {
                ++blobOffset;
                ++blockIndex;
                ++blocksCount;
                ++iter;
            }
            range->SetBlocksCount(blocksCount);
        } while (iter != args.Marks.end() && iter->BlobId == blobId);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
