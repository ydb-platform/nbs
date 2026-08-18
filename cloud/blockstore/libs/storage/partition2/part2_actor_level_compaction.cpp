#include "part2_actor.h"

#include <cloud/blockstore/libs/storage/partition2/model/promote_compaction_visitor.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 CalculateUsedBlocksNeededForPromoteL0(
    const TStorageConfigPtr config,
    const TPartitionState& state)
{
    const size_t l1RangesCountPerL0Range =
        state.GetMeta().GetL0RangeSize() / state.GetMeta().GetL1RangeSize();
    const size_t blocksForHugeBlob =
        config->GetWriteBlobThresholdSSD() / state.GetBlockSize();

    return blocksForHugeBlob * l1RangesCountPerL0Range;
}

class TPromoteCompactionActor final
    : public TActorBootstrapped<TPromoteCompactionActor>
{
private:
    const ui64 TabletId;
    const ui64 CommitId;
    const NActors::TActorId TabletActorId;
    const TTabletStorageInfoPtr TabletStorageInfo;
    const TRequestInfoPtr RequestInfo;
    const ui32 BlockSize;

    TVector<TPartialBlobId> BlobIds;
    TVector<TPromoteCompactionVisitor::TReadBlobRequest> ReadBlobRequests;
    TPromoteCompactionVisitor::TScanResult ScanResult;

    TGuardedSgList GuardedEmptySgList;

    ui64 ReadBlobsWaitingResponses = 0;
    ui64 WriteBlobsWaitingResponses = 0;

public:
    TPromoteCompactionActor(
        ui64 tabletId,
        ui64 commitId,
        NActors::TActorId tabletActorId,
        TTabletStorageInfoPtr tabletStorageInfo,
        TRequestInfoPtr requestInfo,
        ui32 blockSize,
        TVector<TPartialBlobId> blobIds,
        TVector<TPromoteCompactionVisitor::TReadBlobRequest> readBlobRequests,
        TPromoteCompactionVisitor::TScanResult scanResult)
        : TabletId(tabletId)
        , CommitId(commitId)
        , TabletActorId(tabletActorId)
        , TabletStorageInfo(std::move(tabletStorageInfo))
        , RequestInfo(std::move(requestInfo))
        , BlockSize(blockSize)
        , BlobIds(std::move(blobIds))
        , ReadBlobRequests(std::move(readBlobRequests))
        , ScanResult(std::move(scanResult))
    {}

    ~TPromoteCompactionActor() override
    {
        // Needed to close all sglist used for writing\reading blobs.
        GuardedEmptySgList.Close();
    }

    void Bootstrap(const TActorContext& ctx)
    {
        Become(&TPromoteCompactionActor::StateWork);
        ReadBlobs(ctx);
    }

private:
    void ReadBlobs(const TActorContext& ctx);
    void WriteBlobs(const TActorContext& ctx);
    void AddBlobs(const TActorContext& ctx);

    void ReplyAndDie(const TActorContext& ctx, NProto::TError error);

private:
    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadBlobResponse(
        const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleWriteBlobResponse(
        const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandleAddBlobsResponse(
        const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
        const TActorContext& ctx);
};

void TPromoteCompactionActor::ReadBlobs(const TActorContext& ctx)
{
    for (const auto& request: ReadBlobRequests) {
        TGuardedSgList guardedSglist =
            GuardedEmptySgList.Create(request.Sglist);

        auto readBlobRequest =
            std::make_unique<TEvPartitionCommonPrivate::TEvReadBlobRequest>(
                MakeBlobId(TabletId, request.BlobId),
                TabletStorageInfo->BSProxyIDForChannel(
                    request.BlobId.Channel(),
                    request.BlobId.Generation()),
                request.BlobOffsets,
                std::move(guardedSglist),
                TabletStorageInfo->GroupFor(
                    request.BlobId.Channel(),
                    request.BlobId.Generation()),
                true,              // async
                TInstant::Max(),   // deadline
                false              // shouldCalculateChecksums
            );

        NCloud::Send(ctx, TabletActorId, std::move(readBlobRequest));
        ++ReadBlobsWaitingResponses;
    }

    if (ReadBlobsWaitingResponses == 0) {
        WriteBlobs(ctx);
    }
}

void TPromoteCompactionActor::WriteBlobs(const TActorContext& ctx)
{
    STORAGE_VERIFY(
        BlobIds.size() == ScanResult.ResultedBlobs.size(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    for (size_t i = 0; i < BlobIds.size(); ++i) {
        const auto& blobId = BlobIds[i];
        const auto& sglist =
            ScanResult.ResultedBlobs[i].BlobContent.GetBlocks();

        auto writeBlobRequest =
            std::make_unique<TEvPartitionCommonPrivate::TEvWriteBlobRequest>(
                blobId,
                GuardedEmptySgList.Create(sglist),
                BlockSize,
                true,             // async
                TInstant::Max()   // deadline
            );

        NCloud::Send(ctx, TabletActorId, std::move(writeBlobRequest));
        ++WriteBlobsWaitingResponses;
    }

    Y_ABORT_UNLESS(WriteBlobsWaitingResponses);
}

void TPromoteCompactionActor::AddBlobs(const TActorContext& ctx)
{
    STORAGE_VERIFY(
        BlobIds.size() == ScanResult.ResultedBlobs.size(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    TVector<TAddLevelIndexBlob> l1Blobs;

    for (size_t i = 0; i < BlobIds.size(); ++i) {
        TVector<ui32> blockIndices;
        TVector<ui64> commitIds;

        for (const auto& [blockIndex, mark]:
             ScanResult.ResultedBlobs[i].BlockIndexToMark)
        {
            blockIndices.push_back(blockIndex);
            commitIds.push_back(mark.CommitId);
        }

        l1Blobs.emplace_back(
            BlobIds[i],
            std::move(blockIndices),
            std::move(commitIds),
            TVector<ui32>());   // checksums
    }

    TAffectedBlobs affectedBlobs;
    for (auto& [blobId, blobMeta]: ScanResult.AffectedBlobs) {
        TAffectedBlob affectedBlob;
        affectedBlob.BlobMeta = std::move(blobMeta);
        affectedBlobs.emplace(blobId, std::move(affectedBlob));
    }

    auto addBlobsRequest =
        std::make_unique<TEvPartitionPrivate::TEvAddBlobsRequest>(
            CommitId,
            TVector<TAddMixedBlob>{},        // mixedBlobs
            TVector<TAddMergedBlob>{},       // mergedBlobs
            TVector<TAddFreshBlob>{},        // freshBlobs
            TVector<TAddLevelIndexBlob>{},   // l0Blobs
            std::move(l1Blobs),
            EAddBlobMode::ADD_PROMOTE_COMPACTION_RESULT,
            std::move(affectedBlobs));

    NCloud::Send(ctx, TabletActorId, std::move(addBlobsRequest));
}

void TPromoteCompactionActor::ReplyAndDie(
    const TActorContext& ctx,
    NProto::TError error)
{
    auto completionEvent =
        std::make_unique<TEvPartitionPrivate::TEvPromoteCompactionCompleted>(
            error);

    completionEvent->ExecCycles = RequestInfo->GetExecCycles();
    completionEvent->TotalCycles = RequestInfo->GetTotalCycles();
    completionEvent->CommitId = CommitId;

    // TODO: add stats

    NCloud::Send(ctx, TabletActorId, std::move(completionEvent));

    auto responseEvent =
        std::make_unique<TEvPartitionPrivate::TEvPromoteCompactionResponse>(
            error);
    NCloud::Reply(ctx, *RequestInfo, std::move(responseEvent));

    Die(ctx);
}

STFUNC(TPromoteCompactionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvPartitionCommonPrivate::TEvReadBlobResponse,
            HandleReadBlobResponse);
        HFunc(
            TEvPartitionCommonPrivate::TEvWriteBlobResponse,
            HandleWriteBlobResponse);

        HFunc(
            TEvPartitionPrivate::TEvAddBlobsResponse,
            HandleAddBlobsResponse);
        default:
            break;
    }
}

void TPromoteCompactionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TPromoteCompactionActor::HandleReadBlobResponse(
    const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        ReplyAndDie(ctx, msg->Error);
        return;
    }

    --ReadBlobsWaitingResponses;
    if (ReadBlobsWaitingResponses == 0) {
        WriteBlobs(ctx);
    }
}

void TPromoteCompactionActor::HandleWriteBlobResponse(
    const TEvPartitionCommonPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        ReplyAndDie(ctx, msg->Error);
        return;
    }

    --WriteBlobsWaitingResponses;
    if (WriteBlobsWaitingResponses == 0) {
        AddBlobs(ctx);
    }
}

void TPromoteCompactionActor::HandleAddBlobsResponse(
    const TEvPartitionPrivate::TEvAddBlobsResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->Error);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::EnqueueLevelCompactionIfNeeded(
    const NActors::TActorContext& ctx)
{
    auto& cm = State->GetCompactionMapL0();

    // TODO: implement parallel level compactions
    if (!cm.GetCompactions().empty()) {
        return;
    }

    auto top = cm.GetCompactionMap().GetTopByUsedBlocks();

    const size_t usedBlocksNeededForPromoteL0 =
        CalculateUsedBlocksNeededForPromoteL0(Config, *State);

    if (top.Stat.UsedBlockCount < usedBlocksNeededForPromoteL0) {
        return;
    }

    auto request =
        std::make_unique<TEvPartitionPrivate::TEvPromoteCompactionRequest>();
    NCloud::Send(ctx, ctx.SelfID, std::move(request));
}

void TPartitionActor::HandlePromoteCompaction(
    const TEvPartitionPrivate::TEvPromoteCompactionRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto& cm = State->GetCompactionMapL0();

    // TODO: implement parallel level compactions
    if (!cm.GetCompactions().empty()) {
        return;
    }

    auto top = cm.GetCompactionMap().GetTopByUsedBlocks();

    const size_t usedBlocksNeededForPromoteL0 =
        CalculateUsedBlocksNeededForPromoteL0(Config, *State);

    if (top.Stat.UsedBlockCount < usedBlocksNeededForPromoteL0) {
        return;
    }

    ui32 rangeIndex = top.BlockIndex / State->GetMeta().GetL0RangeSize();

    const ui64 commitId = SharedState->GenerateCommitId();
    if (commitId == InvalidCommitId) {
        RebootPartitionOnCommitIdOverflow(ctx, "TEvPromoteCompactionRequest");
        return;
    }

    cm.CompactionStarted({rangeIndex}, commitId);

    auto tx = CreateTx<TPromoteCompaction>(
        CreateRequestInfo(ev->Sender, ev->Cookie, ev->Get()->CallContext),
        rangeIndex,
        commitId);

    State->GetCleanupQueue().AcquireBarrier(commitId);
    State->GetGarbageQueue().AcquireBarrier(commitId);

    auto maybeTx = WaitForCommitsCompleted<std::unique_ptr<ITransactionBase>>(
        State->AccessL0CommitQueue(),
        commitId,
        std::move(tx));

    if (maybeTx) {
        ExecuteTx(ctx, std::move(*maybeTx));
    }
}

bool TPartitionActor::PreparePromoteCompaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TPromoteCompaction& args)
{
    Y_UNUSED(ctx);

    TRequestScope timer(*args.RequestInfo);
    TPartitionDatabase db(
        tx.DB,
        State->GetMeta().GetL0RangeSize(),
        State->GetMeta().GetL1RangeSize());

    auto range = TBlockRange32::WithLength(
        args.RangeIndex * State->GetMeta().GetL0RangeSize(),
        State->GetMeta().GetL0RangeSize());

    TPromoteCompactionVisitor visitor(
        State->GetMeta().GetL1RangeSize(),
        State->GetBlockSize(),
        State->GetMaxBlocksInBlob(),
        /*allowBlockDuplicates*/ false);

    bool ready = db.FindBlocksInL0Index(
        visitor,
        range,
        /*minCommitId*/
        State->GetBlocksFilterL0()
            .GetRangeBaselineCommitId(args.RangeIndex)
            .value_or(0),
        /*maxCommitId*/ args.CommitId);

    if (!ready) {
        return false;
    }

    args.ScanResult = visitor.Finish();

    return true;
}

void TPartitionActor::ExecutePromoteCompaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxPartition::TPromoteCompaction& args)
{
    Y_UNUSED(ctx, tx, args);
}

void TPartitionActor::CompletePromoteCompaction(
    const TActorContext& ctx,
    TTxPartition::TPromoteCompaction& args)
{
    State->GetBlocksFilterL0().UpdateCompactionBaselineCommitId(
        args.CommitId,
        args.ScanResult.MaxCommitId + 1);

    auto readBlobRequests = TPromoteCompactionVisitor::CollectReadBlobRequests(
        args.ScanResult.ResultedBlobs);

    TVector<TPartialBlobId> blobIds;
    for (const auto& blob: args.ScanResult.ResultedBlobs) {
        auto blobId = State->GenerateBlobId(
            EChannelDataKind::Mixed,
            EChannelPermission::UserWritesAllowed,
            args.CommitId,
            blob.BlobContent.GetBytesCount(),
            blobIds.size());

        blobIds.push_back(blobId);
    }

    auto actorId = NCloud::Register<TPromoteCompactionActor>(
        ctx,
        TabletID(),
        args.CommitId,
        ctx.SelfID,
        Info(),
        std::move(args.RequestInfo),
        State->GetBlockSize(),
        std::move(blobIds),
        std::move(readBlobRequests),
        std::move(args.ScanResult));

    Actors.Insert(actorId);
}

void TPartitionActor::HandlePromoteCompactionCompleted(
    const TEvPartitionPrivate::TEvPromoteCompactionCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    ui64 commitId = msg->CommitId;
    LOG_DEBUG(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s Complete promote compaction @%lu",
        LogTitle.GetWithTime().c_str(),
        commitId);

    if (HasError(msg->GetError())) {
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s Compaction @%lu failed: %s",
            LogTitle.GetWithTime().c_str(),
            commitId,
            FormatError(msg->GetError()).c_str());
        State->GetCompactionMapL0().CompactionFailed();
    } else {
        State->GetCompactionMapL0().CompactionFinished();
    }

    UpdateStats(msg->Stats);

    State->GetCleanupQueue().ReleaseBarrier(commitId);
    State->GetGarbageQueue().ReleaseBarrier(commitId);

    Actors.Erase(ev->Sender);

    EnqueueCleanupIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);
    EnqueueLevelCompactionIfNeeded(ctx);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
