#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/storage/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCompactionActor final
    : public TActorBootstrapped<TCompactionActor>
{
private:
    const TString LogTag;
    const TString FileSystemId;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const ui64 CommitId;
    const ui32 RangeId;
    const ui32 BlockSize;
    const IProfileLogPtr ProfileLog;

    TVector<TMixedBlobMeta> SrcBlobs;
    const TVector<TCompactionBlob> DstBlobs;
    ui32 OperationSize = 0;

    THashMap<TPartialBlobId, IBlockBufferPtr, TPartialBlobIdHash> Buffers;

    size_t RequestsInFlight = 0;

    NProto::TProfileLogRequestInfo ProfileLogRequest;

public:
    TCompactionActor(
        TString logTag,
        TString fileSystemId,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        ui32 rangeId,
        ui32 blockSize,
        IProfileLogPtr profileLog,
        TVector<TMixedBlobMeta> srcBlobs,
        TVector<TCompactionBlob> dstBlobs,
        NProto::TProfileLogRequestInfo profileLogRequest);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ReadBlob(const TActorContext& ctx);
    void HandleReadBlobResponse(
        const TEvIndexTabletPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void WriteBlob(const TActorContext& ctx);
    void HandleWriteBlobResponse(
        const TEvIndexTabletPrivate::TEvWriteBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void AddBlob(const TActorContext& ctx);
    void HandleAddBlobResponse(
        const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TCompactionActor::TCompactionActor(
        TString logTag,
        TString fileSystemId,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        ui32 rangeId,
        ui32 blockSize,
        IProfileLogPtr profileLog,
        TVector<TMixedBlobMeta> srcBlobs,
        TVector<TCompactionBlob> dstBlobs,
        NProto::TProfileLogRequestInfo profileLogRequest)
    : LogTag(std::move(logTag))
    , FileSystemId(std::move(fileSystemId))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , RangeId(rangeId)
    , BlockSize(blockSize)
    , ProfileLog(std::move(profileLog))
    , SrcBlobs(std::move(srcBlobs))
    , DstBlobs(std::move(dstBlobs))
    , ProfileLogRequest(std::move(profileLogRequest))
{
    for (const auto& b: SrcBlobs) {
        OperationSize += b.Blocks.size() * BlockSize;
    }
}

void TCompactionActor::Bootstrap(const TActorContext& ctx)
{
    Become(&TThis::StateWork);

    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "Compaction");

    AddBlobsInfo(BlockSize, SrcBlobs, ProfileLogRequest);

    if (DstBlobs) {
        ReadBlob(ctx);

        if (!RequestsInFlight) {
            WriteBlob(ctx);
        }
    } else {
        AddBlob(ctx);
    }
}

void TCompactionActor::ReadBlob(const TActorContext& ctx)
{
    for (const auto& blob: SrcBlobs) {
        TVector<TReadBlob::TBlock> blocks(Reserve(blob.Blocks.size()));

        ui32 blobOffset = 0, blockOffset = 0;
        for (const auto& block: blob.Blocks) {
            if (block.MinCommitId < block.MaxCommitId) {
                blocks.emplace_back(blobOffset, blockOffset++);
            }
            ++blobOffset;
        }

        if (blocks) {
            auto request = std::make_unique<TEvIndexTabletPrivate::TEvReadBlobRequest>(
                RequestInfo->CallContext
            );
            request->Buffer = CreateBlockBuffer(TByteRange(
                0,
                blocks.size() * BlockSize,
                BlockSize
            ));
            request->Blobs.emplace_back(blob.BlobId, std::move(blocks));
            request->Blobs.back().Async = true;

            Buffers[blob.BlobId] = request->Buffer;

            NCloud::Send(ctx, Tablet, std::move(request));
            ++RequestsInFlight;
        }
    }
}

void TCompactionActor::HandleReadBlobResponse(
    const TEvIndexTabletPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    TABLET_VERIFY(RequestsInFlight);
    if (--RequestsInFlight == 0) {
        WriteBlob(ctx);
    }
}

void TCompactionActor::WriteBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvWriteBlobRequest>(
        RequestInfo->CallContext
    );

    for (const auto& blob: DstBlobs) {
        TString blobContent(Reserve(BlockSize * blob.Blocks.size()));

        for (const auto& block: blob.Blocks) {
            auto& buffer = Buffers[block.BlobId];
            blobContent.append(buffer->GetBlock(block.BlobOffset));
        }

        request->Blobs.emplace_back(blob.BlobId, std::move(blobContent));
        request->Blobs.back().Async = true;
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TCompactionActor::HandleWriteBlobResponse(
    const TEvIndexTabletPrivate::TEvWriteBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetStatus())) {
        ReplyAndDie(ctx, msg->GetError());
        return;
    }

    AddBlob(ctx);
}

void TCompactionActor::AddBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvAddBlobRequest>(
        RequestInfo->CallContext
    );
    request->Mode = EAddBlobMode::Compaction;
    request->SrcBlobs = std::move(SrcBlobs);

    for (const auto& blob: DstBlobs) {
        TVector<TBlock> blocks(Reserve(blob.Blocks.size()));
        for (const auto& block: blob.Blocks) {
            blocks.emplace_back(block);
        }

        request->MixedBlobs.emplace_back(blob.BlobId, std::move(blocks));
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TCompactionActor::HandleAddBlobResponse(
    const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TCompactionActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TCompactionActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    // log request
    FinalizeProfileLogRequestInfo(
        std::move(ProfileLogRequest),
        ctx.Now(),
        FileSystemId,
        error,
        ProfileLog);

    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvCompactionCompleted;
        auto response = std::make_unique<TCompletion>(
            error,
            TSet<ui32>({RangeId}),
            CommitId,
            1,
            OperationSize,
            ctx.Now() - RequestInfo->StartedTs);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "Compaction");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response = std::make_unique<TEvIndexTabletPrivate::TEvCompactionResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TCompactionActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTabletPrivate::TEvReadBlobResponse, HandleReadBlobResponse);
        HFunc(TEvIndexTabletPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvIndexTabletPrivate::TEvAddBlobResponse, HandleAddBlobResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::EnqueueBlobIndexOpIfNeeded(const TActorContext& ctx)
{
    const auto compactionInfo = GetCompactionInfo();
    const auto cleanupInfo = GetCleanupInfo();
    const bool shouldCleanup =
        ShouldCleanupAfterApplyingThrottlingRules(ctx, cleanupInfo);

    if (IsBlobIndexOpsQueueEmpty()) {
        auto blobIndexOpsPriority = Config->GetBlobIndexOpsPriority();
        TString message;
        if (IsCloseToBackpressureThresholds(&message)) {
            // if we are close to our backpressure thresholds, we should fall
            // back to fair scheduling so that all operations show some progress
            blobIndexOpsPriority = NProto::BIOP_FAIR;
            LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
                "%s EnqueueBlobIndexOpIfNeeded: Falling back to BIOP_FAIR: %s",
                LogTag.c_str(),
                message.Quote().c_str());
        }

        switch (blobIndexOpsPriority) {
            case NProto::BIOP_CLEANUP_FIRST: {
                if (shouldCleanup) {
                    AddBackgroundBlobIndexOp(EBlobIndexOp::Cleanup);
                } else if (compactionInfo.ShouldCompact) {
                    AddBackgroundBlobIndexOp(EBlobIndexOp::Compaction);
                }
                break;
            }

            case NProto::BIOP_COMPACTION_FIRST: {
                if (compactionInfo.ShouldCompact) {
                    AddBackgroundBlobIndexOp(EBlobIndexOp::Compaction);
                } else if (shouldCleanup) {
                    AddBackgroundBlobIndexOp(EBlobIndexOp::Cleanup);
                }
                break;
            }

            case NProto::BIOP_FAIR: {
                if (compactionInfo.ShouldCompact) {
                    AddBackgroundBlobIndexOp(EBlobIndexOp::Compaction);
                }
                if (shouldCleanup) {
                    AddBackgroundBlobIndexOp(EBlobIndexOp::Cleanup);
                }
                break;
            }
        }

        if (GetFreshBytesCount() >= Config->GetFlushBytesThreshold()
                || GetDeletedFreshBytesCount()
                >= Config->GetFlushBytesThreshold())
        {
            AddBackgroundBlobIndexOp(EBlobIndexOp::FlushBytes);
        }
    }

    if (!EnqueueBackgroundBlobIndexOp()) {
        return;
    }

    switch (GetCurrentBackgroundBlobIndexOp()) {
        case EBlobIndexOp::Compaction: {
            ctx.Send(
                SelfId(),
                new TEvIndexTabletPrivate::TEvCompactionRequest(
                    compactionInfo.RangeId,
                    false)
            );
            break;
        }

        case EBlobIndexOp::Cleanup: {
            ctx.Send(
                SelfId(),
                new TEvIndexTabletPrivate::TEvCleanupRequest(
                    cleanupInfo.RangeId)
            );
            break;
        }

        case EBlobIndexOp::FlushBytes: {
            // Flush blocked since FlushBytes op rewrites some fresh blocks as
            // blobs
            if (!FlushState.Enqueue()) {
                StartBackgroundBlobIndexOp();
                CompleteBlobIndexOp();
                if (!IsBlobIndexOpsQueueEmpty()) {
                    EnqueueBlobIndexOpIfNeeded(ctx);
                }

                return;
            }

            ctx.Send(
                SelfId(),
                new TEvIndexTabletPrivate::TEvFlushBytesRequest()
            );
            break;
        }

        default:
            TABLET_VERIFY(0);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCompaction(
    const TEvIndexTabletPrivate::TEvCompactionRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    FILESTORE_TRACK(
        BackgroundTaskStarted_Tablet,
        msg->CallContext,
        "Compaction",
        msg->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    auto replyError = [&] (const NProto::TError& error) {
        if (ev->Sender == ctx.SelfID) {
            // nothing to do though should not happen
            return;
        }

        FILESTORE_TRACK(
            ResponseSent_Tablet,
            msg->CallContext,
            "Compaction");

        auto response =
            std::make_unique<TEvIndexTabletPrivate::TEvCompactionResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    const bool started = ev->Sender == ctx.SelfID
        ? StartBackgroundBlobIndexOp() : BlobIndexOpState.Start();

    if (!started) {
        replyError(MakeError(E_TRY_AGAIN, "cleanup/compaction is in progress"));
        return;
    }

    if (!CompactionStateLoadStatus.Finished) {
        CompleteBlobIndexOp();

        replyError(MakeError(E_TRY_AGAIN, "compaction state not loaded yet"));
        return;
    }

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Compaction started (range: #%u)",
        LogTag.c_str(),
        msg->RangeId);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    ExecuteTx<TCompaction>(
        ctx,
        std::move(requestInfo),
        msg->RangeId,
        msg->FilterNodes);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_Compaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCompaction& args)
{
    InitProfileLogRequestInfo(args.ProfileLogRequest, ctx.Now());

    TIndexTabletDatabase db(tx.DB);

    args.CommitId = GetCurrentCommitId();

    // should not ref mixed range on tx restart due to nodes validation
    if (!args.RangeLoaded) {
        if (!LoadMixedBlocks(db, args.RangeId)) {
            return false;
        }
    }

    args.RangeLoaded = true;
    args.CompactionBlobs = GetBlobsForCompaction(args.RangeId);
    if (!args.CompactionBlobs || !args.FilterNodes) {
        // nothing else to do
        return true;
    }

    TSet<ui64> nodes;
    for (const auto& blob: args.CompactionBlobs) {
        for (const auto& block: blob.Blocks) {
            nodes.insert(block.NodeId);
        }
    }

    bool ready = true;
    for (auto nodeId: nodes) {
        TMaybe<IIndexTabletDatabase::TNode> node;
        if (!ReadNode(db, nodeId, args.CommitId, node)) {
            ready = false;
            continue;
        }

        if (ready && node.Defined()) {
            args.Nodes.insert(node->NodeId);
        }
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_Compaction(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TCompaction& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    const auto garbageThreshold =
        Config->GetCompactRangeGarbagePercentageThreshold();
    const auto blobSizeThreshold =
        Config->GetCompactRangeAverageBlobSizeThreshold();

    const auto stats = GetCompactionStats(args.RangeId);

    if (!args.CompactionBlobs) {
        TIndexTabletDatabase db(tx.DB);
        UpdateCompactionMap(args.RangeId, 0, stats.DeletionsCount, 0, true);
        db.WriteCompactionMap(args.RangeId, 0, stats.DeletionsCount, 0);
        args.SkipRangeRewrite = true;
    } else if (garbageThreshold && blobSizeThreshold) {
        ui32 storedBlocks = 0;
        ui32 garbageBlocks = 0;
        for (const auto& blob: args.CompactionBlobs) {
            storedBlocks += blob.Blocks.size();
            for (const auto& block: blob.Blocks) {
                if (block.MaxCommitId != InvalidCommitId) {
                    ++garbageBlocks;
                }
            }
        }

        const ui32 usedBlocks = storedBlocks - garbageBlocks;
        const ui32 garbagePercentage =
            usedBlocks ? 100 * garbageBlocks / usedBlocks : Max<ui32>();
        const ui32 averageBlobSize = static_cast<ui64>(GetBlockSize())
            * storedBlocks / args.CompactionBlobs.size();

        if (garbagePercentage <= garbageThreshold
                && averageBlobSize >= blobSizeThreshold)
        {
            // updating only the 'compacted' flag
            UpdateCompactionMap(
                args.RangeId,
                stats.BlobsCount,
                stats.DeletionsCount,
                stats.GarbageBlocksCount,
                true);

            args.SkipRangeRewrite = true;
        }
    }

    for (const ui64 nodeId: args.Nodes) {
        InvalidateReadAheadCache(nodeId);
    }
}

void TIndexTabletActor::CompleteTx_Compaction(
    const TActorContext& ctx,
    TTxIndexTablet::TCompaction& args)
{
    auto replyError = [&] (
        const TActorContext& ctx,
        TTxIndexTablet::TCompaction& args,
        const NProto::TError& error)
    {
        // log request
        FinalizeProfileLogRequestInfo(
            std::move(args.ProfileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            error,
            ProfileLog);

        FILESTORE_TRACK(
            ResponseSent_Tablet,
            args.RequestInfo->CallContext,
            "Compaction");

        if (args.RequestInfo->Sender != ctx.SelfID) {
            // reply to caller
            using TResponse = TEvIndexTabletPrivate::TEvCompactionResponse;
            auto response = std::make_unique<TResponse>(error);
            NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        }
    };

    if (args.SkipRangeRewrite) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s Compaction completed, nothing to do (range: #%u)",
            LogTag.c_str(), args.RangeId);

        replyError(ctx, args, MakeError(S_FALSE, "nothing to do"));

        CompleteBlobIndexOp();
        EnqueueBlobIndexOpIfNeeded(ctx);
        Metrics.Compaction.Update(
            1,  // count
            0,  // requestBytes
            ctx.Now() - args.RequestInfo->StartedTs);
        Metrics.Compaction.DudCount.fetch_add(1, std::memory_order_relaxed);
        return;
    }

    TVector<TBlockDataRef> blocks(Reserve(args.CompactionBlobs.size() * MaxBlocksCount));

    for (const auto& blob: args.CompactionBlobs) {
        ui32 blockOffset = 0;   // offset in read buffer, not in blob!
        for (const auto& block: blob.Blocks) {
            if (block.MinCommitId < block.MaxCommitId) {
                if (args.FilterNodes && !args.Nodes.count(block.NodeId)) {
                    continue;
                }

                blocks.emplace_back(
                    TBlockDataRef { block, blob.BlobId, blockOffset++ });
            }
        }
    }

    Sort(blocks, TBlockCompare());

    TCompactionBlobBuilder builder(
        CalculateMaxBlocksInBlob(Config->GetMaxBlobSize(), GetBlockSize()));

    for (const auto& block: blocks) {
        builder.Accept(block);
    }

    auto dstBlobs = builder.Finish();

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "Compaction");
    }

    ui32 blobIndex = 0;
    for (auto& blob: dstBlobs) {
        const auto ok = GenerateBlobId(
            args.CommitId,
            blob.Blocks.size() * GetBlockSize(),
            blobIndex++,
            &blob.BlobId);

        if (!ok) {
            ReassignDataChannelsIfNeeded(ctx);

            replyError(
                ctx,
                args,
                MakeError(E_FS_OUT_OF_SPACE, "failed to generate blobId"));

            CompleteBlobIndexOp();

            return;
        }
    }

    AcquireCollectBarrier(args.CommitId);

    auto actor = std::make_unique<TCompactionActor>(
        LogTag,
        GetFileSystemId(),
        ctx.SelfID,
        args.RequestInfo,
        args.CommitId,
        args.RangeId,
        GetBlockSize(),
        ProfileLog,
        std::move(args.CompactionBlobs),
        std::move(dstBlobs),
        std::move(args.ProfileLogRequest));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleCompactionCompleted(
    const TEvIndexTabletPrivate::TEvCompactionCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s Compaction completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    ReleaseMixedBlocks(msg->MixedBlocksRanges);
    TABLET_VERIFY(TryReleaseCollectBarrier(msg->CommitId));

    CompleteBlobIndexOp();
    EnqueueBlobIndexOpIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);

    Metrics.Compaction.Update(msg->Count, msg->Size, msg->Time);

    WorkerActors.erase(ev->Sender);
}

}   // namespace NCloud::NFileStore::NStorage
