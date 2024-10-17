#include "tablet_actor.h"

#include "profile_log_events.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/tablet/model/group_by.h>

#include <util/generic/set.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TAddBlobsExecutor
{
private:
    const TString LogTag;
    TIndexTabletActor& Tablet;
    const ui32 CompactionThreshold;
    THashMap<ui32, TCompactionStats> RangeId2CompactionStats;

public:
    TAddBlobsExecutor(
            TString logTag,
            TIndexTabletActor& tablet,
            ui32 compactionThreshold)
        : LogTag(std::move(logTag))
        , Tablet(tablet)
        , CompactionThreshold(compactionThreshold)
    {
    }

public:
    void Execute(
        const TActorContext& ctx,
        TTransactionContext& tx,
        TTxIndexTablet::TAddBlob& args)
    {
        TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

        switch (args.Mode) {
            case EAddBlobMode::Write:
                Execute_AddBlob_Write(ctx, db, args);
                break;

            case EAddBlobMode::WriteBatch:
                Execute_AddBlob_WriteBatch(ctx, db, args);
                break;

            case EAddBlobMode::Flush:
                Execute_AddBlob_Flush(db, args);
                break;

            case EAddBlobMode::FlushBytes:
                Execute_AddBlob_FlushBytes(db, args);
                break;

            case EAddBlobMode::Compaction:
                Execute_AddBlob_Compaction(db, args);
                break;
        }

        if (!HasError(args.Error)) {
            UpdateCompactionMap(db, args);
        }
    }

private:
    void Execute_AddBlob_Write(
        const TActorContext& ctx,
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        TABLET_VERIFY(!args.SrcBlobs);
        TABLET_VERIFY(!args.MixedBlobs);

        AddBlobsInfo(
            Tablet.GetBlockSize(),
            args.MergedBlobs,
            args.ProfileLogRequest);

        // Flush/Compaction just transfers blocks from one place to another,
        // but Write is different: we need to generate MinCommitId
        // and mark overwritten blocks now
        args.CommitId = Tablet.GenerateCommitId();
        if (args.CommitId == InvalidCommitId) {
            return Tablet.RebootTabletOnCommitOverflow(ctx, "AddBlobWrite");
        }

        for (const auto& part: args.UnalignedDataParts) {
            const auto offset = part.OffsetInBlock
                + static_cast<ui64>(part.BlockIndex) * Tablet.GetBlockSize();
            auto error = Tablet.CheckFreshBytes(
                part.NodeId,
                args.CommitId,
                offset,
                part.Data);

            if (HasError(error)) {
                ReportCheckFreshBytesFailed(error.GetMessage());
                args.Error = std::move(error);
                return;
            }
        }

        for (auto& blob: args.MergedBlobs) {
            auto& block = blob.Block;

            if (!args.Nodes.contains(block.NodeId)) {
                // already deleted
                continue;
            }

            TABLET_VERIFY(block.MinCommitId == InvalidCommitId
                && block.MaxCommitId == InvalidCommitId);
            block.MinCommitId = args.CommitId;

            Tablet.MarkFreshBlocksDeleted(
                db,
                block.NodeId,
                args.CommitId,
                block.BlockIndex,
                blob.BlocksCount);

            Tablet.MarkMixedBlocksDeleted(
                db,
                block.NodeId,
                args.CommitId,
                block.BlockIndex,
                blob.BlocksCount);

            Tablet.WriteMixedBlocks(
                db,
                blob.BlobId,
                blob.Block,
                blob.BlocksCount);

            ui32 rangeId = Tablet.GetMixedRangeIndex(
                block.NodeId,
                block.BlockIndex,
                blob.BlocksCount);
            AccessCompactionStats(rangeId).BlobsCount += 1;
        }

        for (const auto& part: args.UnalignedDataParts) {
            const auto offset = part.OffsetInBlock
                + static_cast<ui64>(part.BlockIndex) * Tablet.GetBlockSize();
            Tablet.WriteFreshBytes(
                db,
                part.NodeId,
                args.CommitId,
                offset,
                part.Data);
        }

        UpdateNodeAttrs(db, args);
    }

    void Execute_AddBlob_WriteBatch(
        const TActorContext& ctx,
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        TABLET_VERIFY(!args.SrcBlobs);
        TABLET_VERIFY(!args.MergedBlobs);
        TABLET_VERIFY(!args.UnalignedDataParts);

        AddBlobsInfo(
            Tablet.GetBlockSize(),
            args.MixedBlobs,
            args.ProfileLogRequest);

        // Flush/Compaction just transfers blocks from one place to another,
        // but Write is different: we need to generate MinCommitId
        // and mark overwritten blocks now
        args.CommitId = Tablet.GenerateCommitId();
        if (args.CommitId == InvalidCommitId) {
            return Tablet.RebootTabletOnCommitOverflow(ctx, "AddBlobWrite");
        }

        TVector<bool> isMixedBlobWritten(args.MixedBlobs.size());
        for (ui32 i = 0; i < args.MixedBlobs.size(); ++i) {
            auto& blob = args.MixedBlobs[i];

            for (auto& block: blob.Blocks) {
                TABLET_VERIFY(block.MinCommitId == InvalidCommitId
                    && block.MaxCommitId == InvalidCommitId);
                block.MinCommitId = args.CommitId;
            }

            GroupBy(
                MakeArrayRef(blob.Blocks),
                [] (const auto& l, const auto& r) {
                    return r.NodeId == l.NodeId
                        && r.BlockIndex == l.BlockIndex + 1;
                },
                [&] (TArrayRef<const TBlock> group) {
                    Tablet.MarkFreshBlocksDeleted(
                        db,
                        group[0].NodeId,
                        args.CommitId,
                        group[0].BlockIndex,
                        group.size());

                    Tablet.MarkMixedBlocksDeleted(
                        db,
                        group[0].NodeId,
                        args.CommitId,
                        group[0].BlockIndex,
                        group.size());
                });

            bool written = Tablet.WriteMixedBlocks(db, blob.BlobId, blob.Blocks);
            if (written) {
                ui32 rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
                AccessCompactionStats(rangeId).BlobsCount += 1;
            }
        }

        UpdateNodeAttrs(db, args);
    }

    void Execute_AddBlob_Flush(
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        TABLET_VERIFY(!args.SrcBlobs);
        TABLET_VERIFY(!args.MergedBlobs);
        TABLET_VERIFY(!args.UnalignedDataParts);

        for (auto& blob: args.MixedBlobs) {
            for (auto& block: blob.Blocks) {
                TABLET_VERIFY(block.MinCommitId != InvalidCommitId);

                if (block.MaxCommitId == InvalidCommitId) {
                    // block could be overwritten while we were flushing
                    auto freshBlock = Tablet.FindFreshBlock(
                        block.NodeId,
                        block.MinCommitId,
                        block.BlockIndex);

                    TABLET_VERIFY(freshBlock);
                    block.MaxCommitId = freshBlock->MaxCommitId;
                }
            }

            Tablet.DeleteFreshBlocks(db, blob.Blocks);

            const auto rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
            auto& stats = AccessCompactionStats(rangeId);
            if (Tablet.WriteMixedBlocks(db, blob.BlobId, blob.Blocks)) {
                stats.BlobsCount += 1;
            }
        }
    }

    void Execute_AddBlob_FlushBytes(
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        TABLET_VERIFY(!args.MergedBlobs);
        TABLET_VERIFY(!args.UnalignedDataParts);

        for (auto& blob: args.SrcBlobs) {
            const auto rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
            auto& stats = AccessCompactionStats(rangeId);
            if (!Tablet.UpdateBlockLists(db, blob)) {
                stats.BlobsCount = Max(stats.BlobsCount, 1U) - 1;
            }
        }

        for (auto& block: args.SrcBlocks) {
            TABLET_VERIFY(block.MaxCommitId != InvalidCommitId);

            Tablet.MarkFreshBlocksDeleted(
                db,
                block.NodeId,
                block.MaxCommitId,
                block.BlockIndex,
                1
            );
        }

        for (auto& blob: args.MixedBlobs) {
            const auto rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
            auto& stats = AccessCompactionStats(rangeId);
            if (Tablet.WriteMixedBlocks(db, blob.BlobId, blob.Blocks)) {
                stats.BlobsCount += 1;
            }
        }
    }

    void Execute_AddBlob_Compaction(
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        TABLET_VERIFY(!args.MergedBlobs);
        TABLET_VERIFY(!args.UnalignedDataParts);

        for (const auto& blob: args.SrcBlobs) {
            const auto rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
            auto& stats = AccessCompactionStats(rangeId);
            // Decrementing compaction counter for the overwritten blobs.
            // The counter is not guaranteed to be perfectly in sync with the
            // actual blob count in range so a check for moving below zero is
            // needed.
            stats.BlobsCount = Max(1U, stats.BlobsCount) - 1;
            Tablet.DeleteMixedBlocks(db, blob.BlobId, blob.Blocks);
        }

        THashMap<ui32, ui32> rangeId2AddedBlobsCount;
        for (auto& blob: args.MixedBlobs) {
            const auto rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
            // Incrementing blobs count as there could be multiple blobs
            // per compacted range see NBS-4424
            if (Tablet.WriteMixedBlocks(db, blob.BlobId, blob.Blocks)) {
                ++rangeId2AddedBlobsCount[rangeId];
            }
        }

        for (const auto& [rangeId, addedBlobsCount]: rangeId2AddedBlobsCount) {
            auto& stats = AccessCompactionStats(rangeId);

            // If addedBlobsCount >= compactionThreshold, then Compaction will
            // enter an infinite loop
            // A simple solution is to limit addedBlobsCount by threshold - 1
            auto increment = addedBlobsCount;
            if (increment >= CompactionThreshold && CompactionThreshold > 1) {
                increment = CompactionThreshold - 1;
            }
            stats.BlobsCount += increment;
        }
    }


    void UpdateCompactionMap(
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        for (const auto& x: RangeId2CompactionStats) {
            db.WriteCompactionMap(
                x.first,
                x.second.BlobsCount,
                x.second.DeletionsCount,
                x.second.GarbageBlocksCount);
            Tablet.UpdateCompactionMap(
                x.first,
                x.second.BlobsCount,
                x.second.DeletionsCount,
                x.second.GarbageBlocksCount);

            AddCompactionRange(
                args.CommitId,
                x.first,
                x.second.BlobsCount,
                x.second.DeletionsCount,
                args.ProfileLogRequest);
        }
    }

    void UpdateNodeAttrs(
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        for (auto [id, maxOffset]: args.WriteRanges) {
            auto it = args.Nodes.find(id);
            TABLET_VERIFY(it != args.Nodes.end());

            if (it->Attrs.GetSize() < maxOffset) {
                auto attrs = CopyAttrs(it->Attrs, E_CM_CMTIME);
                attrs.SetSize(maxOffset);

                Tablet.UpdateNode(
                    db,
                    id,
                    it->MinCommitId,
                    args.CommitId,
                    attrs,
                    it->Attrs);
            }
        }
    }

    TCompactionStats& AccessCompactionStats(ui32 rangeId)
    {
        THashMap<ui32, TCompactionStats>::insert_ctx ctx;
        auto it = RangeId2CompactionStats.find(rangeId, ctx);
        if (it == RangeId2CompactionStats.end()) {
            it = RangeId2CompactionStats.emplace_direct(
                ctx,
                rangeId,
                Tablet.GetCompactionStats(rangeId));
        }

        return it->second;
    }
};

}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAddBlob(
    const TEvIndexTabletPrivate::TEvAddBlobRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    FILESTORE_TRACK(
        BackgroundRequestReceived_Tablet,
        msg->CallContext,
        "AddBlob");

    ExecuteTx<TAddBlob>(
        ctx,
        std::move(requestInfo),
        msg->Mode,
        std::move(msg->SrcBlobs),
        std::move(msg->SrcBlocks),
        std::move(msg->MixedBlobs),
        std::move(msg->MergedBlobs),
        std::move(msg->WriteRanges),
        std::move(msg->UnalignedDataParts));
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AddBlob(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAddBlob& args)
{
    InitProfileLogRequestInfo(args.ProfileLogRequest, ctx.Now());

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    bool ready = true;
    for (auto [id, maxOffset]: args.WriteRanges) {
        TMaybe<IIndexTabletDatabase::TNode> node;
        if (!ReadNode(db, id, args.CommitId, node)) {
            ready = false;
        }

        if (!ready || !node) {
            // FIXME: should not allow to write to remove node
            // either not ready or already deleted
            continue;
        }

        auto [it, inserted] = args.Nodes.insert(*node);
        TABLET_VERIFY(inserted);

        AddRange(id, 0, maxOffset, args.ProfileLogRequest);
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_AddBlob(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAddBlob& args)
{
    const auto compactionThreshold =
        ScaleCompactionThreshold(Config->GetCompactionThreshold());
    TAddBlobsExecutor executor(LogTag, *this, compactionThreshold);
    executor.Execute(ctx, tx, args);
}

void TIndexTabletActor::CompleteTx_AddBlob(
    const TActorContext& ctx,
    TTxIndexTablet::TAddBlob& args)
{
    // log request
    FinalizeProfileLogRequestInfo(
        std::move(args.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        {},
        ProfileLog);

    FILESTORE_TRACK(
        ResponseSent_Tablet,
        args.RequestInfo->CallContext,
        "AddBlob");

    auto response =
        std::make_unique<TEvIndexTabletPrivate::TEvAddBlobResponse>(args.Error);
    NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

    EnqueueCollectGarbageIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
