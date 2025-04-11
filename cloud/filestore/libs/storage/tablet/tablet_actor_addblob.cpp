#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/tablet/model/group_by.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
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

    struct TCompactionRangeInfo
    {
        ui32 BlobsCount = 0;
        ui32 GarbageBlocksCount = 0;
        bool Compacted = false;
    };
    THashMap<ui32, TCompactionRangeInfo> RangeId2CompactionStats;

public:
    TAddBlobsExecutor(TString logTag, TIndexTabletActor& tablet)
        : LogTag(std::move(logTag))
        , Tablet(tablet)
    {}

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
            AccessCompactionRangeInfo(rangeId).BlobsCount += 1;
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

            auto writeBlocksResult = Tablet.WriteMixedBlocks(db, blob.BlobId, blob.Blocks);
            if (writeBlocksResult.NewBlob) {
                ui32 rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
                AccessCompactionRangeInfo(rangeId).BlobsCount += 1;
                AccessCompactionRangeInfo(rangeId).GarbageBlocksCount +=
                    writeBlocksResult.GarbageBlocksCount;
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
            auto& stats = AccessCompactionRangeInfo(rangeId);
            auto writeBlocksResult =
                Tablet.WriteMixedBlocks(db, blob.BlobId, blob.Blocks);
            if (writeBlocksResult.NewBlob) {
                stats.BlobsCount += 1;
                stats.GarbageBlocksCount +=
                    writeBlocksResult.GarbageBlocksCount;
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
            auto& stats = AccessCompactionRangeInfo(rangeId);
            if (!Tablet.UpdateBlockLists(db, blob)) {
                stats.BlobsCount = Max(stats.BlobsCount, 1U) - 1;
                // no proper way to reliably decrement stats.GarbageBlocksCount
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
            auto& stats = AccessCompactionRangeInfo(rangeId);
            auto writeBlocksResult =
                Tablet.WriteMixedBlocks(db, blob.BlobId, blob.Blocks);
            if (writeBlocksResult.NewBlob) {
                stats.BlobsCount += 1;
                stats.GarbageBlocksCount += writeBlocksResult.GarbageBlocksCount;
            }
        }
    }

    void Execute_AddBlob_Compaction(
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        TABLET_VERIFY(!args.MergedBlobs);
        TABLET_VERIFY(!args.UnalignedDataParts);

        THashSet<ui32> rangeIds;

        for (const auto& blob: args.SrcBlobs) {
            const auto rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
            auto& rangeInfo = AccessCompactionRangeInfo(rangeId);
            // Decrementing compaction counter for the overwritten blobs.
            // The counter is not guaranteed to be perfectly in sync with the
            // actual blob count in range so a check for moving below zero is
            // needed.
            rangeInfo.BlobsCount =
                Max(1U, rangeInfo.BlobsCount) - 1;
            if (rangeInfo.BlobsCount == 0) {
                // this range will be fully compacted after this Compaction
                // iteration
                rangeInfo.Compacted = true;
            }

            Tablet.DeleteMixedBlocks(db, blob.BlobId, blob.Blocks);

            rangeIds.insert(rangeId);
        }

        THashSet<ui32> writtenRangeIds;
        for (auto& blob: args.MixedBlobs) {
            const auto rangeId = Tablet.GetMixedRangeIndex(blob.Blocks);
            if (Tablet.WriteMixedBlocks(db, blob.BlobId, blob.Blocks).NewBlob) {
                writtenRangeIds.insert(rangeId);
            }

            rangeIds.insert(rangeId);
        }

        for (const auto& rangeId: writtenRangeIds) {
            // Deliberately incrementing BlobsCount only once per range. The
            // data belonging to a range might need to be stored in more than
            // one blob due to hash collisions in the calculation of
            // <nodeId, blockIndex> -> rangeId mapping. We don't want such
            // situations to cause extra compactions and we thus treat such
            // a group of blobs as a single "logical" blob in CompactionMap.
            ++AccessCompactionRangeInfo(rangeId).BlobsCount;
        }

        // recalculating GarbageBlocksCount for each of the affected ranges
        for (const auto rangeId: rangeIds) {
            AccessCompactionRangeInfo(rangeId).GarbageBlocksCount =
                Tablet.CalculateMixedIndexRangeGarbageBlockCount(rangeId);
        }
    }

    void UpdateCompactionMap(
        TIndexTabletDatabase& db,
        TTxIndexTablet::TAddBlob& args)
    {
        for (const auto& [rangeId, updatedStats]: RangeId2CompactionStats) {
            auto stats = Tablet.GetCompactionStats(rangeId);
            db.WriteCompactionMap(
                rangeId,
                updatedStats.BlobsCount,
                stats.DeletionsCount,
                updatedStats.GarbageBlocksCount);
            Tablet.UpdateCompactionMap(
                rangeId,
                updatedStats.BlobsCount,
                stats.DeletionsCount,
                updatedStats.GarbageBlocksCount,
                updatedStats.Compacted);

            AddCompactionRange(
                args.CommitId,
                rangeId,
                updatedStats.BlobsCount,
                stats.DeletionsCount,
                updatedStats.GarbageBlocksCount,
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

    TCompactionRangeInfo& AccessCompactionRangeInfo(ui32 rangeId)
    {
        THashMap<ui32, TCompactionRangeInfo>::insert_ctx ctx;
        auto it = RangeId2CompactionStats.find(rangeId, ctx);
        if (it == RangeId2CompactionStats.end()) {
            const auto& stats = Tablet.GetCompactionStats(rangeId);
            it = RangeId2CompactionStats.emplace_direct(
                ctx,
                rangeId,
                TCompactionRangeInfo{
                    .BlobsCount = stats.BlobsCount,
                    .GarbageBlocksCount = stats.GarbageBlocksCount});
        }

        return it->second;
    }
};

}   // namespace

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
    requestInfo->StartedTs = ctx.Now();

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
    TAddBlobsExecutor executor(LogTag, *this);
    executor.Execute(ctx, tx, args);
}

void TIndexTabletActor::CompleteTx_AddBlob(
    const TActorContext& ctx,
    TTxIndexTablet::TAddBlob& args)
{
    for (auto [nodeId, _]: args.WriteRanges) {
        InvalidateNodeCaches(nodeId);
    }

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
