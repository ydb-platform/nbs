#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/tablet/actors/tablet_writedata.h>
#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleWriteData(
    const TEvService::TEvWriteDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    TString& buffer = *msg->Record.MutableBuffer();
    const TByteRange range(
        msg->Record.GetOffset(),
        buffer.size(),
        GetBlockSize()
    );

    auto replyError = [&] (const NProto::TError& error) {
        FILESTORE_TRACK(
            ResponseSent_Tablet,
            msg->CallContext,
            "WriteData");

        auto response =
            std::make_unique<TEvService::TEvWriteDataResponse>(error);
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    if (!CompactionStateLoadStatus.Finished) {
        const ui32 limitInQueue =
            Config->GetMaxOutOfOrderCompactionMapLoadRequestsInQueue();
        auto& s = CompactionStateLoadStatus;

        bool reject = false;

        for (ui64 b = range.FirstBlock();
                b < range.FirstBlock() + range.BlockCount();
                ++b)
        {
            const auto rangeId = GetMixedRangeIndex(
                msg->Record.GetNodeId(),
                range.FirstBlock());

            if (rangeId > s.MaxLoadedInOrderRangeId
                    && !s.LoadedOutOfOrderRangeIds.contains(rangeId))
            {
                reject = true;

                bool shouldEnqueue = true;
                ui32 oooRequestsInQueue = 0;
                for (const auto& req: s.LoadQueue) {
                    if (!req.OutOfOrder) {
                        continue;
                    }

                    if (req.FirstRangeId == rangeId) {
                        shouldEnqueue = false;
                        break;
                    }

                    if (++oooRequestsInQueue == limitInQueue) {
                        shouldEnqueue = false;
                        break;
                    }
                }

                if (shouldEnqueue) {
                    s.LoadQueue.push_back({rangeId, 1, true});
                }
            }
        }

        if (reject) {
            replyError(MakeError(
                E_REJECTED,
                "compaction state not loaded yet"));

            return;
        }
    }

    auto validator = [&](const NProto::TWriteDataRequest& request)
    {
        return ValidateWriteRequest(
            ctx,
            request,
            range,
            !Config->GetAllowHandlelessIO());
    };

    if (!AcceptRequest<TEvService::TWriteDataMethod>(ev, ctx, validator)) {
        return;
    }

    // either rejected or put into queue
    if (ThrottleIfNeeded<TEvService::TWriteDataMethod>(ev, ctx)) {
        return;
    }

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    auto blockBuffer = CreateBlockBuffer(range, std::move(buffer));
    if (Config->GetWriteBatchEnabled()) {
        auto request = std::make_unique<TWriteRequest>(
            std::move(requestInfo),
            msg->Record,
            range,
            std::move(blockBuffer));

        EnqueueWriteBatch<TEvService::TWriteDataMethod>(ctx, std::move(request));
        return;
    }

    AddTransaction<TEvService::TWriteDataMethod>(*requestInfo);

    ExecuteTx<TWriteData>(
        ctx,
        std::move(requestInfo),
        Config->GetWriteBlobThreshold(),
        msg->Record,
        range,
        std::move(blockBuffer));
}

void TIndexTabletActor::HandleWriteDataCompleted(
    const TEvIndexTabletPrivate::TEvWriteDataCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TABLET_VERIFY(TryReleaseCollectBarrier(msg->CommitId));

    WorkerActors.erase(ev->Sender);
    EnqueueBlobIndexOpIfNeeded(ctx);

    Metrics.WriteData.Update(msg->Count, msg->Size, msg->Time);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_WriteData(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteData& args)
{
    auto* session = FindSession(
        args.ClientId,
        args.SessionId,
        args.SessionSeqNo);
    if (!session) {
        args.Error = ErrorInvalidSession(
            args.ClientId,
            args.SessionId,
            args.SessionSeqNo);
        return true;
    }

    if (Config->GetAllowHandlelessIO()) {
        if (args.ExplicitNodeId == InvalidNodeId) {
            // handleless read
            args.Error = ErrorInvalidArgument();
            return true;
        }
        args.NodeId = args.ExplicitNodeId;
    } else {
        auto* handle = FindHandle(args.Handle);
        if (!handle || handle->Session != session) {
            args.Error = ErrorInvalidHandle(args.Handle);
            return true;
        }

        if (!HasFlag(handle->GetFlags(), NProto::TCreateHandleRequest::E_WRITE))
        {
            args.Error = ErrorInvalidHandle(args.Handle);
            return true;
        }

        args.NodeId = handle->GetNodeId();
    }
    args.CommitId = GetCurrentCommitId();

    LOG_TRACE(ctx, TFileStoreComponents::TABLET,
        "%s WriteNodeData tx %lu @%lu %s",
        LogTag.c_str(),
        args.CommitId,
        args.NodeId,
        args.ByteRange.Describe().c_str());

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    if (!ReadNode(db, args.NodeId, args.CommitId, args.Node)) {
        return false;
    }

    // TODO: access check
    TABLET_VERIFY(args.Node);
    if (!HasSpaceLeft(args.Node->Attrs, args.ByteRange.End())) {
        args.Error = ErrorNoSpaceLeft();
        return true;
    }

    return true;
}

void TIndexTabletActor::ExecuteTx_WriteData(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteData& args)
{
    FILESTORE_VALIDATE_TX_ERROR(WriteData, args);

    if (args.ShouldWriteBlob()) {
        return;
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "WriteData");
    }

    // XXX mark head and tail?

    MarkFreshBlocksDeleted(
        db,
        args.NodeId,
        args.CommitId,
        args.ByteRange.FirstAlignedBlock(),
        args.ByteRange.AlignedBlockCount());

    SplitRange(
        args.ByteRange.FirstAlignedBlock(),
        args.ByteRange.AlignedBlockCount(),
        BlockGroupSize,
        [&] (ui32 blockOffset, ui32 blocksCount) {
            MarkMixedBlocksDeleted(
                db,
                args.NodeId,
                args.CommitId,
                args.ByteRange.FirstAlignedBlock() + blockOffset,
                blocksCount);
        });

    for (ui64 b = args.ByteRange.FirstAlignedBlock();
            b < args.ByteRange.FirstAlignedBlock() + args.ByteRange.AlignedBlockCount();
            ++b)
    {
        WriteFreshBlock(
            db,
            args.NodeId,
            args.CommitId,
            b,
            args.Buffer->GetBlock(b - args.ByteRange.FirstAlignedBlock()));
    }

    if (args.ByteRange.UnalignedHeadLength()) {
        WriteFreshBytes(
            db,
            args.NodeId,
            args.CommitId,
            args.ByteRange.Offset,
            args.Buffer->GetUnalignedHead()
        );
    }

    if (args.ByteRange.UnalignedTailLength()) {
        if (args.Node->Attrs.GetSize() <= args.ByteRange.End()) {
            // it's safe to write at the end of file fresh block w 0s at the end
            WriteFreshBlock(
                db,
                args.NodeId,
                args.CommitId,
                args.ByteRange.LastBlock(),
                args.Buffer->GetUnalignedTail());
        } else {
            WriteFreshBytes(
                db,
                args.NodeId,
                args.CommitId,
                args.ByteRange.UnalignedTailOffset(),
                args.Buffer->GetUnalignedTail()
            );
        }
    }

    auto attrs = CopyAttrs(args.Node->Attrs, E_CM_MTIME);
    if (args.ByteRange.End() > args.Node->Attrs.GetSize()) {
        attrs.SetSize(args.ByteRange.End());
    }

    UpdateNode(
        db,
        args.NodeId,
        args.Node->MinCommitId,
        args.CommitId,
        attrs,
        args.Node->Attrs);
}

void TIndexTabletActor::CompleteTx_WriteData(
    const TActorContext& ctx,
    TTxIndexTablet::TWriteData& args)
{
    InvalidateNodeCaches(args.NodeId);

    RemoveTransaction(*args.RequestInfo);

    auto reply = [&] (
        const TActorContext& ctx,
        TTxIndexTablet::TWriteData& args)
    {
        auto response = std::make_unique<TEvService::TEvWriteDataResponse>(args.Error);
        CompleteResponse<TEvService::TWriteDataMethod>(
            response->Record,
            args.RequestInfo->CallContext,
            ctx);

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    };

    if (FAILED(args.Error.GetCode())) {
        reply(ctx, args);
        return;
    }

    if (!args.ShouldWriteBlob()) {
        reply(ctx, args);

        EnqueueFlushIfNeeded(ctx);
        EnqueueBlobIndexOpIfNeeded(ctx);

        Metrics.WriteData.Count.fetch_add(1, std::memory_order_relaxed);
        Metrics.WriteData.RequestBytes.fetch_add(
            args.ByteRange.Length,
            std::memory_order_relaxed);
        Metrics.WriteData.Time.Record(ctx.Now() - args.RequestInfo->StartedTs);

        return;
    }

    TMergedBlobBuilder builder(GetBlockSize());

    SplitRange(
        args.ByteRange.FirstAlignedBlock(),
        args.ByteRange.AlignedBlockCount(),
        BlockGroupSize,
        [&] (ui32 blockOffset, ui32 blocksCount) {
            TBlock block {
                args.NodeId,
                IntegerCast<ui32>(
                    args.ByteRange.FirstAlignedBlock() + blockOffset
                ),
                // correct CommitId will be assigned later in AddBlobs
                InvalidCommitId,
                InvalidCommitId
            };

            builder.Accept(block, blocksCount, blockOffset, *args.Buffer);
        });

    auto blobs = builder.Finish();
    TABLET_VERIFY(blobs);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "WriteData");
    }

    ui32 blobIndex = 0;
    for (auto& blob: blobs) {
        const auto ok = GenerateBlobId(
            args.CommitId,
            blob.BlobContent.size(),
            blobIndex++,
            &blob.BlobId);

        if (!ok) {
            ReassignDataChannelsIfNeeded(ctx);

            args.Error = MakeError(E_FS_OUT_OF_SPACE, "failed to generate blobId");
            reply(ctx, args);

            return;
        }
    }

    AcquireCollectBarrier(args.CommitId);

    auto actor = std::make_unique<TWriteDataActor>(
        TraceSerializer,
        LogTag,
        ctx.SelfID,
        args.RequestInfo,
        args.CommitId,
        std::move(blobs),
        TWriteRange{args.NodeId, args.ByteRange.End()});

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
