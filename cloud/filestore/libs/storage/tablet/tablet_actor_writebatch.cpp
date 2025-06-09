#include "helpers.h"
#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TBatchInfo
{
    ui64 Size = 0;
    bool IsAligned = true;
};

TBatchInfo GetBatchInfo(const TWriteRequestList& writeBatch)
{
    TBatchInfo info;

    for (const auto& write: writeBatch) {
        info.Size += write.ByteRange.Length;
        info.IsAligned &= write.ByteRange.IsAligned();
    }

    return info;
}

const NProto::TError& PickError(
    const NProto::TError& e1,
    const NProto::TError& e2)
{
    return FAILED(e1.GetCode()) ? e1 : e2;
}

////////////////////////////////////////////////////////////////////////////////

class TWriteBatchActor final
    : public TActorBootstrapped<TWriteBatchActor>
{
private:
    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const ui64 CommitId;
    const TWriteRequestList WriteBatch;
    /*const*/ TVector<TMixedBlob> Blobs;
    /*const*/ TVector<TWriteRange> WriteRanges;

public:
    TWriteBatchActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TWriteRequestList writeBatch,
        TVector<TMixedBlob> blobs,
        TVector<TWriteRange> writeRanges);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

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

TWriteBatchActor::TWriteBatchActor(
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TWriteRequestList writeBatch,
        TVector<TMixedBlob> blobs,
        TVector<TWriteRange> writeRanges)
    : LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , WriteBatch(std::move(writeBatch))
    , Blobs(std::move(blobs))
    , WriteRanges(std::move(writeRanges))
{}

void TWriteBatchActor::Bootstrap(const TActorContext& ctx)
{
    WriteBlob(ctx);
    Become(&TThis::StateWork);
}

void TWriteBatchActor::WriteBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvWriteBlobRequest>(
        RequestInfo->CallContext
    );

    for (auto& blob: Blobs) {
        request->Blobs.emplace_back(blob.BlobId, std::move(blob.BlobContent));
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TWriteBatchActor::HandleWriteBlobResponse(
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

void TWriteBatchActor::AddBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvAddBlobRequest>(
        RequestInfo->CallContext
    );
    request->Mode = EAddBlobMode::WriteBatch;
    request->WriteRanges = std::move(WriteRanges);

    for (auto& blob: Blobs) {
        request->MixedBlobs.emplace_back(blob.BlobId, std::move(blob.Blocks));
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TWriteBatchActor::HandleAddBlobResponse(
    const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TWriteBatchActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TWriteBatchActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvWriteBatchCompleted;
        auto response =
            std::make_unique<TCompletion>(error, TSet<ui32>(), CommitId);
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        using TResponse = TEvIndexTabletPrivate::TEvWriteBatchResponse;
        auto response = std::make_unique<TResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    for (const auto& write: WriteBatch) {
        // reply to original request
        auto response = std::make_unique<TEvService::TEvWriteDataResponse>(
            PickError(write.Error, error));
        NCloud::Reply(ctx, *write.RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TWriteBatchActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(
            TEvIndexTabletPrivate::TEvWriteBlobResponse,
            HandleWriteBlobResponse);
        HFunc(TEvIndexTabletPrivate::TEvAddBlobResponse, HandleAddBlobResponse);

        default:
            HandleUnexpectedEvent(
                ev,
                TFileStoreComponents::TABLET_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleWriteBatch(
    const TEvIndexTabletPrivate::TEvWriteBatchRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto writeBatch = DequeueWriteBatch();
    if (!writeBatch) {
        LOG_TRACE(ctx, TFileStoreComponents::TABLET,
            "%s WriteBatch completed (nothing to do)",
            LogTag.c_str());

        if (ev->Sender != ctx.SelfID) {
            // reply to caller
            auto response =
                std::make_unique<TEvIndexTabletPrivate::TEvWriteBatchResponse>();
            NCloud::Reply(ctx, *ev, std::move(response));
        }
        return;
    }

    for (const auto& request: writeBatch) {
        AddTransaction(*request.RequestInfo, request.RequestInfo->CancelRoutine);
    }

    auto batchInfo = GetBatchInfo(writeBatch);
    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s WriteBatch started (%u bytes, %s)",
        LogTag.c_str(),
        batchInfo.Size,
        batchInfo.IsAligned ? "aligned" : "unaligned");

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    bool skipFresh = batchInfo.Size >= Config->GetWriteBlobThreshold()
        && batchInfo.IsAligned;
    ExecuteTx<TWriteBatch>(
        ctx,
        std::move(requestInfo),
        skipFresh,
        std::move(writeBatch));
}

void TIndexTabletActor::HandleWriteBatchCompleted(
    const TEvIndexTabletPrivate::TEvWriteBatchCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    if (FAILED(msg->GetError().GetCode())) {
        LOG_ERROR(ctx, TFileStoreComponents::TABLET,
            "%s WriteBatch failed (%s)",
            LogTag.c_str(),
            FormatError(msg->GetError()).c_str());
    } else {
        LOG_TRACE(ctx, TFileStoreComponents::TABLET,
            "%s WriteBatch completed (%s)",
            LogTag.c_str(),
            FormatError(msg->GetError()).c_str());
    }

    TABLET_VERIFY(TryReleaseCollectBarrier(msg->CommitId));

    WorkerActors.erase(ev->Sender);
    EnqueueBlobIndexOpIfNeeded(ctx);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_WriteBatch(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteBatch& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GetCurrentCommitId();

    bool ready = true;
    ui32 deltaBlocks = 0;

    TSet<ui32> rangeIds;
    for (auto& write: args.WriteBatch) {
        auto* session = FindSession(
            write.ClientId,
            write.SessionId,
            write.SessionSeqNo);
        if (!session) {
            write.Error = ErrorInvalidSession(
                write.ClientId,
                write.SessionId,
                write.SessionSeqNo);
            continue;
        }

        auto* handle = FindHandle(write.Handle);
        if (!handle || handle->Session != session) {
            write.Error = ErrorInvalidHandle(write.Handle);
            continue;
        }

        // TODO: access check

        write.NodeId = handle->GetNodeId();

        SplitRange(
            write.ByteRange.FirstBlock(),
            write.ByteRange.BlockCount(),
            BlockGroupSize,
            [&] (ui32 blockOffset, ui32 blocksCount) {
                rangeIds.insert(GetMixedRangeIndex(
                    write.NodeId,
                    write.ByteRange.FirstBlock() + blockOffset,
                    blocksCount));
            });

        auto& maxOffset = args.WriteRanges[write.NodeId];
        maxOffset = Max(maxOffset, write.ByteRange.End());

        // load inodes so we could check and update stats later
        auto it = args.Nodes.find(write.NodeId);
        if (it == args.Nodes.end()) {
            TMaybe<IIndexTabletDatabase::TNode> node;
            if (!ReadNode(db, write.NodeId, args.CommitId, node)) {
                ready = false;
                continue;
            }

            // TODO: access check
            TABLET_VERIFY(node);

            bool inserted = false;
            std::tie(it, inserted) = args.Nodes.insert(*node);
            TABLET_VERIFY(inserted);
        }

        if (it != args.Nodes.end()) {
            i64 delta = GetBlocksDifference(
                it->Attrs.GetSize(),
                write.ByteRange.End(),
                GetBlockSize());

            if (delta > 0) {
                deltaBlocks += delta;
            }
        }
    }

    if (!HasBlocksLeft(deltaBlocks)) {
        args.Error = ErrorNoSpaceLeft();
        return true;
    }

    for (ui32 rangeId: rangeIds) {
        if (!LoadMixedBlocks(db, rangeId)) {
            ready = false;
        }
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_WriteBatch(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TWriteBatch& args)
{
    if (args.SkipFresh) {
        // skip fresh completely for large writes
        return;
    }

    TIndexTabletDatabaseProxy db(tx.DB, args.NodeUpdates);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "WriteBatch");
    }

    for (auto& write: args.WriteBatch) {
        if (FAILED(write.Error.GetCode())) {
            continue;
        }

        MarkFreshBlocksDeleted(
            db,
            write.NodeId,
            args.CommitId,
            write.ByteRange.FirstAlignedBlock(),
            write.ByteRange.AlignedBlockCount());

        SplitRange(
            write.ByteRange.FirstAlignedBlock(),
            write.ByteRange.AlignedBlockCount(),
            BlockGroupSize,
            [&] (ui32 blockOffset, ui32 blocksCount) {
                MarkMixedBlocksDeleted(
                    db,
                    write.NodeId,
                    args.CommitId,
                    write.ByteRange.FirstAlignedBlock() + blockOffset,
                    blocksCount);
            });

        for (ui64 b = write.ByteRange.FirstAlignedBlock();
                b < write.ByteRange.FirstAlignedBlock() + write.ByteRange.AlignedBlockCount();
                ++b)
        {
            WriteFreshBlock(
                db,
                write.NodeId,
                args.CommitId,
                b,
                write.Buffer->GetBlock(b - write.ByteRange.FirstAlignedBlock()));
        }

        if (write.ByteRange.UnalignedHeadLength()) {
            WriteFreshBytes(
                db,
                write.NodeId,
                args.CommitId,
                write.ByteRange.Offset,
                write.Buffer->GetUnalignedHead()
            );
        }

        if (write.ByteRange.UnalignedTailLength()) {
            WriteFreshBytes(
                db,
                write.NodeId,
                args.CommitId,
                write.ByteRange.UnalignedTailOffset(),
                write.Buffer->GetUnalignedTail()
            );
        }
    }

    // update inode stats
    for (auto [id, maxOffset]: args.WriteRanges) {
        auto it = args.Nodes.find(id);
        TABLET_VERIFY(it != args.Nodes.end());

        auto attrs = CopyAttrs(it->Attrs, E_CM_CMTIME);
        if (maxOffset > attrs.GetSize()) {
            attrs.SetSize(maxOffset);
        }

        UpdateNode(
            db,
            id,
            it->MinCommitId,
            args.CommitId,
            attrs,
            it->Attrs);
    }
}

void TIndexTabletActor::CompleteTx_WriteBatch(
    const TActorContext& ctx,
    TTxIndexTablet::TWriteBatch& args)
{
    for (const auto& request: args.WriteBatch) {
        RemoveTransaction(*request.RequestInfo);
    }

    auto reply = [] (
        const TActorContext& ctx,
        TTxIndexTablet::TWriteBatch& args)
    {
        FILESTORE_TRACK(
            ResponseSent_Tablet,
            args.RequestInfo->CallContext,
            "WriteBatch");

        if (args.RequestInfo->Sender != ctx.SelfID) {
            // reply to caller
            auto response = std::make_unique<TEvIndexTabletPrivate::TEvWriteBatchResponse>();
            NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        }

        for (const auto& write: args.WriteBatch) {
            FILESTORE_TRACK(
                ResponseSent_Tablet,
                write.RequestInfo->CallContext,
                "WriteData");

            // reply to original request
            auto response = std::make_unique<TEvService::TEvWriteDataResponse>(write.Error);
            NCloud::Reply(ctx, *write.RequestInfo, std::move(response));
        }
    };

    if (!args.SkipFresh) {
        LOG_TRACE(ctx, TFileStoreComponents::TABLET,
            "%s WriteBatch completed (fresh)",
            LogTag.c_str());

        reply(ctx, args);

        EnqueueFlushIfNeeded(ctx);
        EnqueueBlobIndexOpIfNeeded(ctx);
        return;
    }

    TVector<TFreshBlock> blocks;
    for (const auto& write: args.WriteBatch) {
        TABLET_VERIFY(write.ByteRange.IsAligned());

        for (ui64 b = write.ByteRange.FirstAlignedBlock();
                b < write.ByteRange.FirstAlignedBlock() + write.ByteRange.AlignedBlockCount();
                ++b)
        {
            TBlock block {
                write.NodeId,
                IntegerCast<ui32>(b),
                // correct CommitId will be assigned later in AddBlobs
                InvalidCommitId,
                InvalidCommitId
            };

            blocks.push_back(TFreshBlock{
                block,
                write.Buffer->GetBlock(b - write.ByteRange.FirstAlignedBlock())
            });
        }
    }

    // it is important to keep order of writes
    StableSort(blocks, TBlockCompare());

    TMixedBlobBuilder builder(
        GetRangeIdHasher(),
        GetBlockSize(),
        CalculateMaxBlocksInBlob(Config->GetMaxBlobSize(), GetBlockSize()));

    for (const auto& block: blocks) {
        builder.Accept(block, block.BlockData);
    }

    auto blobs = builder.Finish();
    TABLET_VERIFY(blobs);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "WriteBatch");
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

            for (auto& write: args.WriteBatch) {
                write.Error =
                    MakeError(E_FS_OUT_OF_SPACE, "failed to generate blobId");
            }

            reply(ctx, args);

            return;
        }
    }

    TVector<TWriteRange> writeRanges;
    writeRanges.reserve(args.WriteRanges.size());
    for (auto [node, maxOffset]: args.WriteRanges) {
        writeRanges.emplace_back(TWriteRange{node, maxOffset});
    }

    AcquireCollectBarrier(args.CommitId);

    auto actor = std::make_unique<TWriteBatchActor>(
        LogTag,
        ctx.SelfID,
        args.RequestInfo,
        args.CommitId,
        std::move(args.WriteBatch),
        std::move(blobs),
        std::move(writeRanges));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
