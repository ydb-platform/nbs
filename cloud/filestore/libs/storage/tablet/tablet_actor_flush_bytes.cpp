#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/model/block_buffer.h>
#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/model/profile_log_events.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TReadBlockVisitor final
    : public IFreshBlockVisitor
    , public IMixedBlockVisitor
    , public ILargeBlockVisitor
    , public IFreshBytesVisitor
{
private:
    const TString LogTag;
    const ui32 BlockSize;
    TBlockWithBytes& Block;
    ui64 BlockMinCommitId = 0;
    bool ApplyingByteLayer = false;

public:
    TReadBlockVisitor(TString logTag, ui32 blockSize, TBlockWithBytes& block)
        : LogTag(std::move(logTag))
        , BlockSize(blockSize)
        , Block(block)
    {
    }

    void Accept(const TBlock& block, TStringBuf blockData) override
    {
        TABLET_VERIFY(!ApplyingByteLayer);

        if (block.MinCommitId > BlockMinCommitId) {
            BlockMinCommitId = block.MinCommitId;
            TOwningFreshBlock ofb;
            static_cast<TBlock&>(ofb) = block;
            ofb.BlockData = blockData;
            Block.Block = std::move(ofb);
        }
    }

    void Accept(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui32 blobOffset) override
    {
        TABLET_VERIFY(!ApplyingByteLayer);

        if (block.MinCommitId > BlockMinCommitId) {
            BlockMinCommitId = block.MinCommitId;
            TBlockDataRef ref;
            static_cast<TBlock&>(ref) = block;
            ref.BlobId = blobId;
            ref.BlobOffset = blobOffset;
            Block.Block = std::move(ref);
        }
    }

    void Accept(const TBlockDeletion& deletion) override
    {
        TABLET_VERIFY(!ApplyingByteLayer);

        if (BlockMinCommitId < deletion.CommitId) {
            Block.Block = {};
        }
    }

    void Accept(const TBytes& bytes, TStringBuf data) override
    {
        ApplyingByteLayer = true;

        if (bytes.MinCommitId > BlockMinCommitId) {
            const auto bytesOffset =
                static_cast<ui64>(Block.BlockIndex) * BlockSize;
            Block.BytesMinCommitId = bytes.MinCommitId;
            TABLET_VERIFY_C(
                bytes.Offset - bytesOffset <= BlockSize,
                "Bytes: " << bytes.Describe()
                    << ", Block: " << Block.BlockIndex
                    << ", BlockSize: " << BlockSize);
            Block.BlockBytes.Intervals.push_back({
                IntegerCast<ui32>(bytes.Offset - bytesOffset),
                TString(data)
            });
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TFlushBytesActor final
    : public TActorBootstrapped<TFlushBytesActor>
{
private:
    const TString LogTag;
    const TString FileSystemId;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const ui64 CommitId;
    const ui32 BlockSize;
    const ui64 ChunkId;
    const IProfileLogPtr ProfileLog;
    NProto::TProfileLogRequestInfo ProfileLogRequest;
    TVector<TMixedBlobMeta> SrcBlobs;
    TVector<TMixedBlobMeta> SrcBlobsToRead;
    TVector<TVector<ui32>> SrcBlobOffsets;
    const TVector<TFlushBytesBlob> DstBlobs;
    /* const */ TSet<ui32> MixedBlocksRanges;
    ui32 OperationSize = 0;

    THashMap<TPartialBlobId, IBlockBufferPtr, TPartialBlobIdHash> Buffers;
    using TBlobOffsetMap =
        THashMap<TBlockLocationInBlob, ui32, TBlockLocationInBlobHash>;
    TBlobOffsetMap SrcBlobOffset2DstBlobOffset;

    size_t RequestsInFlight = 0;

public:
    TFlushBytesActor(
        TString logTag,
        TString fileSystemId,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        ui32 blockSize,
        ui64 chunkId,
        IProfileLogPtr profileLog,
        NProto::TProfileLogRequestInfo profileLogRequest,
        TVector<TMixedBlobMeta> srcBlobs,
        TVector<TMixedBlobMeta> srcBlobsToRead,
        TVector<TVector<ui32>> srcBlobOffsets,
        TVector<TFlushBytesBlob> dstBlobs,
        TSet<ui32> mixedBlocksRanges);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ReadBlobs(const TActorContext& ctx);
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

TFlushBytesActor::TFlushBytesActor(
        TString logTag,
        TString fileSystemId,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        ui32 blockSize,
        ui64 chunkId,
        IProfileLogPtr profileLog,
        NProto::TProfileLogRequestInfo profileLogRequest,
        TVector<TMixedBlobMeta> srcBlobs,
        TVector<TMixedBlobMeta> srcBlobsToRead,
        TVector<TVector<ui32>> srcBlobOffsets,
        TVector<TFlushBytesBlob> dstBlobs,
        TSet<ui32> mixedBlocksRanges)
    : LogTag(std::move(logTag))
    , FileSystemId(std::move(fileSystemId))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , BlockSize(blockSize)
    , ChunkId(chunkId)
    , ProfileLog(std::move(profileLog))
    , ProfileLogRequest(std::move(profileLogRequest))
    , SrcBlobs(std::move(srcBlobs))
    , SrcBlobsToRead(std::move(srcBlobsToRead))
    , SrcBlobOffsets(std::move(srcBlobOffsets))
    , DstBlobs(std::move(dstBlobs))
    , MixedBlocksRanges(std::move(mixedBlocksRanges))
{
    for (const auto& b: DstBlobs) {
        for (const auto& block: b.Blocks) {
            for (const auto& i: block.BlockBytes.Intervals) {
                OperationSize += i.Data.size();
            }
        }
    }
}

void TFlushBytesActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "FlushBytes");

    Become(&TThis::StateWork);

    ReadBlobs(ctx);
    if (!RequestsInFlight) {
        WriteBlob(ctx);
    }
}

void TFlushBytesActor::ReadBlobs(const TActorContext& ctx)
{
    for (ui32 i = 0; i < SrcBlobsToRead.size(); ++i) {
        auto& blobToRead = SrcBlobsToRead[i];
        auto& blobOffsets = SrcBlobOffsets[i];
        TVector<TReadBlob::TBlock> blocks(Reserve(blobToRead.Blocks.size()));

        ui32 blockOffset = 0;

        for (ui32 j = 0; j < blobToRead.Blocks.size(); ++j) {
            const auto& block = blobToRead.Blocks[j];
            if (block.MinCommitId < block.MaxCommitId) {
                TBlockLocationInBlob key{blobToRead.BlobId, blobOffsets[j]};
                SrcBlobOffset2DstBlobOffset[key] = blockOffset;

                blocks.emplace_back(blobOffsets[j], blockOffset++);
            }
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
            request->Blobs.emplace_back(blobToRead.BlobId, std::move(blocks));
            request->Blobs.back().Async = true;

            Buffers[blobToRead.BlobId] = request->Buffer;

            NCloud::Send(ctx, Tablet, std::move(request));
            ++RequestsInFlight;
        }
    }
}

void TFlushBytesActor::HandleReadBlobResponse(
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

void TFlushBytesActor::WriteBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvWriteBlobRequest>(
        RequestInfo->CallContext
    );

    for (const auto& blob: DstBlobs) {
        TString blobContent(Reserve(BlockSize * blob.Blocks.size()));

        for (const auto& block: blob.Blocks) {
            blobContent.append(BlockSize, 0);
            TStringBuf blockContent =
                TStringBuf(blobContent).substr(blobContent.size() - BlockSize);
            if (auto* ref = std::get_if<TBlockDataRef>(&block.Block)) {
                TBlockLocationInBlob key{ref->BlobId, ref->BlobOffset};

                auto& buffer = Buffers[ref->BlobId];
                memcpy(
                    const_cast<char*>(blockContent.data()),
                    buffer->GetBlock(SrcBlobOffset2DstBlobOffset[key]).data(),
                    BlockSize
                );
            } else if (auto* fresh = std::get_if<TOwningFreshBlock>(&block.Block)) {
                memcpy(
                    const_cast<char*>(blockContent.data()),
                    fresh->BlockData.data(),
                    BlockSize
                );
            }

            for (const auto& interval: block.BlockBytes.Intervals) {
                memcpy(
                    const_cast<char*>(blockContent.data()) + interval.OffsetInBlock,
                    interval.Data.data(),
                    interval.Data.size()
                );
            }
        }

        request->Blobs.emplace_back(blob.BlobId, std::move(blobContent));
        request->Blobs.back().Async = true;
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TFlushBytesActor::HandleWriteBlobResponse(
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

void TFlushBytesActor::AddBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvAddBlobRequest>(
        RequestInfo->CallContext
    );
    request->Mode = EAddBlobMode::FlushBytes;
    request->SrcBlobs = std::move(SrcBlobs);

    THashMap<TBlockLocation, ui64, TBlockLocationHash> blockIndex2BytesCommitId;

    for (const auto& blob: DstBlobs) {
        TVector<TBlock> blocks(Reserve(blob.Blocks.size()));
        for (const auto& block: blob.Blocks) {
            ui64 maxCommitId = InvalidCommitId;

            if (auto* ref = std::get_if<TBlockDataRef>(&block.Block)) {
                maxCommitId = ref->MaxCommitId;
            } else if (auto* fresh = std::get_if<TOwningFreshBlock>(&block.Block)) {
                request->SrcBlocks.push_back(*fresh);
                maxCommitId = fresh->MaxCommitId;
            }

            blocks.emplace_back(
                block.NodeId,
                block.BlockIndex,
                block.BytesMinCommitId,
                maxCommitId
            );

            TBlockLocation key{block.NodeId, block.BlockIndex};
            auto& commitId = blockIndex2BytesCommitId[key];
            TABLET_VERIFY(commitId == 0);

            commitId = block.BytesMinCommitId;
        }

        request->MixedBlobs.emplace_back(blob.BlobId, std::move(blocks));
    }

    for (auto& srcBlob: request->SrcBlobs) {
        for (auto& block: srcBlob.Blocks) {
            TBlockLocation key{block.NodeId, block.BlockIndex};
            if (auto p = blockIndex2BytesCommitId.FindPtr(key)) {
                block.MaxCommitId = *p;
            }
        }
    }

    for (auto& block: request->SrcBlocks) {
        TBlockLocation key{block.NodeId, block.BlockIndex};
        if (auto p = blockIndex2BytesCommitId.FindPtr(key)) {
            block.MaxCommitId = *p;
        }
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TFlushBytesActor::HandleAddBlobResponse(
    const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TFlushBytesActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TFlushBytesActor::ReplyAndDie(
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
        using TCompletion = TEvIndexTabletPrivate::TEvFlushBytesCompleted;
        auto response = std::make_unique<TCompletion>(
            error,
            1,
            OperationSize,
            ctx.Now() - RequestInfo->StartedTs,
            RequestInfo->CallContext,
            std::move(MixedBlocksRanges),
            CommitId,
            ChunkId);

        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "FlushBytes");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response =
            std::make_unique<TEvIndexTabletPrivate::TEvFlushBytesResponse>(error);
        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TFlushBytesActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTabletPrivate::TEvReadBlobResponse, HandleReadBlobResponse);
        HFunc(TEvIndexTabletPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
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

void TIndexTabletActor::HandleFlushBytes(
    const TEvIndexTabletPrivate::TEvFlushBytesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    FILESTORE_TRACK(
        BackgroundTaskStarted_Tablet,
        msg->CallContext,
        "FlushBytes",
        msg->CallContext->FileSystemId,
        GetFileSystem().GetStorageMediaKind());

    auto reply = [] (
        const TActorContext& ctx,
        auto& ev,
        const NProto::TError& error)
    {
        FILESTORE_TRACK(
            ResponseSent_Tablet,
            ev.Get()->CallContext,
            "FlushBytes");

        if (ev.Sender != ctx.SelfID) {
            // reply to caller
            auto response =
                std::make_unique<TEvIndexTabletPrivate::TEvFlushBytesResponse>(error);
            NCloud::Reply(ctx, ev, std::move(response));
        }
    };

    const bool started = ev->Sender == ctx.SelfID
        ? StartBackgroundBlobIndexOp() : BlobIndexOpState.Start();

    if (!started) {
        if (FlushState.Start()) {
            FlushState.Complete();
        }

        reply(
            ctx,
            *ev,
            MakeError(E_TRY_AGAIN, "cleanup/compaction is in progress")
        );

        return;
    }

    if (!FlushState.Start()) {
        CompleteBlobIndexOp();

        reply(ctx, *ev, MakeError(E_TRY_AGAIN, "flush is in progress"));

        return;
    }

    if (!CompactionStateLoadStatus.Finished) {
        CompleteBlobIndexOp();
        FlushState.Complete();

        reply(
            ctx,
            *ev,
            MakeError(E_TRY_AGAIN, "compaction state not loaded yet")
        );

        return;
    }

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s FlushBytes started",
        LogTag.c_str());

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    TVector<TBytes> bytes;
    // deletionMarkers won't be needed in the transactions - the actual localdb
    // cleanup will use the markers stored in TFreshBytes via the FinishCleanup
    // call which uses TFreshBytes::VisitTop
    TVector<TBytes> deletionMarkers;
    auto cleanupInfo = StartFlushBytes(&bytes, &deletionMarkers);

    if (bytes.empty()) {
        if (deletionMarkers.empty()) {
            CompleteBlobIndexOp();
            FlushState.Complete();
            EnqueueBlobIndexOpIfNeeded(ctx);
            EnqueueFlushIfNeeded(ctx);

            reply(ctx, *ev, MakeError(S_FALSE, "no bytes to flush"));

            return;
        }

        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s FlushBytes: only deletion markers found, trimming",
            LogTag.c_str());

        reply(ctx, *ev, {});

        ExecuteTx<TTrimBytes>(
            ctx,
            std::move(requestInfo),
            cleanupInfo.ChunkId);

        return;
    }

    ExecuteTx<TFlushBytes>(
        ctx,
        std::move(requestInfo),
        cleanupInfo.ClosingCommitId,
        cleanupInfo.ChunkId,
        std::move(bytes)
    );
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_FlushBytes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TFlushBytes& args)
{
    TIndexTabletDatabase db(tx.DB);

    InitProfileLogRequestInfo(args.ProfileLogRequest, ctx.Now());

    bool ready = true;
    for (const auto& bytes: args.Bytes) {
        ui32 rangeId = GetMixedRangeIndex(bytes.NodeId, bytes.Offset / GetBlockSize());
        if (!args.MixedBlocksRanges.count(rangeId)) {
            if (LoadMixedBlocks(db, rangeId)) {
                args.MixedBlocksRanges.insert(rangeId);
            } else {
                ready = false;
            }
        }
    }

    return ready;
}

void TIndexTabletActor::ExecuteTx_FlushBytes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TFlushBytes& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);

    for (const auto& bytes: args.Bytes) {
        InvalidateReadAheadCache(bytes.NodeId);
    }
}

void TIndexTabletActor::CompleteTx_FlushBytes(
    const TActorContext& ctx,
    TTxIndexTablet::TFlushBytes& args)
{
    auto replyError = [&] (
        const TActorContext& ctx,
        TTxIndexTablet::TFlushBytes& args,
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
            "FlushBytes");

        ReleaseMixedBlocks(args.MixedBlocksRanges);

        if (args.RequestInfo->Sender != ctx.SelfID) {
            // reply to caller
            auto response =
                std::make_unique<TEvIndexTabletPrivate::TEvFlushBytesResponse>(error);
            NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        }
    };



    THashMap<TBlockLocation, TBlockWithBytes, TBlockLocationHash> blockMap;

    struct TSrcBlobInfo
    {
        TMixedBlobMeta SrcBlob;
        TMixedBlobMeta SrcBlobToRead;
        TVector<ui32> BlobOffsets;
    };
    THashMap<TPartialBlobId, TSrcBlobInfo, TPartialBlobIdHash> srcBlobMap;

    for (const auto& bytes: args.Bytes) {
        {
            auto* range = args.ProfileLogRequest.AddRanges();
            range->SetNodeId(bytes.NodeId);
            range->SetOffset(bytes.Offset);
            range->SetBytes(bytes.Length);
        }

        TByteRange byteRange(bytes.Offset, bytes.Length, GetBlockSize());
        TABLET_VERIFY(byteRange.BlockCount() == 1);
        ui32 blockIndex = byteRange.FirstBlock();
        TBlockLocation key{bytes.NodeId, blockIndex};

        if (blockMap.contains(key)) {
            // we have already processed this block and found all actual data
            continue;
        }

        TBlockWithBytes blockWithBytes;
        blockWithBytes.NodeId = bytes.NodeId;
        blockWithBytes.BlockIndex = blockIndex;

        TReadBlockVisitor visitor(LogTag, GetBlockSize(), blockWithBytes);

        FindFreshBlocks(
            visitor,
            bytes.NodeId,
            args.ReadCommitId,
            blockIndex,
            1);

        FindMixedBlocks(
            visitor,
            bytes.NodeId,
            args.ReadCommitId,
            blockIndex,
            1);

        FindLargeBlocks(
            visitor,
            bytes.NodeId,
            args.ReadCommitId,
            blockIndex,
            1);

        FindFreshBytes(
            visitor,
            bytes.NodeId,
            args.ReadCommitId,
            TByteRange::BlockRange(blockIndex, GetBlockSize()));

        if (blockWithBytes.BlockBytes.Intervals.empty()) {
            // this interval has been overwritten by a full block, skipping
            continue;
        }

        if (auto* ref = std::get_if<TBlockDataRef>(&blockWithBytes.Block)) {
            // the base block at blockIndex belongs to a blob - we need to read
            // its data from blobstorage and update this blob's blocklist upon
            // TxAddBlob

            auto& srcBlobInfo = srcBlobMap[ref->BlobId];
            if (!srcBlobInfo.SrcBlob.BlobId) {
                const auto rangeId = GetMixedRangeIndex(bytes.NodeId, blockIndex);
                srcBlobInfo.SrcBlob = FindBlob(rangeId, ref->BlobId);
                srcBlobInfo.SrcBlobToRead.BlobId = ref->BlobId;
            }
            srcBlobInfo.SrcBlobToRead.Blocks.push_back(
                static_cast<const TBlock&>(*ref)
            );
            srcBlobInfo.BlobOffsets.push_back(ref->BlobOffset);
        }

        // XXX not adding srcBlobs whose blocks were overwritten by fresh
        // blocks - relying on a future Flush here

        blockMap[key] = std::move(blockWithBytes);
    }

    if (blockMap.empty()) {
        // all bytes have been overwritten by full blocks, can trim straight away
        ExecuteTx<TTrimBytes>(
            ctx,
            args.RequestInfo,
            args.ChunkId);

        replyError(ctx, args, {});

        return;
    }

    TVector<TMixedBlobMeta> srcBlobs;
    TVector<TMixedBlobMeta> srcBlobsToRead;
    TVector<TVector<ui32>> srcBlobOffsets;
    for (auto& x: srcBlobMap) {
        srcBlobs.push_back(std::move(x.second.SrcBlob));
        srcBlobsToRead.push_back(std::move(x.second.SrcBlobToRead));
        srcBlobOffsets.push_back(std::move(x.second.BlobOffsets));
    }

    TVector<TBlockWithBytes> blocks;
    for (auto& x: blockMap) {
        blocks.push_back(std::move(x.second));
    }
    Sort(
        blocks.begin(),
        blocks.end(),
        TBlockWithBytesCompare());

    TFlushBytesBlobBuilder builder(
        GetRangeIdHasher(),
        CalculateMaxBlocksInBlob(Config->GetMaxBlobSize(), GetBlockSize()));

    for (auto& block: blocks) {
        builder.Accept(std::move(block));
    }

    auto dstBlobs = builder.Finish();
    TABLET_VERIFY(dstBlobs);

    args.CommitId = GenerateCommitId();
    if (args.CommitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "FlushBytes");
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
            FlushState.Complete();
            EnqueueBlobIndexOpIfNeeded(ctx);
            EnqueueFlushIfNeeded(ctx);

            return;
        }
    }

    AcquireCollectBarrier(args.CommitId);
    // TODO(#1923): it may be problematic to acquire the barrier only upon
    // completion of the transaction, because blobs, that have been read at the
    // prepare stage, may be tampered with by the time of the transaction
    // completion

    auto actor = std::make_unique<TFlushBytesActor>(
        LogTag,
        GetFileSystemId(),
        ctx.SelfID,
        args.RequestInfo,
        args.CommitId,
        GetBlockSize(),
        args.ChunkId,
        ProfileLog,
        std::move(args.ProfileLogRequest),
        std::move(srcBlobs),
        std::move(srcBlobsToRead),
        std::move(srcBlobOffsets),
        std::move(dstBlobs),
        std::move(args.MixedBlocksRanges));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleFlushBytesCompleted(
    const TEvIndexTabletPrivate::TEvFlushBytesCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s FlushBytes completed (%s)",
        LogTag.c_str(),
        FormatError(msg->GetError()).c_str());

    ReleaseMixedBlocks(msg->MixedBlocksRanges);
    TABLET_VERIFY(TryReleaseCollectBarrier(msg->CommitId));
    WorkerActors.erase(ev->Sender);

    Metrics.FlushBytes.Update(1, msg->Size, msg->Time);

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    FILESTORE_TRACK(
        BackgroundRequestReceived_Tablet,
        msg->CallContext,
        "TrimBytes");

    // FIXME: validate for errors
    ExecuteTx<TTrimBytes>(ctx, std::move(requestInfo), msg->ChunkId);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_TrimBytes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TTrimBytes& args)
{
    Y_UNUSED(tx);

    InitProfileLogRequestInfo(args.ProfileLogRequest, ctx.Now());

    return true;
}

void TIndexTabletActor::ExecuteTx_TrimBytes(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TTrimBytes& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    auto result = FinishFlushBytes(
        db,
        Config->GetTrimBytesItemCount(),
        args.ChunkId,
        args.ProfileLogRequest);

    args.TrimmedBytes = result.TotalBytesFlushed;
    args.TrimmedAll = result.ChunkCompleted;
}

void TIndexTabletActor::CompleteTx_TrimBytes(
    const TActorContext& ctx,
    TTxIndexTablet::TTrimBytes& args)
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
        "TrimBytes");

    Metrics.TrimBytes.Update(
        1,
        args.TrimmedBytes,
        ctx.Now() - args.RequestInfo->StartedTs);

    if (!args.TrimmedAll) {
        LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
            "%s TrimBytes partially completed (%lu, %lu)",
            LogTag.c_str(),
            args.ChunkId,
            args.TrimmedBytes);

        ExecuteTx<TTrimBytes>(
            ctx,
            args.RequestInfo,
            args.ChunkId);
        return;
    }

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s TrimBytes completed (%lu, %lu)",
        LogTag.c_str(),
        args.ChunkId,
        args.TrimmedBytes);

    CompleteBlobIndexOp();
    FlushState.Complete();

    EnqueueBlobIndexOpIfNeeded(ctx);
    EnqueueCollectGarbageIfNeeded(ctx);
    EnqueueFlushIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
