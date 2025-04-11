#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <util/generic/algorithm.h>
#include <util/generic/cast.h>
#include <util/generic/hash.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

bool ShouldReadBlobs(const TVector<TBlockDataRef>& blocks)
{
    for (const auto& block: blocks) {
        if (block.BlobId) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void ApplyBytes(const TBlockBytes& bytes, TStringBuf blockData)
{
    for (const auto& interval: bytes.Intervals) {
        memcpy(
            const_cast<char*>(blockData.data()) + interval.OffsetInBlock,
            interval.Data.data(),
            interval.Data.size());
    }
}

void ApplyBytes(
    const TString& LogTag,
    const TByteRange& byteRange,
    TVector<TBlockBytes> bytes,
    IBlockBuffer& buffer)
{
    TABLET_VERIFY(byteRange.IsAligned());
    for (ui32 i = 0; i < byteRange.AlignedBlockCount(); ++i) {
        ApplyBytes(bytes[i], buffer.GetBlock(i));
    }
}

////////////////////////////////////////////////////////////////////////////////

void FillDescribeDataResponse(
    const TTabletStorageInfo& info,
    const ui64 tabletId,
    const ui32 blockSize,
    const TByteRange& responseRange,
    const TTxIndexTablet::TReadData& args,
    NProtoPrivate::TDescribeDataResponse* record)
{
    // metadata

    record->SetFileSize(args.Node->Attrs.GetSize());

    // data

    using TBlocks = TVector<TBlockDataRef>;
    // using TMap to make responses more stable and, thus, easier to test
    TMap<TPartialBlobId, TBlocks> blobBlocks;
    NProtoPrivate::TFreshDataRange freshRange;
    for (ui64 i = 0; i < args.Blocks.size(); ++i) {
        const auto& block = args.Blocks[i];
        const ui64 curOffset = args.ActualRange().Offset + i * blockSize;
        const TByteRange blockByteRange(curOffset, blockSize, blockSize);
        if (!responseRange.Overlaps(blockByteRange)) {
            continue;
        }

        const auto& bytes = args.Bytes[i];

        if (!block.BlobId && block.MinCommitId) {
            // it's a fresh block
            if (freshRange.GetContent()) {
                ui64 endOffset =
                    freshRange.GetOffset() + freshRange.GetContent().size();
                if (endOffset < curOffset) {
                    *record->AddFreshDataRanges() = std::move(freshRange);
                    freshRange.Clear();
                }
            }

            if (!freshRange.GetContent()) {
                freshRange.SetOffset(curOffset);
            }

            // applying fresh byte ranges that intersect with our current
            // block
            auto target = args.Buffer->GetBlock(i);
            ApplyBytes(bytes, target);
            freshRange.MutableContent()->append(target);

            continue;
        }

        if (freshRange.GetContent()) {
            // adding our current fresh range to the response
            *record->AddFreshDataRanges() = std::move(freshRange);
            freshRange.Clear();
        }

        if (block.BlobId) {
            // it's a block that should be read from a blob
            blobBlocks[block.BlobId].push_back(block);
        }

        // adding all fresh byte ranges that intersect with our current
        // block to the response
        for (const auto& interval: bytes.Intervals) {
            auto& byteRange = *record->AddFreshDataRanges();
            byteRange.SetOffset(curOffset + interval.OffsetInBlock);
            byteRange.SetContent(interval.Data);
        }
    }

    if (freshRange.GetContent()) {
        *record->AddFreshDataRanges() = std::move(freshRange);
        freshRange.Clear();
    }

    for (const auto& x: blobBlocks) {
        auto& piece = *record->AddBlobPieces();
        LogoBlobIDFromLogoBlobID(
            MakeBlobId(tabletId, x.first),
            piece.MutableBlobId());

        piece.SetBSGroupId(info.GroupFor(
            x.first.Channel(),
            x.first.Generation()));

        // joining adjacent ranges from this blob
        NProtoPrivate::TRangeInBlob rangeInBlob;
        ui32 prevBlobOffset = 0;
        for (const auto& b: x.second) {
            const auto curOffset =
                static_cast<ui64>(b.BlockIndex) * blockSize;
            if (rangeInBlob.GetLength()) {
                const ui64 endOffset =
                    rangeInBlob.GetOffset() + rangeInBlob.GetLength();

                // if either Offsets are not adjacent or BlobOffsets are
                // not adjacent, we should start a new range
                if (endOffset < curOffset
                        || prevBlobOffset + 1 != b.BlobOffset)
                {
                    *piece.AddRanges() = std::move(rangeInBlob);
                    rangeInBlob.Clear();
                }
            }

            if (!rangeInBlob.GetLength()) {
                rangeInBlob.SetOffset(curOffset);
                rangeInBlob.SetBlobOffset(b.BlobOffset * blockSize);
            }

            rangeInBlob.SetLength(rangeInBlob.GetLength() + blockSize);
            prevBlobOffset = b.BlobOffset;
        }

        if (rangeInBlob.GetLength()) {
            *piece.AddRanges() = std::move(rangeInBlob);
            rangeInBlob.Clear();
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

class TReadDataVisitor final
    : public IFreshBlockVisitor
    , public IMixedBlockVisitor
    , public ILargeBlockVisitor
    , public IFreshBytesVisitor
{
private:
    const TString& LogTag;
    TTxIndexTablet::TReadData& Args;
    bool ApplyingByteLayer = false;

public:
    TReadDataVisitor(const TString& logTag, TTxIndexTablet::TReadData& args)
        : LogTag(logTag)
        , Args(args)
    {
        TABLET_VERIFY(Args.ActualRange().IsAligned());
    }

    void Accept(const TBlock& block, TStringBuf blockData) override
    {
        TABLET_VERIFY(!ApplyingByteLayer);

        ui32 blockOffset = block.BlockIndex - Args.ActualRange().FirstBlock();
        TABLET_VERIFY(blockOffset < Args.ActualRange().BlockCount());

        auto& prev = Args.Blocks[blockOffset];
        if (Update(prev, block, {}, 0)) {
            Args.Buffer->SetBlock(blockOffset, blockData);
        }
    }

    void Accept(
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui32 blobOffset) override
    {
        TABLET_VERIFY(!ApplyingByteLayer);
        TABLET_VERIFY(blobId);

        ui32 blockOffset = block.BlockIndex - Args.ActualRange().FirstBlock();
        TABLET_VERIFY(blockOffset < Args.ActualRange().BlockCount());

        auto& prev = Args.Blocks[blockOffset];
        if (Update(prev, block, blobId, blobOffset)) {
            Args.Buffer->ClearBlock(blockOffset);
        }
    }

    void Accept(const TBlockDeletion& deletion) override
    {
        TABLET_VERIFY(!ApplyingByteLayer);

        ui32 blockOffset = deletion.BlockIndex - Args.ActualRange().FirstBlock();
        TABLET_VERIFY(blockOffset < Args.ActualRange().BlockCount());

        auto& prev = Args.Blocks[blockOffset];
        if (prev.MinCommitId < deletion.CommitId) {
            prev = {};
            Args.Buffer->ClearBlock(blockOffset);
        }
    }

    void Accept(const TBytes& bytes, TStringBuf data) override
    {
        ApplyingByteLayer = true;

        const auto firstBlockOffset =
            Args.ActualRange().FirstBlock() * Args.ActualRange().BlockSize;
        ui64 i = 0;

        while (i < bytes.Length) {
            auto offset = bytes.Offset + i;
            auto relOffset = offset - firstBlockOffset;
            auto blockIndex = relOffset / Args.ActualRange().BlockSize;
            auto offsetInBlock =
                relOffset - blockIndex * Args.ActualRange().BlockSize;
            // FreshBytes should be organized in such a way that newer commits
            // for the same bytes will be visited later than older commits, so
            // tracking individual byte commit ids is not needed
            auto& prev = Args.Blocks[blockIndex];
            auto next = Min<ui32>(
                bytes.Length,
                (blockIndex + 1) * Args.ActualRange().BlockSize
            );
            if (prev.MinCommitId < bytes.MinCommitId) {
                Args.Bytes[blockIndex].Intervals.push_back({
                    IntegerCast<ui32>(offsetInBlock),
                    TString(data.data() + i, next - i)
                });
            }

            i = next;
        }
    }

private:
    bool Update(
        TBlockDataRef& prev,
        const TBlock& block,
        const TPartialBlobId& blobId,
        ui32 blobOffset)
    {
        if (prev.MinCommitId < block.MinCommitId) {
            memcpy(&prev, &block, sizeof(TBlock));
            prev.BlobId = blobId;
            prev.BlobOffset = blobOffset;
            return true;
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TReadDataActor final
    : public TActorBootstrapped<TReadDataActor>
{
private:
    const ITraceSerializerPtr TraceSerializer;
    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;
    const ui64 CommitId;
    const TByteRange OriginByteRange;
    const TByteRange ActualRange;
    const ui64 TotalSize;
    const TVector<TBlockDataRef> Blocks;
    TVector<TBlockBytes> Bytes;
    const IBlockBufferPtr Buffer;
    /*const*/ TSet<ui32> MixedBlocksRanges;

public:
    TReadDataActor(
        ITraceSerializerPtr traceSerializer,
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TByteRange originByteRange,
        TByteRange actualRange,
        ui64 totalSize,
        TVector<TBlockDataRef> blocks,
        TVector<TBlockBytes> bytes,
        IBlockBufferPtr buffer,
        TSet<ui32> mixedBlocksRanges);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void ReadBlob(const TActorContext& ctx);
    void HandleReadBlobResponse(
        const TEvIndexTabletPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void ReplyAndDie(
        const TActorContext& ctx,
        const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TReadDataActor::TReadDataActor(
        ITraceSerializerPtr traceSerializer,
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TByteRange originByteRange,
        TByteRange actualRange,
        ui64 totalSize,
        TVector<TBlockDataRef> blocks,
        TVector<TBlockBytes> bytes,
        IBlockBufferPtr buffer,
        TSet<ui32> mixedBlocksRanges)
    : TraceSerializer(std::move(traceSerializer))
    , LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , OriginByteRange(originByteRange)
    , ActualRange(actualRange)
    , TotalSize(totalSize)
    , Blocks(std::move(blocks))
    , Bytes(std::move(bytes))
    , Buffer(std::move(buffer))
    , MixedBlocksRanges(std::move(mixedBlocksRanges))
{
    TABLET_VERIFY(ActualRange.IsAligned());
}

void TReadDataActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "ReadData");

    ReadBlob(ctx);
    Become(&TThis::StateWork);
}

void TReadDataActor::ReadBlob(const TActorContext& ctx)
{
    using TBlocksByBlob = THashMap<
        TPartialBlobId,
        TVector<TReadBlob::TBlock>,
        TPartialBlobIdHash
    >;

    TBlocksByBlob blocksByBlob;

    ui32 blockOffset = 0;
    for (const auto& block: Blocks) {
        ++blockOffset;

        if (!block.BlobId) {
            continue;
        }

        blocksByBlob[block.BlobId].emplace_back(block.BlobOffset, blockOffset - 1);
    }

    auto request = std::make_unique<TEvIndexTabletPrivate::TEvReadBlobRequest>(
        RequestInfo->CallContext);
    request->Buffer = Buffer;

    auto comparer = [] (const auto& l, const auto& r) {
        return l.BlobOffset < r.BlobOffset;
    };

    for (auto& [blobId, blocks]: blocksByBlob) {
        Sort(blocks, comparer);
        request->Blobs.emplace_back(blobId, std::move(blocks));
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TReadDataActor::HandleReadBlobResponse(
    const TEvIndexTabletPrivate::TEvReadBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ApplyBytes(LogTag, ActualRange, std::move(Bytes), *Buffer);
    ReplyAndDie(ctx, msg->GetError());
}

void TReadDataActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "tablet is shutting down"));
}

void TReadDataActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvReadDataCompleted;
        auto response = std::make_unique<TCompletion>(
            error,
            std::move(MixedBlocksRanges),
            CommitId,
            1,
            OriginByteRange.Length,
            ctx.Now() - RequestInfo->StartedTs);

        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "ReadData");

    if (RequestInfo->Sender != Tablet) {
        // reply to caller
        auto response = std::make_unique<TEvService::TEvReadDataResponse>(error);
        if (SUCCEEDED(error.GetCode())) {
            CopyFileData(
                LogTag,
                OriginByteRange,
                ActualRange,
                TotalSize,
                *Buffer,
                response->Record.MutableBuffer());
        }

        LOG_DEBUG(ctx, TFileStoreComponents::TABLET_WORKER,
            "%s ReadData: #%lu completed (%s)",
            LogTag.c_str(),
            RequestInfo->CallContext->RequestId,
            FormatError(response->Record.GetError()).c_str());

        BuildTraceInfo(
            TraceSerializer,
            RequestInfo->CallContext,
            response->Record);
        BuildThrottlerInfo(*RequestInfo->CallContext, response->Record);

        NCloud::Reply(ctx, *RequestInfo, std::move(response));
    }

    Die(ctx);
}

STFUNC(TReadDataActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTabletPrivate::TEvReadBlobResponse, HandleReadBlobResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TReadRequest>
NProto::TError ValidateRequest(
    const TReadRequest& request,
    ui32 blockSize,
    ui32 maxFileBlocks)
{
    const TByteRange range(
        request.GetOffset(),
        request.GetLength(),
        blockSize
    );

    if (auto error = ValidateRange(range, maxFileBlocks); HasError(error)) {
        return error;
    }

    return {};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleReadData(
    const TEvService::TEvReadDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto validator = [&] (const NProto::TReadDataRequest& request) {
        return ValidateRequest(
            request,
            GetBlockSize(),
            Config->GetMaxFileBlocks());
    };

    if (!AcceptRequest<TEvService::TReadDataMethod>(ev, ctx, validator)) {
        return;
    }

    // either rejected or put in the queue
    if (ThrottleIfNeeded<TEvService::TReadDataMethod>(ev, ctx)) {
        return;
    }

    auto* msg = ev->Get();
    const TByteRange byteRange(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        GetBlockSize()
    );

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    AddTransaction<TEvService::TReadDataMethod>(*requestInfo);

    TByteRange alignedByteRange = byteRange.AlignedSuperRange();
    auto blockBuffer = CreateBlockBuffer(alignedByteRange);

    ExecuteTx<TReadData>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        byteRange,
        alignedByteRange,
        std::move(blockBuffer),
        false /* describeOnly */);
}

void TIndexTabletActor::HandleReadDataCompleted(
    const TEvIndexTabletPrivate::TEvReadDataCompleted::TPtr& ev,
    const TActorContext&)
{
    const auto* msg = ev->Get();

    ReleaseMixedBlocks(msg->MixedBlocksRanges);
    TABLET_VERIFY(TryReleaseCollectBarrier(msg->CommitId));
    WorkerActors.erase(ev->Sender);

    Metrics.ReadData.Update(msg->Count, msg->Size, msg->Time);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleDescribeData(
    const TEvIndexTablet::TEvDescribeDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto validator = [&] (const NProtoPrivate::TDescribeDataRequest& request) {
        return ValidateRequest(
            request,
            GetBlockSize(),
            Config->GetMaxFileBlocks());
    };

    if (!AcceptRequest<TEvIndexTablet::TDescribeDataMethod>(ev, ctx, validator)) {
        return;
    }

    if (Config->GetMultipleStageRequestThrottlingEnabled() &&
        ThrottleIfNeeded<TEvIndexTablet::TDescribeDataMethod>(ev, ctx))
    {
        return;
    }

    auto* msg = ev->Get();
    const TByteRange byteRange(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        GetBlockSize()
    );

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    auto* handle = FindHandle(msg->Record.GetHandle());
    const ui64 nodeId = handle ? handle->GetNodeId() : InvalidNodeId;
    NProtoPrivate::TDescribeDataResponse result;
    const bool filled = TryFillDescribeResult(
        nodeId,
        msg->Record.GetHandle(),
        byteRange,
        &result);

    if (filled) {
        RegisterDescribe(nodeId, msg->Record.GetHandle(), byteRange);

        auto response =
            std::make_unique<TEvIndexTablet::TEvDescribeDataResponse>();
        response->Record = std::move(result);

        CompleteResponse<TEvIndexTablet::TDescribeDataMethod>(
            response->Record,
            requestInfo->CallContext,
            ctx);

        NCloud::Reply(ctx, *requestInfo, std::move(response));

        Metrics.ReadAheadCacheHitCount.fetch_add(1, std::memory_order_relaxed);
        Metrics.DescribeData.Update(
            1,
            byteRange.Length,
            ctx.Now() - requestInfo->StartedTs);

        return;
    }

    AddTransaction<TEvIndexTablet::TDescribeDataMethod>(*requestInfo);

    TByteRange alignedByteRange = byteRange.AlignedSuperRange();
    auto blockBuffer = CreateLazyBlockBuffer(alignedByteRange);

    ExecuteTx<TReadData>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        byteRange,
        alignedByteRange,
        std::move(blockBuffer),
        true /* describeOnly */);
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::ValidateTx_ReadData(
    const TActorContext& ctx,
    TTxIndexTablet::TReadData& args)
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
        return false;
    }

    auto* handle = FindHandle(args.Handle);
    if (!handle || handle->Session != session) {
        args.Error = ErrorInvalidHandle(args.Handle);
        return false;
    }

    if (!HasFlag(handle->GetFlags(), NProto::TCreateHandleRequest::E_READ)) {
        args.Error = ErrorInvalidHandle(args.Handle);
        return false;
    }

    args.NodeId = handle->GetNodeId();
    args.CommitId = handle->GetCommitId();
    if (args.DescribeOnly) {
        // initializing args.ReadAheadRange in an ugly way since TMaybe doesn't
        // support classes which don't have an assignment operator
        auto readAheadRange = RegisterDescribe(
            args.NodeId,
            args.Handle,
            args.AlignedByteRange);
        if (readAheadRange.Defined()) {
            args.ReadAheadRange.ConstructInPlace(*readAheadRange);
            args.Blocks.resize(args.ActualRange().BlockCount());
            args.Bytes.resize(args.ActualRange().BlockCount());
            args.Buffer = CreateLazyBlockBuffer(args.ActualRange());
        }
    }

    LOG_DEBUG(ctx, TFileStoreComponents::TABLET,
        "%s[%s] ReadNodeData @%lu [%lu] %s",
        LogTag.c_str(),
        session->GetSessionId().c_str(),
        args.Handle,
        args.NodeId,
        args.ActualRange().Describe().c_str());

    if (args.CommitId == InvalidCommitId) {
        args.CommitId = GetCurrentCommitId();
    }

    return true;
}

bool TIndexTabletActor::PrepareTx_ReadData(
    const TActorContext& ctx,
    IIndexTabletDatabase& db,
    TTxIndexTablet::TReadData& args)
{
    Y_UNUSED(ctx);

    bool ready = true;
    if (!ReadNode(db, args.NodeId, args.CommitId, args.Node)) {
        ready = false;
    } else {
        TABLET_VERIFY(args.Node);
        // TODO: access check
    }

    TSet<ui32> ranges;
    SplitRange(
        args.ActualRange().FirstBlock(),
        args.ActualRange().BlockCount(),
        BlockGroupSize,
        [&] (ui32 blockOffset, ui32 blocksCount) {
            ranges.insert(GetMixedRangeIndex(
                args.NodeId,
                IntegerCast<ui32>(args.ActualRange().FirstBlock() + blockOffset),
                blocksCount));
        });

    for (ui32 rangeId: ranges) {
        if (!args.MixedBlocksRanges.count(rangeId)) {
            if (LoadMixedBlocks(db, rangeId)) {
                args.MixedBlocksRanges.insert(rangeId);
            } else {
                ready = false;
            }
        }
    }

    if (!ready) {
        return false;
    }

    TReadDataVisitor visitor(LogTag, args);

    FindFreshBlocks(
        visitor,
        args.NodeId,
        args.CommitId,
        args.ActualRange().FirstBlock(),
        args.ActualRange().BlockCount());

    SplitRange(
        args.ActualRange().FirstBlock(),
        args.ActualRange().BlockCount(),
        BlockGroupSize,
        [&] (ui32 blockOffset, ui32 blocksCount) {
            FindMixedBlocks(
                visitor,
                args.NodeId,
                args.CommitId,
                IntegerCast<ui32>(args.ActualRange().FirstBlock() + blockOffset),
                blocksCount);
        });

    FindLargeBlocks(
        visitor,
        args.NodeId,
        args.CommitId,
        args.ActualRange().FirstBlock(),
        args.ActualRange().BlockCount());

    // calling FindFreshBytes after FindFreshBlocks and FindMixedBlocks is
    // important since we compare bytes.MinCommitId with the corresponding
    // blocks' MinCommitIds and keep only those fresh byte ranges whose
    // MinCommitIds are newer than block MinCommitIds
    FindFreshBytes(
        visitor,
        args.NodeId,
        args.CommitId,
        args.ActualRange());

    return true;
}

void TIndexTabletActor::CompleteTx_ReadData(
    const TActorContext& ctx,
    TTxIndexTablet::TReadData& args)
{
    RemoveTransaction(*args.RequestInfo);

    Y_DEFER {
        ReleaseMixedBlocks(args.MixedBlocksRanges);
    };

    if (args.DescribeOnly && !HasError(args.Error)) {
        if (args.ReadAheadRange) {
            NProtoPrivate::TDescribeDataResponse record;
            FillDescribeDataResponse(
                *Info(),
                TabletID(),
                GetBlockSize(),
                *args.ReadAheadRange,
                args,
                &record);
            RegisterReadAheadResult(
                args.NodeId,
                args.Handle,
                *args.ReadAheadRange,
                record);
        }

        auto response =
            std::make_unique<TEvIndexTablet::TEvDescribeDataResponse>();
        auto& record = response->Record;
        FillDescribeDataResponse(
            *Info(),
            TabletID(),
            GetBlockSize(),
            args.AlignedByteRange,
            args,
            &record);

        CompleteResponse<TEvIndexTablet::TDescribeDataMethod>(
            response->Record,
            args.RequestInfo->CallContext,
            ctx);

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));

        Metrics.DescribeData.Update(
            1,
            args.OriginByteRange.Length,
            ctx.Now() - args.RequestInfo->StartedTs);

        return;
    }

    if (HasError(args.Error)) {
        if (args.DescribeOnly) {
            auto response =
                std::make_unique<TEvIndexTablet::TEvDescribeDataResponse>(args.Error);
            CompleteResponse<TEvIndexTablet::TDescribeDataMethod>(
                response->Record,
                args.RequestInfo->CallContext,
                ctx);

            NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        } else {
            auto response =
                std::make_unique<TEvService::TEvReadDataResponse>(args.Error);
            CompleteResponse<TEvService::TReadDataMethod>(
                response->Record,
                args.RequestInfo->CallContext,
                ctx);

            NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        }

        return;
    }

    if (!ShouldReadBlobs(args.Blocks)) {
        ApplyBytes(
            LogTag,
            args.ActualRange(),
            std::move(args.Bytes),
            *args.Buffer);

        auto response = std::make_unique<TEvService::TEvReadDataResponse>();
        CopyFileData(
            LogTag,
            args.OriginByteRange,
            args.ActualRange(),
            args.Node->Attrs.GetSize(),
            *args.Buffer,
            response->Record.MutableBuffer());

        CompleteResponse<TEvService::TReadDataMethod>(
            response->Record,
            args.RequestInfo->CallContext,
            ctx);

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
        return;
    }

    AcquireCollectBarrier(args.CommitId);

    auto actor = std::make_unique<TReadDataActor>(
        TraceSerializer,
        LogTag,
        ctx.SelfID,
        args.RequestInfo,
        args.CommitId,
        args.OriginByteRange,
        args.ActualRange(),
        args.Node->Attrs.GetSize(),
        std::move(args.Blocks),
        std::move(args.Bytes),
        std::move(args.Buffer),
        std::move(args.MixedBlocksRanges));

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
