#include "tablet_actor.h"

#include "helpers.h"

#include <cloud/filestore/libs/diagnostics/throttler_info_serializer.h>
#include <cloud/filestore/libs/diagnostics/trace_serializer.h>
#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/iterator/zip.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteDataActor final
    : public TActorBootstrapped<TWriteDataActor>
{
private:
    const ITraceSerializerPtr TraceSerializer;

    const TString LogTag;
    const TActorId Tablet;
    const TRequestInfoPtr RequestInfo;

    const ui64 CommitId;
    /*const*/ TVector<TMergedBlob> Blobs;
    const TWriteRange WriteRange;
    ui32 BlobsSize = 0;

    // This parameter is used for a scenario, when there is already a blobId
    // with data written to. In this case, we don't need to send requests to
    // blobstorage. This field is used only for two-stage writes. For more info
    // see See #539
    bool ShouldSkipBlobStorage = false;

public:
    TWriteDataActor(
        ITraceSerializerPtr traceSerializer,
        TString logTag,
        TActorId tablet,
        TRequestInfoPtr requestInfo,
        ui64 commitId,
        TVector<TMergedBlob> blobs,
        TWriteRange writeRange,
        bool shouldSkipBlobStorage = false);

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

    void
    ReplyAndDie(const TActorContext& ctx, const NProto::TError& error = {});
};

////////////////////////////////////////////////////////////////////////////////

TWriteDataActor::TWriteDataActor(
    ITraceSerializerPtr traceSerializer,
    TString logTag,
    TActorId tablet,
    TRequestInfoPtr requestInfo,
    ui64 commitId,
    TVector<TMergedBlob> blobs,
    TWriteRange writeRange,
    bool shouldSkipBlobStorage)
    : TraceSerializer(std::move(traceSerializer))
    , LogTag(std::move(logTag))
    , Tablet(tablet)
    , RequestInfo(std::move(requestInfo))
    , CommitId(commitId)
    , Blobs(std::move(blobs))
    , WriteRange(writeRange)
    , ShouldSkipBlobStorage(shouldSkipBlobStorage)
{
    for (const auto& blob: Blobs) {
        BlobsSize += blob.BlobContent.Size();
    }
}

void TWriteDataActor::Bootstrap(const TActorContext& ctx)
{
    FILESTORE_TRACK(
        RequestReceived_TabletWorker,
        RequestInfo->CallContext,
        "WriteData");

    WriteBlob(ctx);
    Become(&TThis::StateWork);
}

void TWriteDataActor::WriteBlob(const TActorContext& ctx)
{
    if (ShouldSkipBlobStorage) {
        // In the case, when there is already a blobId with data written to,
        // bypass BlobStorage access
        AddBlob(ctx);
        return;
    }

    auto request = std::make_unique<TEvIndexTabletPrivate::TEvWriteBlobRequest>(
        RequestInfo->CallContext
    );

    for (auto& blob: Blobs) {
        request->Blobs.emplace_back(blob.BlobId, std::move(blob.BlobContent));
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TWriteDataActor::HandleWriteBlobResponse(
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

void TWriteDataActor::AddBlob(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvIndexTabletPrivate::TEvAddBlobRequest>(
        RequestInfo->CallContext
    );
    request->Mode = EAddBlobMode::Write;
    request->WriteRanges.push_back(WriteRange);

    for (const auto& blob: Blobs) {
        request->MergedBlobs.emplace_back(
            blob.BlobId,
            blob.Block,
            blob.BlocksCount);
    }

    NCloud::Send(ctx, Tablet, std::move(request));
}

void TWriteDataActor::HandleAddBlobResponse(
    const TEvIndexTabletPrivate::TEvAddBlobResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    ReplyAndDie(ctx, msg->GetError());
}

void TWriteDataActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);
    ReplyAndDie(ctx, MakeError(E_REJECTED, "request cancelled"));
}

void TWriteDataActor::ReplyAndDie(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    {
        // notify tablet
        using TCompletion = TEvIndexTabletPrivate::TEvWriteDataCompleted;
        auto response = std::make_unique<TCompletion>(error);
        response->CommitId = CommitId;
        response->Count = 1;
        response->Size = BlobsSize;
        response->Time = ctx.Now() - RequestInfo->StartedTs;
        // In cases when the blob is already written, we do not report stats
        response->WasTwoStageWrite = ShouldSkipBlobStorage;
        NCloud::Send(ctx, Tablet, std::move(response));
    }

    FILESTORE_TRACK(
        ResponseSent_TabletWorker,
        RequestInfo->CallContext,
        "WriteData");

    if (RequestInfo->Sender != Tablet) {
        if (ShouldSkipBlobStorage) {
            // In the case, when there is already a blobId with data written to,
            // we don't need to send requests to blobstorage. In this case, we
            // need to notify the sender about the completion of the request
            auto response = std::make_unique<TEvIndexTablet::TEvMarkWriteCompletedResponse>(error);
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s MarkWriteCompleted: #%lu completed (%s)",
                LogTag.c_str(),
                RequestInfo->CallContext->RequestId,
                FormatError(response->Record.GetError()).c_str());
            NCloud::Reply(ctx, *RequestInfo, std::move(response));
        } else {
            auto response =
                std::make_unique<TEvService::TEvWriteDataResponse>(error);
            LOG_DEBUG(
                ctx,
                TFileStoreComponents::TABLET_WORKER,
                "%s WriteData: #%lu completed (%s)",
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
    }

    Die(ctx);
}

STFUNC(TWriteDataActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvIndexTabletPrivate::TEvWriteBlobResponse, HandleWriteBlobResponse);
        HFunc(TEvIndexTabletPrivate::TEvAddBlobResponse, HandleAddBlobResponse);

        default:
            HandleUnexpectedEvent(ev, TFileStoreComponents::TABLET_WORKER);
            break;
    }
}

}   // namespace

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

    auto validator = [&] (const NProto::TWriteDataRequest& request) {
        if (auto error = ValidateRange(range); HasError(error)) {
            return error;
        }

        auto* handle = FindHandle(request.GetHandle());
        if (!handle || handle->GetSessionId() != GetSessionId(request)) {
            return ErrorInvalidHandle(request.GetHandle());
        }

        if (!IsWriteAllowed(BuildBackpressureThresholds())) {
            if (CompactionStateLoadStatus.Finished
                    && ++BackpressureErrorCount >=
                    Config->GetMaxBackpressureErrorsBeforeSuicide())
            {
                LOG_WARN(ctx, TFileStoreComponents::TABLET_WORKER,
                    "%s Suiciding after %u backpressure errors",
                    LogTag.c_str(),
                    BackpressureErrorCount);

                Suicide(ctx);
            }

            return MakeError(E_REJECTED, "rejected due to backpressure");
        }

        return NProto::TError{};
    };

    if (!AcceptRequest<TEvService::TWriteDataMethod>(ev, ctx, validator)) {
        return;
    }

    // this request passed the backpressure check => tablet is not stuck
    // anywhere, we can reset our backpressure error counter
    BackpressureErrorCount = 0;

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
        std::move(blockBuffer),
        InvalidCommitId,
        TVector<NKikimr::TLogoBlobID>{});
}

void TIndexTabletActor::HandleWriteDataCompleted(
    const TEvIndexTabletPrivate::TEvWriteDataCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    ReleaseCollectBarrier(msg->CommitId);

    // In case of using two-stage write, we must have acquired CollectBarrier
    // on MarkWriteComplete request. In this case, we need to release it
    if (msg->WasTwoStageWrite) {
        ReleaseCollectBarrier(msg->CommitId, true);
    }

    WorkerActors.erase(ev->Sender);
    EnqueueBlobIndexOpIfNeeded(ctx);

    // We report metrics only in case of default write path. Reporting both
    // cases altogether might be misleading
    if (!msg->WasTwoStageWrite) {
        Metrics.WriteData.Count.fetch_add(
            msg->Count,
            std::memory_order_relaxed);
        Metrics.WriteData.RequestBytes.fetch_add(
            msg->Size,
            std::memory_order_relaxed);
        Metrics.WriteData.Time.Record(msg->Time);
    }
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

    auto* handle = FindHandle(args.Handle);
    if (!handle || handle->Session != session) {
        args.Error = ErrorInvalidHandle(args.Handle);
        return true;
    }

    if (!HasFlag(handle->GetFlags(), NProto::TCreateHandleRequest::E_WRITE)) {
        args.Error = ErrorInvalidHandle(args.Handle);
        return true;
    }

    args.NodeId = handle->GetNodeId();
    args.CommitId = GetCurrentCommitId();

    LOG_TRACE(ctx, TFileStoreComponents::TABLET,
        "%s WriteNodeData tx %lu @%lu %s",
        LogTag.c_str(),
        args.CommitId,
        args.NodeId,
        args.ByteRange.Describe().c_str());

    TIndexTabletDatabase db(tx.DB);

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

    TIndexTabletDatabase db(tx.DB);

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
        LOG_DEBUG(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Failed TX two-stage completion: %lu",
            LogTag.c_str(),
            args.RequestInfo->CallContext->RequestId);
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

    if (args.IsBlobAlreadyWritten()) {
        // No need to assign blobIds as they are already assigned
        TABLET_VERIFY(blobs.size() == args.BlobIds.size());

        for (auto [targetBlob, srcBlob]: Zip(blobs, args.BlobIds)) {
            targetBlob.BlobId = TPartialBlobId(
                srcBlob.Generation(),
                srcBlob.Step(),
                srcBlob.Channel(),
                srcBlob.BlobSize(),
                srcBlob.Cookie(),
                srcBlob.PartId());
        }
    } else {
        args.BlobCommitId = GenerateCommitId();
        if (args.BlobCommitId == InvalidCommitId) {
            return RebootTabletOnCommitOverflow(ctx, "WriteData");
        }

        ui32 blobIndex = 0;
        for (auto& blob: blobs) {
            const auto ok = GenerateBlobId(
                args.BlobCommitId,
                blob.BlobContent.size(),
                blobIndex++,
                &blob.BlobId);

            if (!ok) {
                ReassignDataChannelsIfNeeded(ctx);

                args.Error =
                    MakeError(E_FS_OUT_OF_SPACE, "failed to generate blobId");
                reply(ctx, args);

                return;
            }
        }
    }

    if (!args.IsBlobAlreadyWritten()) {
        // In the two-stage write scenario, CollectBarrier is acquired on the
        // issuing of the blobIds by the TIndexTabletActor
        AcquireCollectBarrier(args.BlobCommitId);
    }

    auto actor = std::make_unique<TWriteDataActor>(
        TraceSerializer,
        LogTag,
        ctx.SelfID,
        args.RequestInfo,
        args.BlobCommitId,
        std::move(blobs),
        TWriteRange{args.NodeId, args.ByteRange.End()},
        args.IsBlobAlreadyWritten());

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

}   // namespace NCloud::NFileStore::NStorage
