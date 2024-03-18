#include "tablet_actor.h"
#include "tablet_actor_writedata_actor.h"

#include <cloud/filestore/libs/storage/tablet/model/blob_builder.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/iterator/enumerate.h>
#include <library/cpp/iterator/zip.h>

#include <util/generic/cast.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>


namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

class TWriteDataActor;

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_AddData(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAddData& args)
{
    auto* session =
        FindSession(args.ClientId, args.SessionId, args.SessionSeqNo);
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
    ui64 commitId = GetCurrentCommitId();

    LOG_TRACE(
        ctx,
        TFileStoreComponents::TABLET,
        "%s AddData tx %lu @%lu %s",
        LogTag.c_str(),
        args.CommitId,
        args.NodeId,
        args.ByteRange.Describe().c_str());

    TIndexTabletDatabase db(tx.DB);

    if (!ReadNode(db, args.NodeId, commitId, args.Node)) {
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

void TIndexTabletActor::ExecuteTx_AddData(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TAddData& args)
{
    FILESTORE_VALIDATE_TX_ERROR(WriteData, args);
    Y_UNUSED(ctx, tx);
}

void TIndexTabletActor::CompleteTx_AddData(
    const TActorContext& ctx,
    TTxIndexTablet::TAddData& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto reply = [&](const TActorContext& ctx, TTxIndexTablet::TAddData& args)
    {
        auto response =
            std::make_unique<TEvService::TEvWriteDataResponse>(args.Error);
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

    TMergedBlobBuilder builder(GetBlockSize());

    IBlockBufferPtr buffer = CreateLazyBlockBuffer(args.ByteRange);

    SplitRange(
        args.ByteRange.FirstAlignedBlock(),
        args.ByteRange.AlignedBlockCount(),
        BlockGroupSize,
        [&](ui32 blockOffset, ui32 blocksCount)
        {
            TBlock block{
                args.NodeId,
                IntegerCast<ui32>(
                    args.ByteRange.FirstAlignedBlock() + blockOffset),
                // correct CommitId will be assigned later in AddBlobs
                InvalidCommitId,
                InvalidCommitId};

            builder.Accept(block, blocksCount, blockOffset, *buffer, false);
        });

    auto blobs = builder.Finish();
    TABLET_VERIFY(blobs);

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
    auto actor = std::make_unique<TWriteDataActor>(
        TraceSerializer,
        LogTag,
        ctx.SelfID,
        args.RequestInfo,
        args.CommitId,
        std::move(blobs),
        TWriteRange{args.NodeId, args.ByteRange.End()},
        true);

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

namespace {

/**
 * @param range Aligned byte range
 * @returns A vector of sizes of the blobs that the data should be split into.
 */
TVector<ui64> SplitData(ui32 blockSize, TByteRange range)
{
    TVector<size_t> blobs;

    IBlockBufferPtr buffer = CreateLazyBlockBuffer(range);

    TMergedBlobBuilder builder(blockSize);
    SplitRange(
        range.FirstAlignedBlock(),
        range.AlignedBlockCount(),
        BlockGroupSize,
        [&](ui32 blockOffset, ui32 blocksCount)
        {
            TBlock block{
                0,
                IntegerCast<ui32>(range.FirstAlignedBlock() + blockOffset),
                InvalidCommitId,
                InvalidCommitId};
            builder.Accept(block, blocksCount, blockOffset, *buffer, false);
        });

    for (const auto& blob: builder.Finish()) {
        blobs.push_back(blob.BlocksCount * blockSize);
    }
    return blobs;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGenerateBlobs(
    const TEvIndexTablet::TEvGenerateBlobsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: %s",
        LogTag.c_str(),
        "GenerateBlobs",
        DumpMessage(msg->Record).c_str());

    // It is up to the client to provide the aligned range, but we still verify
    // it and reject the request if it is not aligned.
    const ui32 blockSize = GetBlockSize();
    if (msg->Record.GetLength() % blockSize != 0 ||
        msg->Record.GetOffset() % blockSize != 0)
    {
        auto response =
            std::make_unique<TEvIndexTablet::TEvGenerateBlobsResponse>(
                MakeError(E_ARGUMENT, "unaligned range"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    ui64 commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "GenerateBlobs");
    }

    // We schedule this event for the case if the client will not call
    // AddData, due to connection loss. Thus we ensure that the
    // collect barrier will be released eventually.
    ctx.Schedule(
        Config->GetGenerateBlobsReleaseCollectBarrierTimeout(),
        new TEvIndexTabletPrivate::TEvReleaseCollectBarrier(commitId));
    AcquireCollectBarrier(commitId);

    TByteRange range(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        blockSize);

    auto response =
        std::make_unique<TEvIndexTablet::TEvGenerateBlobsResponse>();
    ui64 offset = range.Offset;
    for (auto [blobIndex, length]: Enumerate(SplitData(blockSize, range))) {
        TPartialBlobId partialBlobId;
        // TODO(debnatkh): better selection of channel

        const auto ok =
            GenerateBlobId(commitId, length, blobIndex, &partialBlobId);
        if (!ok) {
            ReassignDataChannelsIfNeeded(ctx);

            auto response =
                std::make_unique<TEvIndexTablet::TEvGenerateBlobsResponse>(
                    MakeError(E_FS_OUT_OF_SPACE, "failed to generate blobId"));

            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }
        auto generatedBlob = response->Record.MutableBlobs()->Add();
        LogoBlobIDFromLogoBlobID(
            MakeBlobId(TabletID(), partialBlobId),
            generatedBlob->MutableBlobId());
        generatedBlob->SetOffset(offset);
        generatedBlob->SetLength(length);
        generatedBlob->SetBSGroupId(Info()->GroupFor(
            partialBlobId.Channel(),
            partialBlobId.Generation()));
        offset += length;
    }

    // TODO(debnatkh): Throttling

    response->Record.SetCommitId(commitId);

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAddData(
    const TEvIndexTablet::TEvAddDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (!CompactionStateLoadStatus.Finished) {
        // TODO(debnatkh): Support two-stage write in case of unfinished
        // compaction state loading

        auto response = std::make_unique<TEvIndexTablet::TEvAddDataResponse>(
            MakeError(E_REJECTED, "compaction state not loaded yet"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    auto commitId = msg->Record.GetCommitId();
    if (!IsCollectBarrierAcquired(commitId)) {
        // The client has sent the AddData request too late, after
        // the lease has expired.
        auto response = std::make_unique<TEvIndexTablet::TEvAddDataResponse>(
            MakeError(E_REJECTED, "collect barrier expired"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }
    // We acquire the collect barrier for the second time in order to prolong an
    // already acquired lease.
    AcquireCollectBarrier(commitId);

    const TByteRange range(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        GetBlockSize());

    auto validator = [&](const NProtoPrivate::TAddDataRequest& request)
    {
        if (auto error = ValidateRange(range); HasError(error)) {
            return error;
        }

        auto* handle = FindHandle(request.GetHandle());
        if (!handle || handle->GetSessionId() != GetSessionId(request)) {
            return ErrorInvalidHandle(request.GetHandle());
        }

        if (!IsWriteAllowed(BuildBackpressureThresholds())) {
            if (CompactionStateLoadStatus.Finished &&
                ++BackpressureErrorCount >=
                    Config->GetMaxBackpressureErrorsBeforeSuicide())
            {
                LOG_WARN(
                    ctx,
                    TFileStoreComponents::TABLET_WORKER,
                    "%s Suiciding after %u backpressure errors",
                    LogTag.c_str(),
                    BackpressureErrorCount);

                Suicide(ctx);
            }

            return MakeError(E_REJECTED, "rejected due to backpressure");
        }

        return NProto::TError{};
    };

    if (!AcceptRequest<TEvIndexTablet::TAddDataMethod>(ev, ctx, validator)) {
        return;
    }

    auto requestInfo =
        CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext);
    requestInfo->StartedTs = ctx.Now();

    TVector<NKikimr::TLogoBlobID> blobIds;
    for (const auto& blobId: msg->Record.GetBlobIds()) {
        blobIds.push_back(LogoBlobIDFromLogoBlobID(blobId));
    }

    TABLET_VERIFY(!blobIds.empty());

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: blobId: %s,... (total: %lu)",
        LogTag.c_str(),
        "AddData",
        blobIds[0].ToString().c_str(),
        blobIds.size());

    AddTransaction<TEvIndexTablet::TAddDataMethod>(*requestInfo);

    ExecuteTx<TAddData>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        range,
        msg->Record.GetCommitId(),
        std::move(blobIds));
}

}   // namespace NCloud::NFileStore::NStorage
