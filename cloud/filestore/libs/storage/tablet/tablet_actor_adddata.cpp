#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/tablet/actors/tablet_writedata.h>
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
    Y_UNUSED(ctx, tx, args);
}

void TIndexTabletActor::CompleteTx_AddData(
    const TActorContext& ctx,
    TTxIndexTablet::TAddData& args)
{
    RemoveTransaction(*args.RequestInfo);

    auto reply = [&](const TActorContext& ctx, TTxIndexTablet::TAddData& args)
    {
        TABLET_VERIFY(TryReleaseCollectBarrier(args.CommitId));
        TryReleaseCollectBarrier(args.CommitId);

        auto response =
            std::make_unique<TEvIndexTablet::TEvAddDataResponse>(args.Error);
        CompleteResponse<TEvIndexTablet::TAddDataMethod>(
            response->Record,
            args.RequestInfo->CallContext,
            ctx);

        NCloud::Reply(ctx, *args.RequestInfo, std::move(response));
    };

    if (HasError(args.Error)) {
        reply(ctx, args);
        return;
    }

    TVector<TMergedBlob> blobs;
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
            blobs.emplace_back(
                TPartialBlobId(),   // need to generate BlobId later
                block,
                blocksCount,
                "");
        });

    if (blobs.empty() || blobs.size() != args.BlobIds.size()) {
        args.Error = MakeError(
            MAKE_FILESTORE_ERROR(NProto::E_FS_INVAL),
            TStringBuilder() << "blobs count mismatch: expected" << blobs.size()
                             << " got " << args.BlobIds.size());
        reply(ctx, args);
        return;
    }

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

namespace {

////////////////////////////////////////////////////////////////////////////////

/**
 * @param range Aligned byte range
 * @returns A vector of sizes of the blobs that the data should be split into.
 */
TVector<ui64> SplitData(ui32 blockSize, TByteRange range)
{
    TVector<ui64> blobs;
    blobs.reserve(range.AlignedBlockCount() / BlockGroupSize + 1);

    SplitRange(
        range.FirstAlignedBlock(),
        range.AlignedBlockCount(),
        BlockGroupSize,
        [&](ui32 /*blockOffset*/, ui64 blocksCount)
        { blobs.push_back(blocksCount * blockSize); });

    return blobs;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGenerateBlobIds(
    const TEvIndexTablet::TEvGenerateBlobIdsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: %s",
        LogTag.c_str(),
        "GenerateBlobIds",
        DumpMessage(msg->Record).c_str());

    // It is up to the client to provide the aligned range, but we still verify
    // it and reject the request if it is not aligned.
    const ui32 blockSize = GetBlockSize();
    if (msg->Record.GetLength() % blockSize != 0 ||
        msg->Record.GetOffset() % blockSize != 0)
    {
        auto response =
            std::make_unique<TEvIndexTablet::TEvGenerateBlobIdsResponse>(
                MakeError(E_ARGUMENT, "unaligned range"));
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    ui64 commitId = GenerateCommitId();
    if (commitId == InvalidCommitId) {
        return RebootTabletOnCommitOverflow(ctx, "GenerateBlobIds");
    }

    // We schedule this event for the case if the client does not call AddData.
    // Thus we ensure that the collect barrier will be released eventually.
    ctx.Schedule(
        Config->GetGenerateBlobIdsReleaseCollectBarrierTimeout(),
        new TEvIndexTabletPrivate::TEvReleaseCollectBarrier(commitId, 1));
    AcquireCollectBarrier(commitId);

    TByteRange range(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        blockSize);

    auto response =
        std::make_unique<TEvIndexTablet::TEvGenerateBlobIdsResponse>();
    ui64 offset = range.Offset;
    for (auto [blobIndex, length]: Enumerate(SplitData(blockSize, range))) {
        TPartialBlobId partialBlobId;
        // TODO(debnatkh): better selection of channel

        const auto ok =
            GenerateBlobId(commitId, length, blobIndex, &partialBlobId);
        if (!ok) {
            ReassignDataChannelsIfNeeded(ctx);

            auto response =
                std::make_unique<TEvIndexTablet::TEvGenerateBlobIdsResponse>(
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

    if (auto error = IsDataOperationAllowed(); HasError(error)) {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<TEvIndexTablet::TEvAddDataResponse>(
                std::move(error)));
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
    // We acquire the collect barrier for the second time in order to prolong
    // the already acquired lease
    AcquireCollectBarrier(commitId);
    bool txStarted = false;
    Y_DEFER
    {
        // Until the tx is started, it is this method's responsibility to
        // release the collect barrier
        if (!txStarted) {
            TABLET_VERIFY(TryReleaseCollectBarrier(commitId));
            // The second one is used to release the barrier, acquired in
            // GenerateBlobIds method. Though it will be eventually released
            // upon lease expiration, it is better to release it as soon as
            // possible.
            TryReleaseCollectBarrier(commitId);
        }
    };

    const TByteRange range(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        GetBlockSize());

    auto validator = [&](const NProtoPrivate::TAddDataRequest& request)
    {
        return ValidateWriteRequest(ctx, request, range);
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

    if (blobIds.empty()) {
        auto response =
            std::make_unique<TEvIndexTablet::TEvAddDataResponse>(MakeError(
                E_ARGUMENT,
                "empty list of blobs given in AddData request"));
        NCloud::Reply(ctx, *ev, std::move(response));
    }

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
    txStarted = true;
}

}   // namespace NCloud::NFileStore::NStorage
