#include "tablet_actor.h"

#include <cloud/filestore/libs/storage/tablet/actors/tablet_adddata.h>
#include <cloud/filestore/libs/storage/tablet/model/split_range.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/iterator/enumerate.h>

#include <util/generic/cast.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;

using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

/**
 * @param range Aligned byte range
 * @returns The vector of sizes of the blobs that the data should be split into.
 */
TVector<ui64> SplitData(ui32 blockSize, TByteRange range)
{
    TVector<ui64> blobSizes;
    blobSizes.reserve(range.AlignedBlockCount() / BlockGroupSize + 1);

    SplitRange(
        range.FirstAlignedBlock(),
        range.AlignedBlockCount(),
        BlockGroupSize,
        [&](ui32 /*blockOffset*/, ui64 blocksCount)
        { blobSizes.push_back(blocksCount * blockSize); });

    return blobSizes;
}

}   // namespace

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

    if (Config->GetAllowHandlelessIO()) {
        if (args.ExplicitNodeId == InvalidNodeId) {
            // handleless write
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
    ui64 commitId = GetCurrentCommitId();

    for (auto& part: args.UnalignedDataParts) {
        part.NodeId = args.NodeId;
    }

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
    // TODO: replace VERIFY with a check + critical event
    TABLET_VERIFY(args.Node || Config->GetAllowHandlelessIO());
    if (!args.Node) {
        args.Error = ErrorInvalidArgument();
        return true;
    }
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

    // Note: unlike ExecuteTx_WriteData, we do not need to call UpdateNode here,
    // as it is done in AddBlob operation.
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
                /* data buffer */ "");
        });

    if (blobs.empty() || blobs.size() != args.BlobIds.size()) {
        args.Error = MakeError(
            E_ARGUMENT,
            TStringBuilder() << "blobs count mismatch: expected" << blobs.size()
                             << " got " << args.BlobIds.size());
        reply(ctx, args);
        return;
    }

    for (size_t i = 0; i < blobs.size(); ++i) {
        auto& targetBlob = blobs[i];
        auto& srcBlob = args.BlobIds[i];
        targetBlob.BlobId = TPartialBlobId(
            srcBlob.Generation(),
            srcBlob.Step(),
            srcBlob.Channel(),
            srcBlob.BlobSize(),
            srcBlob.Cookie(),
            srcBlob.PartId());
    }
    auto actor = std::make_unique<TAddDataActor>(
        TraceSerializer,
        LogTag,
        ctx.SelfID,
        args.RequestInfo,
        args.CommitId,
        std::move(blobs),
        std::move(args.UnalignedDataParts),
        TWriteRange{args.NodeId, args.ByteRange.End()});

    auto actorId = NCloud::Register(ctx, std::move(actor));
    WorkerActors.insert(actorId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleGenerateBlobIds(
    const TEvIndexTablet::TEvGenerateBlobIdsRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto startedTs = ctx.Now();

    auto* msg = ev->Get();

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: %s",
        LogTag.c_str(),
        "GenerateBlobIds",
        DumpMessage(msg->Record).c_str());

    const ui32 blockSize = GetBlockSize();
    const TByteRange range(
        msg->Record.GetOffset(),
        msg->Record.GetLength(),
        blockSize);

    // It is up to the client to provide an aligned range, but we still verify
    // it and reject the request if it is not aligned.
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

    auto validator = [&](const NProtoPrivate::TGenerateBlobIdsRequest& request)
    {
        return ValidateWriteRequest(
            ctx,
            request,
            range,
            !Config->GetAllowHandlelessIO());
    };

    const bool accepted = AcceptRequest<TEvIndexTablet::TGenerateBlobIdsMethod>(
        ev,
        ctx,
        validator);

    if (!accepted) {
        return;
    }

    if (Config->GetMultipleStageRequestThrottlingEnabled() &&
        ThrottleIfNeeded<TEvIndexTablet::TGenerateBlobIdsMethod>(ev, ctx))
    {
        return;
    }

    // We schedule this event for the case if the client does not call AddData.
    // Thus we ensure that the collect barrier will be released eventually.
    ctx.Schedule(
        Config->GetGenerateBlobIdsReleaseCollectBarrierTimeout(),
        new TEvIndexTabletPrivate::TEvReleaseCollectBarrier(commitId, 1));
    AcquireCollectBarrier(commitId);

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
        generatedBlob->SetBSGroupId(Info()->GroupFor(
            partialBlobId.Channel(),
            partialBlobId.Generation()));
        offset += length;
    }

    response->Record.SetCommitId(commitId);

    Metrics.GenerateBlobIds.Count.fetch_add(1, std::memory_order_relaxed);
    Metrics.GenerateBlobIds.RequestBytes.fetch_add(
        msg->Record.GetLength(),
        std::memory_order_relaxed);
    Metrics.GenerateBlobIds.Time.Record(ctx.Now() - startedTs);

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
        return ValidateWriteRequest(
            ctx,
            request,
            range,
            !Config->GetAllowHandlelessIO());
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
        return;
    }

    TVector<TBlockBytesMeta> unalignedDataParts;
    for (auto& part: *msg->Record.MutableUnalignedDataRanges()) {
        if (part.GetContent().empty()) {
            auto response =
                std::make_unique<TEvIndexTablet::TEvAddDataResponse>(MakeError(
                    E_ARGUMENT,
                    "empty unaligned data part"));
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        const ui32 blockIndex = part.GetOffset() / GetBlockSize();
        const ui32 lastBlockIndex =
            (part.GetOffset() + part.GetContent().size() - 1) / GetBlockSize();
        if (blockIndex != lastBlockIndex) {
            auto response =
                std::make_unique<TEvIndexTablet::TEvAddDataResponse>(MakeError(
                    E_ARGUMENT,
                    TStringBuilder() << "unaligned part spanning more than one"
                        << " block: " << part.GetOffset() << ":"
                        << part.GetContent().size()));
            NCloud::Reply(ctx, *ev, std::move(response));
            return;
        }

        const ui32 offsetInBlock =
            part.GetOffset() - static_cast<ui64>(blockIndex) * GetBlockSize();

        unalignedDataParts.push_back({
            0, // NodeId is not known at this point
            blockIndex,
            offsetInBlock,
            std::move(*part.MutableContent())});
    }

    auto unalignedMsg = [&] () {
        TStringBuilder sb;
        for (auto& part: unalignedDataParts) {
            if (sb.size()) {
                sb << ", ";
            }

            sb << part.BlockIndex
                << ":" << part.OffsetInBlock
                << ":" << part.Data.size();
        }
        return sb;
    };

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s %s: blobId: %s,... (total: %lu), unaligned: %s",
        LogTag.c_str(),
        "AddData",
        blobIds[0].ToString().c_str(),
        blobIds.size(),
        unalignedMsg().c_str());

    const auto evPutResultCount =
        Min<ui32>(blobIds.size(), msg->Record.StorageStatusFlagsSize());
    for (ui32 i = 0; i < evPutResultCount; ++i) {
        const double approximateFreeSpaceShare =
            i < msg->Record.ApproximateFreeSpaceSharesSize()
            ? msg->Record.GetApproximateFreeSpaceShares(i)
            : 0;
        RegisterEvPutResult(
            ctx,
            blobIds[i].Generation(),
            blobIds[i].Channel(),
            msg->Record.GetStorageStatusFlags(i),
            approximateFreeSpaceShare);
    }

    AddTransaction<TEvIndexTablet::TAddDataMethod>(*requestInfo);

    ExecuteTx<TAddData>(
        ctx,
        std::move(requestInfo),
        msg->Record,
        range,
        std::move(blobIds),
        std::move(unalignedDataParts),
        msg->Record.GetCommitId());
    txStarted = true;
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleAddDataCompleted(
    const TEvIndexTabletPrivate::TEvAddDataCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    if (HasError(msg->Error)) {
        LOG_ERROR(
            ctx,
            TFileStoreComponents::TABLET,
            "%s AddData failed: %s",
            LogTag.c_str(),
            FormatError(msg->Error).Quote().c_str());
    } else {
        Metrics.AddData.Update(msg->Count, msg->Size, msg->Time);
    }

    // We try to release commit barrier twice: once for the lock
    // acquired by the GenerateBlob request and once for the lock
    // acquired by the AddData request. Though, the first lock is
    // scheduled to be released, it is better to release it as early
    // as possible.
    TABLET_VERIFY(TryReleaseCollectBarrier(msg->CommitId));
    TryReleaseCollectBarrier(msg->CommitId);

    WorkerActors.erase(ev->Sender);
    EnqueueBlobIndexOpIfNeeded(ctx);
}

}   // namespace NCloud::NFileStore::NStorage
