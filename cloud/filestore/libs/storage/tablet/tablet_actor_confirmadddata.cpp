#include "tablet_actor.h"

#include "model/split_range.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/error.h>
#include <cloud/filestore/libs/storage/api/tablet.h>

#include <util/generic/cast.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::SendDeferredConfirmAddDataResponse(
    const TActorContext& ctx,
    TPendingConfirmAddData pending,
    const NProto::TError& error)
{
    auto response =
        std::make_unique<TEvIndexTablet::TEvConfirmAddDataResponse>(error);

    CompleteResponse<TEvIndexTablet::TConfirmAddDataMethod>(
        response->Record,
        pending.CallContext,
        ctx);

    ctx.Send(new NActors::IEventHandle(
        pending.Sender,
        ctx.SelfID,
        response.release(),
        0 /* flags */,
        pending.Cookie));

    FinalizeProfileLogRequestInfo(
        std::move(pending.ProfileLogRequest),
        ctx.Now(),
        GetFileSystemId(),
        error,
        ProfileLog);
}

void TIndexTabletActor::SendPendingConfirmAddDataResponse(
    const TActorContext& ctx,
    ui64 commitId,
    const NProto::TError& error)
{
    auto pendingIt = PendingConfirmation.find(commitId);
    if (pendingIt == PendingConfirmation.end()) {
        return;
    }

    auto pending = std::move(pendingIt->second);
    PendingConfirmation.erase(pendingIt);

    if (!HasError(error)) {
        Metrics.ConfirmAddData.Update(1, 0, ctx.Now() - pending.DeferredTs);
    }

    SendDeferredConfirmAddDataResponse(ctx, std::move(pending), error);
}

void TIndexTabletActor::UnconfirmedAddBlobSafePointReached(
    const TActorContext& ctx,
    ui64 commitId,
    const NProto::TError& error)
{
    if (commitId == InvalidCommitId) {
        ReportInvalidCommitIdInUnconfirmedAddBlobSafePoint(
            TStringBuilder()
            << "tabletId: " << TabletID() << ", commitId: " << commitId);
        return;
    }

    ConfirmedData.erase(commitId);

    if (UnconfirmedRecoveryReady) {
        SendPendingConfirmAddDataResponse(ctx, commitId, error);
    } else if (ConfirmedData.empty()) {
        BlobsConfirmed(ctx);
    }
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleConfirmAddData(
    const TEvIndexTablet::TEvConfirmAddDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto startedTs = ctx.Now();
    NProto::TProfileLogRequestInfo profileLogRequest;
    InitTabletProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreRequest::ConfirmAddData,
        msg->Record,
        startedTs,
        BehaveAsShard(msg->Record.GetHeaders()));

    auto finalizeProfile = [&](const NProto::TError& error)
    {
        FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            error,
            ProfileLog);
    };

    auto reply = [&](const NProto::TError& error)
    {
        auto response =
            std::make_unique<TEvIndexTablet::TEvConfirmAddDataResponse>(error);
        CompleteResponse<TEvIndexTablet::TConfirmAddDataMethod>(
            response->Record,
            msg->CallContext,
            ctx);
        NCloud::Reply(ctx, *ev, std::move(response));
        finalizeProfile(error);
    };

    auto validator = [&](const TEvIndexTablet::TConfirmAddDataMethod::TRequest::
                             ProtoRecordType&)
    {
        return IsDataOperationAllowed();
    };

    if (!AcceptRequest<TEvIndexTablet::TConfirmAddDataMethod>(
            ev,
            ctx,
            validator))
    {
        finalizeProfile(MakeError(E_REJECTED, "not accepted"));
        return;
    }

    const ui64 commitId = msg->Record.GetCommitId();

    auto deferReply = [&]
    {
        TPendingConfirmAddData pending;
        pending.Sender = ev->Sender;
        pending.Cookie = ev->Cookie;
        pending.DeferredTs = startedTs;
        pending.CallContext = msg->CallContext;
        pending.ProfileLogRequest = std::move(profileLogRequest);
        PendingConfirmation[commitId] = std::move(pending);
    };

    if (auto unconfirmedIt = UnconfirmedData.find(commitId);
        unconfirmedIt != UnconfirmedData.end())
    {
        ProcessStorageStatusFlags(
            ctx,
            unconfirmedIt->second.Data.GetBlobIds(),
            msg->Record);

        if (DeletionQueue.contains(commitId)) {
            reply(ErrorUnconfirmedDataDeleted());
        } else {
            deferReply();
            ConfirmData(commitId, ctx);
        }
        return;
    }

    if (UnconfirmedDataInProgress.find(commitId) !=
        UnconfirmedDataInProgress.end())
    {
        deferReply();
        Metrics.ConfirmAddDataExtra.DeferredCount.fetch_add(
            1,
            std::memory_order_relaxed);
        return;
    }

    reply(ErrorUnconfirmedDataNotFound());
}

void TIndexTabletActor::HandleCancelAddData(
    const TEvIndexTablet::TEvCancelAddDataRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();
    const auto startedTs = ctx.Now();

    NProto::TProfileLogRequestInfo profileLogRequest;
    InitTabletProfileLogRequestInfo(
        profileLogRequest,
        EFileStoreRequest::CancelAddData,
        msg->Record,
        startedTs,
        BehaveAsShard(msg->Record.GetHeaders()));

    auto finalizeProfile = [&](const NProto::TError& error)
    {
        FinalizeProfileLogRequestInfo(
            std::move(profileLogRequest),
            ctx.Now(),
            GetFileSystemId(),
            error,
            ProfileLog);
    };

    auto reply = [&](const NProto::TError& responseError)
    {
        auto response =
            std::make_unique<TEvIndexTablet::TEvCancelAddDataResponse>(
                responseError);
        CompleteResponse<TEvIndexTablet::TCancelAddDataMethod>(
            response->Record,
            msg->CallContext,
            ctx);
        NCloud::Reply(ctx, *ev, std::move(response));

        finalizeProfile(responseError);

        Metrics.CancelAddData.Update(1, 0, ctx.Now() - startedTs);
    };

    auto validator = [&](const TEvIndexTablet::TCancelAddDataMethod::TRequest::
                             ProtoRecordType&)
    {
        return IsDataOperationAllowed();
    };

    if (!AcceptRequest<TEvIndexTablet::TCancelAddDataMethod>(
            ev,
            ctx,
            validator))
    {
        finalizeProfile(MakeError(E_REJECTED, "not accepted"));
        Metrics.CancelAddData.Update(1, 0, ctx.Now() - startedTs);
        return;
    }

    const ui64 commitId = msg->Record.GetCommitId();

    if (!UnconfirmedData.contains(commitId) &&
        !UnconfirmedDataInProgress.contains(commitId))
    {
        reply(ErrorUnconfirmedDataNotFound());
        return;
    }

    if (!DeletionQueue.contains(commitId)) {
        DeletionQueue.emplace(commitId);
        // We reply to CancelAddData immediately, so from this point forward we
        // rely on DeleteUnconfirmedData  being executed ahead of any later
        // AddBlob TX. Keep this execute before the reply path,
        // and keep TDeleteUnconfirmedData page-fault-free, or a later execute
        // may reorder and revive data that was already cancelled.
        ExecuteTx<TDeleteUnconfirmedData>(
            ctx,
            CreateRequestInfo(
                SelfId(),
                0 /* cookie */,
                MakeIntrusive<TCallContext>()),
            TVector<ui64>{commitId});
    }

    reply(NProto::TError{});
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TEvIndexTabletPrivate::TEvAddBlobRequest>
TIndexTabletActor::BuildAddBlobRequest(
    ui64 commitId,
    const NProto::TUnconfirmedData& entry)
{
    const ui64 nodeId = entry.GetNodeId();

    auto addBlobRequest =
        std::make_unique<TEvIndexTabletPrivate::TEvAddBlobRequest>(
            MakeIntrusive<TCallContext>());
    addBlobRequest->Mode = EAddBlobMode::WriteUnconfirmed;
    addBlobRequest->ConfirmedDataRefCommitId = commitId;

    const TByteRange byteRange(
        entry.GetOffset(),
        entry.GetLength(),
        GetBlockSize());

    addBlobRequest->WriteRanges.push_back(
        TWriteRange{nodeId, entry.GetOffset() + entry.GetLength()});

    for (const auto& part: entry.GetUnalignedDataRanges()) {
        const ui32 blockIndex = part.GetOffset() / GetBlockSize();
        const ui32 offsetInBlock =
            part.GetOffset() - static_cast<ui64>(blockIndex) * GetBlockSize();

        addBlobRequest->UnalignedDataParts.push_back(
            {nodeId, blockIndex, offsetInBlock, part.GetContent()});
    }

    SplitRange(
        byteRange.FirstAlignedBlock(),
        byteRange.AlignedBlockCount(),
        BlockGroupSize,
        [&](ui32 blockOffset, ui32 blocksCount)
        {
            TBlock block{
                nodeId,
                IntegerCast<ui32>(byteRange.FirstAlignedBlock() + blockOffset),
                InvalidCommitId,
                InvalidCommitId};
            addBlobRequest->MergedBlobs.emplace_back(
                TPartialBlobId(),
                block,
                blocksCount);
        });

    // Assign BlobIds from data
    for (size_t i = 0; i < addBlobRequest->MergedBlobs.size() &&
                       i < static_cast<size_t>(entry.BlobIdsSize());
         ++i)
    {
        auto& targetBlob = addBlobRequest->MergedBlobs[i];
        const auto srcBlob = LogoBlobIDFromLogoBlobID(entry.GetBlobIds(i));
        targetBlob.BlobId = TPartialBlobId(
            srcBlob.Generation(),
            srcBlob.Step(),
            srcBlob.Channel(),
            srcBlob.BlobSize(),
            srcBlob.Cookie(),
            srcBlob.PartId());
    }

    return addBlobRequest;
}

void TIndexTabletActor::AddBlobForUnconfirmedData(
    const TActorContext& ctx,
    ui64 commitId,
    const NProto::TUnconfirmedData& entry)
{
    auto addBlobRequest = BuildAddBlobRequest(commitId, entry);
    auto requestInfo = CreateRequestInfo(
        SelfId(),
        0 /* cookie */,
        addBlobRequest->CallContext);
    requestInfo->StartedTs = ctx.Now();

    FILESTORE_TRACK(
        BackgroundRequestReceived_Tablet,
        addBlobRequest->CallContext,
        "AddBlob");

    ExecuteTx<TAddBlob>(
        ctx,
        std::move(requestInfo),
        addBlobRequest->Mode,
        std::move(addBlobRequest->SrcBlobs),
        std::move(addBlobRequest->SrcBlocks),
        std::move(addBlobRequest->MixedBlobs),
        std::move(addBlobRequest->MergedBlobs),
        std::move(addBlobRequest->WriteRanges),
        std::move(addBlobRequest->UnalignedDataParts),
        addBlobRequest->ConfirmedDataRefCommitId);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::ConfirmData(ui64 commitId, const TActorContext& ctx)
{
    auto unconfirmedIt = UnconfirmedData.find(commitId);
    TABLET_VERIFY(unconfirmedIt != UnconfirmedData.end());

    auto [pos, inserted] =
        ConfirmedData.emplace(commitId, std::move(unconfirmedIt->second));
    TABLET_VERIFY(inserted);
    UnconfirmedData.erase(unconfirmedIt);

    const auto& entry = pos->second;

    // Submit AddBlob tx immediately to keep confirmation ordering in tx queue
    // (assuming no page-fault/restart).
    AddBlobForUnconfirmedData(ctx, commitId, entry.Data);
}

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::DeleteUnconfirmedDataForSession(
    const TString& sessionId,
    const TActorContext& ctx)
{
    TVector<ui64> commitIdsToDelete;

    auto enqueueCommitIdsToDelete = [&](const auto& data)
    {
        for (const auto& [commitId, trackedData]: data) {
            if (trackedData.SessionId != sessionId) {
                continue;
            }

            if (!DeletionQueue.emplace(commitId).second) {
                continue;
            }

            commitIdsToDelete.push_back(commitId);
        }
    };

    enqueueCommitIdsToDelete(UnconfirmedData);
    enqueueCommitIdsToDelete(UnconfirmedDataInProgress);

    if (!commitIdsToDelete.empty()) {
        LOG_INFO(
            ctx,
            TFileStoreComponents::TABLET,
            "%s Deleting unconfirmed data: sessionId=%s, "
            "deleteNow=%zu",
            LogTag.c_str(),
            sessionId.Quote().c_str(),
            commitIdsToDelete.size());

        // Session cleanup has the same ordering requirement as CancelAddData:
        // once commitIds are placed into DeletionQueue, DeleteUnconfirmedData
        // must be executed before any later AddBlob execute. For that reason it
        // should be also page-fault-free.
        ExecuteTx<TDeleteUnconfirmedData>(
            ctx,
            CreateRequestInfo(
                SelfId(),
                0 /* cookie */,
                MakeIntrusive<TCallContext>()),
            std::move(commitIdsToDelete));
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TIndexTabletActor::PrepareTx_DeleteUnconfirmedData(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteUnconfirmedData& args)
{
    Y_UNUSED(ctx);
    Y_UNUSED(tx);
    Y_UNUSED(args);

    // Nothing to prepare
    return true;
}

void TIndexTabletActor::ExecuteTx_DeleteUnconfirmedData(
    const TActorContext& ctx,
    TTransactionContext& tx,
    TTxIndexTablet::TDeleteUnconfirmedData& args)
{
    Y_UNUSED(ctx);

    TIndexTabletDatabase db(tx.DB);

    for (ui64 commitId: args.CommitIds) {
        db.DeleteUnconfirmedData(commitId);
        UnconfirmedData.erase(commitId);
        const bool released = TryReleaseCollectBarrier(commitId);
        TABLET_VERIFY(released);
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s DeleteUnconfirmedData tx: %zu commitIds",
        LogTag.c_str(),
        args.CommitIds.size());
}

void TIndexTabletActor::CompleteTx_DeleteUnconfirmedData(
    const TActorContext& ctx,
    TTxIndexTablet::TDeleteUnconfirmedData& args)
{
    for (ui64 commitId: args.CommitIds) {
        DeletionQueue.erase(commitId);
        SendPendingConfirmAddDataResponse(
            ctx,
            commitId,
            ErrorUnconfirmedDataDeleted());
    }

    LOG_DEBUG(
        ctx,
        TFileStoreComponents::TABLET,
        "%s DeleteUnconfirmedData complete: %zu commitIds",
        LogTag.c_str(),
        args.CommitIds.size());
}

}   // namespace NCloud::NFileStore::NStorage
