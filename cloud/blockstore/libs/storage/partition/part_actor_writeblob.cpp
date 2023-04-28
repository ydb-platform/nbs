#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>

#include <ydb/core/base/blobstorage.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/blockcodecs/codecs.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteBlobActor final
    : public TActorBootstrapped<TWriteBlobActor>
{
    using TRequest = TEvPartitionPrivate::TEvWriteBlobRequest;
    using TResponse = TEvPartitionPrivate::TEvWriteBlobResponse;

private:
    const TActorId TabletActorId;
    const TRequestInfoPtr RequestInfo;

    const ui64 TabletId;
    const std::unique_ptr<TRequest> Request;

    TInstant RequestSent;
    TInstant ResponseReceived;
    TStorageStatusFlags StorageStatusFlags;
    double ApproximateFreeSpaceShare = 0;

public:
    TWriteBlobActor(
        const TActorId& tabletActorId,
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        std::unique_ptr<TRequest> request);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendPutRequest(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TResponse> response);

    void ReplyError(
        const TActorContext& ctx,
        const TEvBlobStorage::TEvPutResult& response,
        const TString& description);

private:
    STFUNC(StateWork);

    void HandlePutResult(
        const TEvBlobStorage::TEvPutResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TWriteBlobActor::TWriteBlobActor(
        const TActorId& tabletActorId,
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        std::unique_ptr<TRequest> request)
    : TabletActorId(tabletActorId)
    , RequestInfo(std::move(requestInfo))
    , TabletId(tabletId)
    , Request(std::move(request))
{
    ActivityType = TBlockStoreActivities::PARTITION_WORKER;
}

void TWriteBlobActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker,
        RequestInfo->CallContext->LWOrbit,
        "WriteBlob",
        RequestInfo->CallContext->RequestId);

    SendPutRequest(ctx);
}

void TWriteBlobActor::SendPutRequest(const TActorContext& ctx)
{
    TString blobContent;

    if (const auto* guardedSgList = std::get_if<TGuardedSgList>(&Request->Data)) {
        if (auto guard = guardedSgList->Acquire()) {
            const auto& sgList = guard.Get();
            blobContent.ReserveAndResize(SgListGetSize(sgList));
            SgListCopy(sgList, { blobContent.data(), blobContent.size() });
        } else {
            auto error = MakeError(
                E_CANCELLED,
                "failed to acquire sglist in WriteBlobActor");
            ReplyAndDie(ctx, std::make_unique<TResponse>(error));
            return;
        }
    } else {
        blobContent = std::move(std::get<TString>(Request->Data));
    }

    Y_VERIFY(!blobContent.Empty());

    auto request = std::make_unique<TEvBlobStorage::TEvPut>(
        MakeBlobId(TabletId, Request->BlobId),
        std::move(blobContent),
        Request->Deadline,
        Request->Async
            ? NKikimrBlobStorage::AsyncBlob
            : NKikimrBlobStorage::UserData);

    auto traceId = RequestInfo->TraceId.Clone();
    BLOCKSTORE_TRACE_SENT(ctx, &traceId, this, request);

    request->Orbit = std::move(RequestInfo->CallContext->LWOrbit);

    RequestSent = ctx.Now();

    SendToBSProxy(
        ctx,
        Request->Proxy,
        request.release(),
        0,  // cookie
        std::move(traceId));
}

void TWriteBlobActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request = std::make_unique<TEvPartitionPrivate::TEvWriteBlobCompleted>(
        error);
    request->BlobId = Request->BlobId;
    request->StorageStatusFlags = StorageStatusFlags;
    request->ApproximateFreeSpaceShare = ApproximateFreeSpaceShare;
    request->RequestTime = ResponseReceived - RequestSent;

    NCloud::Send(ctx, TabletActorId, std::move(request));
}

void TWriteBlobActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TResponse> response)
{
    NotifyCompleted(ctx, response->GetError());

    BLOCKSTORE_TRACE_SENT(ctx, &RequestInfo->TraceId, this, response);

    if (ResponseReceived) {
        LWTRACK(
            ResponseSent_Partition,
            RequestInfo->CallContext->LWOrbit,
            "WriteBlob",
            RequestInfo->CallContext->RequestId);
    }

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TWriteBlobActor::ReplyError(
    const TActorContext& ctx,
    const TEvBlobStorage::TEvPutResult& response,
    const TString& description)
{
    LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
        "[%lu] TEvBlobStorage::TEvPut failed: %s\n%s",
        TabletId,
        description.data(),
        response.Print(false).data());

    auto error = MakeError(E_REJECTED, "TEvBlobStorage::TEvPut failed: " + description);
    ReplyAndDie(ctx, std::make_unique<TResponse>(error));
}

////////////////////////////////////////////////////////////////////////////////

void TWriteBlobActor::HandlePutResult(
    const TEvBlobStorage::TEvPutResult::TPtr& ev,
    const TActorContext& ctx)
{
    ResponseReceived = ctx.Now();

    const auto* msg = ev->Get();

    BLOCKSTORE_TRACE_RECEIVED(ctx, &RequestInfo->TraceId, this, msg, &ev->TraceId);

    RequestInfo->CallContext->LWOrbit = std::move(msg->Orbit);

    StorageStatusFlags = msg->StatusFlags;
    ApproximateFreeSpaceShare = msg->ApproximateFreeSpaceShare;

    if (msg->Status != NKikimrProto::OK) {
        ReplyError(ctx, *msg, msg->ErrorReason);
        return;
    }

    auto blobId = MakeBlobId(TabletId, Request->BlobId);
    if (msg->Id != blobId) {
        ReplyError(ctx, *msg, "invalid response received");
        return;
    }

    auto response = std::make_unique<TResponse>();
    response->ExecCycles = RequestInfo->GetExecCycles();

    ReplyAndDie(ctx, std::move(response));
}

void TWriteBlobActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto response = std::make_unique<TEvPartitionPrivate::TEvWriteBlobResponse>(
        MakeError(E_REJECTED, "Tablet is dead"));

    ReplyAndDie(ctx, std::move(response));
}

STFUNC(TWriteBlobActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvPutResult, HandlePutResult);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

EChannelPermissions StorageStatusFlags2ChannelPermissions(TStorageStatusFlags ssf)
{
    const auto outOfSpaceMask = static_cast<NKikimrBlobStorage::EStatusFlags>(
        NKikimrBlobStorage::StatusDiskSpaceRed
        | NKikimrBlobStorage::StatusDiskSpaceOrange
        // no need to check StatusDiskSpaceBlack since BS won't accept any write requests in black state anyway
    );
    if (ssf.Check(outOfSpaceMask)) {
        return {};
    }

    if (ssf.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
        return EChannelPermission::SystemWritesAllowed;
    }

    return EChannelPermission::SystemWritesAllowed | EChannelPermission::UserWritesAllowed;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleWriteBlob(
    const TEvPartitionPrivate::TEvWriteBlobRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto msg = ev->Release();
    const auto compRate = Config->GetBlobCompressionRate();
    if (BlobCodec && compRate && msg->BlobId.GetHash() % compRate == 0) {
        TString blobContent;

        if (const auto* guardedSgList = std::get_if<TGuardedSgList>(&msg->Data)) {
            if (auto guard = guardedSgList->Acquire()) {
                const auto& sgList = guard.Get();
                blobContent.ReserveAndResize(SgListGetSize(sgList));
                SgListCopy(sgList, { blobContent.data(), blobContent.size() });
            }
        } else {
            blobContent = std::get<TString>(msg->Data);
        }

        if (blobContent) {
            TString out;
            out.ReserveAndResize(BlobCodec->MaxCompressedLength(blobContent));
            const size_t sz = BlobCodec->Compress(blobContent, out.begin());
            auto& counters = PartCounters->Cumulative;
            counters.UncompressedBytesWritten.Increment(blobContent.Size());
            counters.CompressedBytesWritten.Increment(sz);
        }
    }

    auto requestInfo = CreateRequestInfo<TEvPartitionPrivate::TWriteBlobMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext,
        std::move(ev->TraceId));

    TRequestScope timer(*requestInfo);

    BLOCKSTORE_TRACE_RECEIVED(ctx, &requestInfo->TraceId, this, msg);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "WriteBlob",
        requestInfo->CallContext->RequestId);

    ui32 channel = msg->BlobId.Channel();
    msg->Proxy = Info()->BSProxyIDForChannel(channel, msg->BlobId.Generation());

    State->EnqueueIORequest(channel, std::make_unique<TWriteBlobActor>(
        SelfId(),
        requestInfo,
        TabletID(),
        std::unique_ptr<TEvPartitionPrivate::TEvWriteBlobRequest>(msg.Release())));

    ProcessIOQueue(ctx, channel);
}

void TPartitionActor::HandleWriteBlobCompleted(
    const TEvPartitionPrivate::TEvWriteBlobCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Actors.erase(ev->Sender);

    ui32 channel = msg->BlobId.Channel();
    ui32 group = Info()->GroupFor(channel, msg->BlobId.Generation());

    const auto isValidFlag = NKikimrBlobStorage::EStatusFlags::StatusIsValid;
    const auto yellowMoveFlag =
        NKikimrBlobStorage::EStatusFlags::StatusDiskSpaceLightYellowMove;
    const auto yellowStopFlag =
        NKikimrBlobStorage::EStatusFlags::StatusDiskSpaceYellowStop;

    if (msg->StorageStatusFlags.Check(isValidFlag)) {
        const auto permissions = StorageStatusFlags2ChannelPermissions(
            msg->StorageStatusFlags);
        UpdateChannelPermissions(ctx, channel, permissions);
        State->UpdateChannelFreeSpaceShare(
            channel,
            msg->ApproximateFreeSpaceShare);

        if (msg->StorageStatusFlags.Check(yellowStopFlag)) {
            LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Yellow stop flag received for channel %u and group %u",
                TabletID(),
                channel,
                group);

            ScheduleYellowStateUpdate(ctx);
            ReassignChannelsIfNeeded(ctx);
        } else if (msg->StorageStatusFlags.Check(yellowMoveFlag)) {
            LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
                "[%lu] Yellow move flag received for channel %u and group %u",
                TabletID(),
                channel,
                group);

            State->RegisterReassignRequestFromBlobStorage(channel);
            ReassignChannelsIfNeeded(ctx);
        }
    }

    if (FAILED(msg->GetStatus())) {
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%lu] Stop tablet because of WriteBlob error: %s",
            TabletID(),
            FormatError(msg->GetError()).data());

        ReportTabletBSFailure();
        Suicide(ctx);
        return;
    }

    if (group == Max<ui32>()) {
        Y_VERIFY_DEBUG(0, "HandleWriteBlobCompleted: invalid blob id received");
    } else {
        UpdateWriteThroughput(ctx.Now(), channel, group, msg->BlobId.BlobSize());
    }
    UpdateNetworkStat(ctx.Now(), msg->BlobId.BlobSize());
    UpdateExecutorStats(ctx);

    PartCounters->RequestCounters.WriteBlob.AddRequest(
        msg->RequestTime.MicroSeconds(),
        msg->BlobId.BlobSize(),
        1,
        State->GetChannelDataKind(channel));

    State->CompleteIORequest(channel);

    ProcessIOQueue(ctx, channel);
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
