#include "part_actor.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>
#include <cloud/blockstore/libs/storage/partition_common/long_running_operation_companion.h>

#include <contrib/ydb/core/base/blobstorage.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <library/cpp/blockcodecs/codecs.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

class TWriteBlobActor final
    : public TActorBootstrapped<TWriteBlobActor>
    , public TLongRunningOperationCompanion
{
    using TRequest = TEvPartitionPrivate::TEvWriteBlobRequest;
    using TResponse = TEvPartitionPrivate::TEvWriteBlobResponse;

private:
    const TActorId TabletActorId;
    const TRequestInfoPtr RequestInfo;

    const ui64 TabletId;
    const TString DiskId;
    const std::unique_ptr<TRequest> Request;
    const ui32 GroupId;

    TInstant RequestSent;
    TInstant ResponseReceived;
    TStorageStatusFlags StorageStatusFlags;
    double ApproximateFreeSpaceShare = 0;
    TVector<ui32> BlockChecksums;

public:
    TWriteBlobActor(
        const TActorId& tabletActorId,
        const TActorId& volumeActorId,
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        TString diskId,
        std::unique_ptr<TRequest> request,
        TDuration longRunningThreshold,
        ui32 groupId);

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
        const TActorId& volumeActorId,
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        TString diskId,
        std::unique_ptr<TRequest> request,
        TDuration longRunningThreshold,
        ui32 groupId)
    : TLongRunningOperationCompanion(
          tabletActorId,
          volumeActorId,
          longRunningThreshold,
          TLongRunningOperationCompanion::EOperation::WriteBlob,
          groupId)
    , TabletActorId(tabletActorId)
    , RequestInfo(std::move(requestInfo))
    , TabletId(tabletId)
    , DiskId(std::move(diskId))
    , Request(std::move(request))
    , GroupId(groupId)
{}

void TWriteBlobActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker_DSProxy,
        RequestInfo->CallContext->LWOrbit,
        "WriteBlob",
        RequestInfo->CallContext->RequestId,
        GroupId);

    SendPutRequest(ctx);
    TLongRunningOperationCompanion::RequestStarted(ctx);
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

    STORAGE_VERIFY(
        !blobContent.empty(),
        TWellKnownEntityTypes::TABLET,
        TabletId);

    if (Request->BlockSizeForChecksums) {
        STORAGE_VERIFY(
            blobContent.size() % Request->BlockSizeForChecksums == 0,
            TWellKnownEntityTypes::TABLET,
            TabletId);

        ui32 offset = 0;
        while (offset < blobContent.size()) {
            BlockChecksums.push_back(ComputeDefaultDigest({
                blobContent.data() + offset,
                Request->BlockSizeForChecksums}));

            offset += Request->BlockSizeForChecksums;
        }
    }

    auto request = std::make_unique<TEvBlobStorage::TEvPut>(
        MakeBlobId(TabletId, Request->BlobId),
        std::move(blobContent),
        Request->Deadline,
        Request->Async
            ? NKikimrBlobStorage::AsyncBlob
            : NKikimrBlobStorage::UserData);

    request->Orbit = std::move(RequestInfo->CallContext->LWOrbit);

    RequestSent = ctx.Now();

    SendToBSProxy(
        ctx,
        Request->Proxy,
        request.release());
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

    if (ResponseReceived) {
        LWTRACK(
            ResponseSent_Partition,
            RequestInfo->CallContext->LWOrbit,
            "WriteBlob",
            RequestInfo->CallContext->RequestId);
    }

    TLongRunningOperationCompanion::RequestFinished(ctx, response->GetError());

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TWriteBlobActor::ReplyError(
    const TActorContext& ctx,
    const TEvBlobStorage::TEvPutResult& response,
    const TString& description)
{
    LOG_ERROR(ctx, TBlockStoreComponents::PARTITION,
        "[%lu][d:%s] TEvBlobStorage::TEvPut failed: %s\n%s",
        TabletId,
        DiskId.c_str(),
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
    response->BlockChecksums = std::move(BlockChecksums);

    ReplyAndDie(ctx, std::move(response));
}

void TWriteBlobActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto response = std::make_unique<TEvPartitionPrivate::TEvWriteBlobResponse>(
        MakeError(E_REJECTED, "tablet is shutting down"));

    ReplyAndDie(ctx, std::move(response));
}

STFUNC(TWriteBlobActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, TLongRunningOperationCompanion::HandleTimeout);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvPutResult, HandlePutResult);

        default:
            HandleUnexpectedEvent(
                ev,
                TBlockStoreComponents::PARTITION_WORKER,
                __PRETTY_FUNCTION__);
            break;
    }
}

EChannelPermissions StorageStatusFlags2ChannelPermissions(TStorageStatusFlags ssf)
{
    /*
    YellowStop: Tablets switch to read-only mode. Only system writes are allowed.

    LightOrange: Alert: "Tablets have not stopped". Compaction writes are not
    allowed if this flag is received.

    PreOrange: VDisk switches to read-only mode.

    Orange: All VDisks in the group switch to read-only mode.

    Red: PDisk stops issuing chunks.

    Black: Reserved for recovery.
    */

    const auto outOfSpaceMask = static_cast<NKikimrBlobStorage::EStatusFlags>(
        NKikimrBlobStorage::StatusDiskSpaceBlack
        | NKikimrBlobStorage::StatusDiskSpaceRed
        | NKikimrBlobStorage::StatusDiskSpaceOrange
        | NKikimrBlobStorage::StatusDiskSpacePreOrange
        | NKikimrBlobStorage::StatusDiskSpaceLightOrange
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
            counters.UncompressedBytesWritten.Increment(blobContent.size());
            counters.CompressedBytesWritten.Increment(sz);
        }
    }

    auto requestInfo = CreateRequestInfo<TEvPartitionPrivate::TWriteBlobMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "WriteBlob",
        requestInfo->CallContext->RequestId);

    ui32 channel = msg->BlobId.Channel();
    msg->Proxy = Info()->BSProxyIDForChannel(channel, msg->BlobId.Generation());
    ui32 groupId = Info()->GroupFor(channel, msg->BlobId.Generation());

    State->EnqueueIORequest(
        channel,
        std::make_unique<TWriteBlobActor>(
            SelfId(),
            VolumeActorId,
            requestInfo,
            TabletID(),
            PartitionConfig.GetDiskId(),
            std::unique_ptr<TEvPartitionPrivate::TEvWriteBlobRequest>(
                msg.Release()),
            GetDowntimeThreshold(
                *DiagnosticsConfig,
                PartitionConfig.GetStorageMediaKind()),
            groupId));

    ProcessIOQueue(ctx, channel);
}

void TPartitionActor::HandleWriteBlobCompleted(
    const TEvPartitionPrivate::TEvWriteBlobCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    Actors.Erase(ev->Sender);

    ui32 channel = msg->BlobId.Channel();
    ui32 groupId = Info()->GroupFor(channel, msg->BlobId.Generation());

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
                "[%lu][d:%s] Yellow stop flag received for channel %u and group %u",
                TabletID(),
                PartitionConfig.GetDiskId().c_str(),
                channel,
                groupId);

            ScheduleYellowStateUpdate(ctx);
            ReassignChannelsIfNeeded(ctx);
        } else if (msg->StorageStatusFlags.Check(yellowMoveFlag)) {
            LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
                "[%lu][d:%s] Yellow move flag received for channel %u and group %u",
                TabletID(),
                PartitionConfig.GetDiskId().c_str(),
                channel,
                groupId);

            State->RegisterReassignRequestFromBlobStorage(channel);
            ReassignChannelsIfNeeded(ctx);
        }
    }

    if (FAILED(msg->GetStatus())) {
        LOG_WARN(ctx, TBlockStoreComponents::PARTITION,
            "[%lu][d:%s] Stop tablet because of WriteBlob error (actor %s, group %u): %s",
            TabletID(),
            PartitionConfig.GetDiskId().c_str(),
            ev->Sender.ToString().c_str(),
            groupId,
            FormatError(msg->GetError()).data());

        ReportTabletBSFailure();
        Suicide(ctx);
        return;
    }

    if (groupId == Max<ui32>()) {
        Y_DEBUG_ABORT_UNLESS(
            0,
            "HandleWriteBlobCompleted: invalid blob id received");
    } else {
        UpdateWriteThroughput(
            ctx.Now(),
            channel,
            groupId,
            msg->BlobId.BlobSize());
        State->RegisterSuccess(ctx.Now(), groupId);
    }
    UpdateNetworkStat(ctx.Now(), msg->BlobId.BlobSize());

    PartCounters->RequestCounters.WriteBlob.AddRequest(
        msg->RequestTime.MicroSeconds(),
        msg->BlobId.BlobSize(),
        1,
        State->GetChannelDataKind(channel));

    State->CompleteIORequest(channel);

    ProcessIOQueue(ctx, channel);
}

void TPartitionActor::HandleLongRunningBlobOperation(
    const TEvPartitionCommonPrivate::TEvLongRunningOperation::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    using TEvLongRunningOperation =
        TEvPartitionCommonPrivate::TEvLongRunningOperation;

    const auto& msg = *ev->Get();

    if (msg.Reason == TEvLongRunningOperation::EReason::LongRunningDetected &&
        msg.FirstNotify)
    {
        State->RegisterDowntime(ctx.Now(), msg.GroupId);
        Actors.MarkLongRunning(ev->Sender, msg.Operation);
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
