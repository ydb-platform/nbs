#include "io_companion.h"
#include "cloud/storage/core/libs/tablet/blob_id.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition/model/fresh_blob.h>

#include <cloud/storage/core/libs/diagnostics/wilson_trace_compatibility.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

// TODO: get rid of code duplication with writeblob_companion.cpp.

namespace {

////////////////////////////////////////////////////////////////////////////////

class TPatchBlobActor final
    : public TActorBootstrapped<TPatchBlobActor>
{
    using TRequest = TEvPartitionCommonPrivate::TEvPatchBlobRequest;
    using TResponse = TEvPartitionCommonPrivate::TEvPatchBlobResponse;

private:
    const TActorId TabletActorId;
    const TRequestInfoPtr RequestInfo;

    const ui64 TabletId;
    const TString DiskId;
    const std::unique_ptr<TRequest> Request;
    TChildLogTitle LogTitle;

    TInstant RequestSent;
    TInstant ResponseReceived;
    TStorageStatusFlags StorageStatusFlags;
    double ApproximateFreeSpaceShare = 0;

    ui32 OriginalGroupId = 0;

    const ui64 BSGroupOperationId = 0;

    const bool PassTraceIdToBlobstorage = false;

public:
    TPatchBlobActor(
        const TActorId& tabletActorId,
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        TString diskId,
        std::unique_ptr<TRequest> request,
        ui32 originalGroupId,
        TChildLogTitle logTitle,
        ui64 bsGroupOperationId,
        bool passTraceIdToBlobstorage);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendPatchRequest(const TActorContext& ctx);

    void NotifyCompleted(const TActorContext& ctx, const NProto::TError& error);

    void ReplyAndDie(
        const TActorContext& ctx,
        std::unique_ptr<TResponse> response);

    void ReplyError(
        const TActorContext& ctx,
        const TEvBlobStorage::TEvPatchResult& response,
        const TString& description);

private:
    STFUNC(StateWork);

    void HandlePatchResult(
        const TEvBlobStorage::TEvPatchResult::TPtr& ev,
        const TActorContext& ctx);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);
};

////////////////////////////////////////////////////////////////////////////////

TPatchBlobActor::TPatchBlobActor(
        const TActorId& tabletActorId,
        TRequestInfoPtr requestInfo,
        ui64 tabletId,
        TString diskId,
        std::unique_ptr<TRequest> request,
        ui32 originalGroupId,
        TChildLogTitle logTitle,
        ui64 bsGroupOperationId,
        bool passTraceIdToBlobstorage)
    : TabletActorId(tabletActorId)
    , RequestInfo(std::move(requestInfo))
    , TabletId(tabletId)
    , DiskId(std::move(diskId))
    , Request(std::move(request))
    , LogTitle(std::move(logTitle))
    , OriginalGroupId(originalGroupId)
    , BSGroupOperationId(bsGroupOperationId)
    , PassTraceIdToBlobstorage(passTraceIdToBlobstorage)
{}

void TPatchBlobActor::Bootstrap(const TActorContext& ctx)
{
    TRequestScope timer(*RequestInfo);

    Become(&TThis::StateWork);

    LWTRACK(
        RequestReceived_PartitionWorker_DSProxy,
        RequestInfo->CallContext->LWOrbit,
        "PatchBlob",
        RequestInfo->CallContext->RequestId,
        OriginalGroupId);

    SendPatchRequest(ctx);
}

void TPatchBlobActor::SendPatchRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvBlobStorage::TEvPatch>(
        OriginalGroupId,
        MakeBlobId(TabletId, Request->OriginalBlobId),
        MakeBlobId(TabletId, Request->PatchedBlobId),
        0, // mask for bruteforcing
        std::move(Request->Diffs),
        Request->DiffCount,
        Request->Deadline);

    NWilson::TTraceId traceId;
    if (PassTraceIdToBlobstorage) {
        traceId = GetTraceIdForRequestId(
            RequestInfo->CallContext->LWOrbit,
            RequestInfo->CallContext->RequestId);
    }
    request->Orbit = std::move(RequestInfo->CallContext->LWOrbit);

    RequestSent = ctx.Now();

    SendToBSProxy(
        ctx,
        Request->Proxy,
        request.release(),
        RequestInfo->Cookie,
        std::move(traceId));
}

void TPatchBlobActor::NotifyCompleted(
    const TActorContext& ctx,
    const NProto::TError& error)
{
    auto request = std::make_unique<TEvPartitionCommonPrivate::TEvPatchBlobCompleted>(
        error);
    request->OriginalBlobId = Request->OriginalBlobId;
    request->PatchedBlobId = Request->PatchedBlobId;
    request->StorageStatusFlags = StorageStatusFlags;
    request->ApproximateFreeSpaceShare = ApproximateFreeSpaceShare;
    request->RequestTime = ResponseReceived - RequestSent;
    request->BSGroupOperationId = BSGroupOperationId;

    NCloud::Send(ctx, TabletActorId, std::move(request));
}

void TPatchBlobActor::ReplyAndDie(
    const TActorContext& ctx,
    std::unique_ptr<TResponse> response)
{
    NotifyCompleted(ctx, response->GetError());

    LWTRACK(
        ResponseSent_Partition,
        RequestInfo->CallContext->LWOrbit,
        "PatchBlob",
        RequestInfo->CallContext->RequestId);

    NCloud::Reply(ctx, *RequestInfo, std::move(response));
    Die(ctx);
}

void TPatchBlobActor::ReplyError(
    const TActorContext& ctx,
    const TEvBlobStorage::TEvPatchResult& response,
    const TString& description)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::PARTITION,
        "%s TEvBlobStorage::TEvPatch failed: %s\n%s",
        LogTitle.GetWithTime().c_str(),
        description.c_str(),
        response.Print(false).c_str());

    ReplyAndDie(
        ctx,
        std::make_unique<TResponse>(MakeError(
            E_REJECTED,
            "TEvBlobStorage::TEvPatch failed: " + description)));
}

////////////////////////////////////////////////////////////////////////////////

void TPatchBlobActor::HandlePatchResult(
    const TEvBlobStorage::TEvPatchResult::TPtr& ev,
    const TActorContext& ctx)
{
    ResponseReceived = ctx.Now();

    const auto* msg = ev->Get();

    if (msg->Status != NKikimrProto::OK) {
        ReplyError(ctx, *msg, msg->ErrorReason);
        return;
    }

    auto blobId = MakeBlobId(TabletId, Request->PatchedBlobId);
    if (msg->Id != blobId) {
        ReplyError(ctx, *msg, "invalid response received");
        return;
    }

    StorageStatusFlags = msg->StatusFlags;
    ApproximateFreeSpaceShare = msg->ApproximateFreeSpaceShare;

    auto response = std::make_unique<TResponse>();
    response->ExecCycles = RequestInfo->GetExecCycles();

    ReplyAndDie(ctx, std::move(response));
}

void TPatchBlobActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    auto response = std::make_unique<TEvPartitionCommonPrivate::TEvPatchBlobResponse>(
        MakeError(E_REJECTED, "tablet is shutting down"));

    ReplyAndDie(ctx, std::move(response));
}

STFUNC(TPatchBlobActor::StateWork)
{
    TRequestScope timer(*RequestInfo);

    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

        HFunc(TEvBlobStorage::TEvPatchResult, HandlePatchResult);

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

void TIOCompanion::HandlePatchBlob(
    const TEvPartitionCommonPrivate::TEvPatchBlobRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto msg = ev->Release();

    auto requestInfo = CreateRequestInfo<TEvPartitionCommonPrivate::TPatchBlobMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    TRequestScope timer(*requestInfo);

    LWTRACK(
        RequestReceived_Partition,
        requestInfo->CallContext->LWOrbit,
        "PatchBlob",
        requestInfo->CallContext->RequestId);

    ui32 originalChannel = msg->OriginalBlobId.Channel();
    ui32 originalGroupId = Info()->GroupFor(
        originalChannel,
        msg->OriginalBlobId.Generation());

    ui32 channel = msg->PatchedBlobId.Channel();
    msg->Proxy =
        Info()->BSProxyIDForChannel(channel, msg->PatchedBlobId.Generation());
    ui64 bsGroupOperationId = BSGroupOperationId++;

    ChannelsState.EnqueueIORequest(
        channel,
        std::make_unique<TPatchBlobActor>(
            ctx.SelfID,
            requestInfo,
            TabletID,
            PartitionConfig.GetDiskId(),
            std::unique_ptr<TEvPartitionCommonPrivate::TEvPatchBlobRequest>(
                msg.Release()),
            originalGroupId,
            LogTitle.GetChild(GetCycleCount()),
            bsGroupOperationId,
            DiagnosticsConfig->GetPassTraceIdToBlobstorage()),
        bsGroupOperationId,
        originalGroupId,
        TBSGroupOperationTimeTracker::EOperationType::Patch,
        PartitionConfig.GetBlockSize());

    ProcessIOQueue(ctx, channel);
}

void TIOCompanion::HandlePatchBlobCompleted(
    const TEvPartitionCommonPrivate::TEvPatchBlobCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    BSGroupOperationTimeTracker.OnFinished(
        msg->BSGroupOperationId,
        GetCycleCount());

    Actors.Erase(ev->Sender);

    ui32 patchedChannel = msg->PatchedBlobId.Channel();
    ui32 patchedGroup = Info()->GroupFor(
        patchedChannel,
        msg->PatchedBlobId.Generation());

    const auto isValidFlag = NKikimrBlobStorage::EStatusFlags::StatusIsValid;
    const auto yellowMoveFlag =
        NKikimrBlobStorage::EStatusFlags::StatusDiskSpaceLightYellowMove;

    if (msg->StorageStatusFlags.Check(isValidFlag)) {
        const auto permissions = StorageStatusFlags2ChannelPermissions(
            msg->StorageStatusFlags);
        Client.UpdateChannelPermissions(ctx, patchedChannel, permissions);
        ChannelsState.UpdateChannelFreeSpaceShare(
            patchedChannel,
            msg->ApproximateFreeSpaceShare);

        if (msg->StorageStatusFlags.Check(yellowMoveFlag)) {
            LOG_WARN(
                ctx,
                TBlockStoreComponents::PARTITION,
                "%s Yellow move flag received for channel %u and group %u",
                LogTitle.GetWithTime().c_str(),
                patchedChannel,
                patchedGroup);

            Client.ScheduleYellowStateUpdate(ctx);
            Client.ReassignChannelsIfNeeded(ctx);
        }
    }

    if (FAILED(msg->GetStatus())) {
        ReportTabletBSFailure(
            TStringBuilder() << "Stop tablet because of PatchBlob error: "
                             << FormatError(msg->GetError()),
            {{"disk", PartitionConfig.GetDiskId()}});
        Client.Poison(ctx);
        return;
    }

    if (patchedGroup == Max<ui32>()) {
        Y_DEBUG_ABORT_UNLESS(
            0,
            "HandlePatchBlobCompleted: invalid blob id received");
    } else {
        ResourceMetricsQueue->Push(
            NPartition::TUpdateWriteThroughput(
                ctx.Now(),
                patchedChannel,
                patchedGroup,
                msg->PatchedBlobId.BlobSize()));
    }
    ResourceMetricsQueue->Push(
        NPartition::TUpdateNetworkStat(
            ctx.Now(),
            msg->PatchedBlobId.BlobSize()));

    PartCounters->Access(
        [&](auto& counters)
        {
            counters->RequestCounters.PatchBlob.AddRequest(
                msg->RequestTime.MicroSeconds(),
                msg->PatchedBlobId.BlobSize(),
                1,
                ChannelsState.GetChannelDataKind(patchedChannel));
        });

    ChannelsState.CompleteIORequest(patchedChannel);

    ProcessIOQueue(ctx, patchedChannel);
}

}   // namespace NCloud::NBlockStore::NStorage
