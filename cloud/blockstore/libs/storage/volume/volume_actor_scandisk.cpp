#include "volume_actor.h"

#include "cloud/blockstore/libs/storage/disk_agent/model/public.h"
#include "cloud/blockstore/libs/storage/partition_common/model/blob_markers.h"
#include "cloud/storage/core/libs/common/block_buffer.h"
#include "cloud/storage/core/libs/tablet/model/partial_blob_id.h"
#include "cloud/storage/core/protos/error.pb.h"
#include "contrib/ydb/library/actors/core/log.h"

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage::NVolume {

using namespace NActors;

using namespace NKikimr;
// using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

// constexpr ui64 MaxUniqueId = std::numeric_limits<ui64>::max();

class TScanDiskActor final: public TActorBootstrapped<TScanDiskActor>
{
private:
    TString DiskId;
    const NActors::TActorId VolumeId;
    const ui32 BlocksPerBatch = 0;
    const TPartialBlobId FinalBlobId;
    const TDuration RetryTimeout;
    bool IsCompleted = false;
    bool HasErrors = false;

    ui64 ErrorBlockId = 0;
    TString ErrorMessage;

    ui64 CurrentBlockId = 0;

    TVector<NBlobMarkers::TBlobMark> RequestsInCurrentBatch;

    // TVector<ui64> BrokenBlobs;

public:
    TScanDiskActor(
        TString diskId,
        const NActors::TActorId& volumeId,
        ui32 blobsPerBatch,
        ui64 finalCommitId,
        TDuration retryTimeout);

    void Bootstrap(const TActorContext& ctx);

private:
    void SendCheckRangeRequest(const TActorContext& ctx);

    void SendReadBlocksRequest(const TActorContext& ctx);

    void CheckRange(const TActorContext& ctx);

    void HandleReadBlocksResponse(
        const TEvService::TEvReadBlocksResponse::TPtr& ev,
        const TActorContext& ctx);

    void ReportStatus(const TActorContext& ctx);

    STFUNC(StateWork);

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx);

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx);

    void HandleCheckRangeResponse(
        const TEvVolume::TEvCheckRangeResponse::TPtr& ev,
        const TActorContext& ctx);

    /*
    void HandleReadBlobResponse(
        const TEvPartitionCommonPrivate::TEvReadBlobResponse::TPtr& ev,
        const TActorContext& ctx);
    */
};

////////////////////////////////////////////////////////////////////////////////

TScanDiskActor::TScanDiskActor(
    TString diskId,
    const NActors::TActorId& volumeId,
    ui32 blobsPerBatch,
    ui64 finalBlobId,
    TDuration retryTimeout
    // TBlockBuffer blockBuffer
    )
    : DiskId(std::move(diskId))
    , VolumeId(volumeId)
    , BlocksPerBatch(blobsPerBatch)
    , FinalBlobId(MakePartialBlobId(finalBlobId, Max()))
    , RetryTimeout(retryTimeout)
{}

void TScanDiskActor::Bootstrap(const TActorContext& ctx)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! scandisk in volume has "
        "started");
        sleep(5);
    CheckRange(ctx);
    Become(&TThis::StateWork);
}

void TScanDiskActor::CheckRange(const TActorContext& ctx)
{
    if (HasErrors) {
        IsCompleted = true;
        ReportStatus(ctx);
        Die(ctx);
    }

    // SendCheckRangeRequest(ctx);

    SendReadBlocksRequest(ctx);
}

void TScanDiskActor::SendCheckRangeRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvCheckRange>(
        MakeIntrusive<TCallContext>(),
        CurrentBlockId,
        BlocksPerBatch,
        DiskId);

    NCloud::Send(ctx, VolumeId, std::move(request));

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! scandisk request "
        "sended ");
}

void TScanDiskActor::SendReadBlocksRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    request->Record.SetDiskId(DiskId);
    request->Record.SetStartIndex(CurrentBlockId);
    // на подумать
    request->Record.SetSessionId("");
    request->Record.SetBlocksCount(BlocksPerBatch);

    auto* headers = request->Record.MutableHeaders();

    headers->SetIsBackgroundRequest(true);
    headers->SetClientId(TString(BackgroundOpsClientId));
    NCloud::Send(ctx, VolumeId, std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

STFUNC(TScanDiskActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(TEvVolume::TEvCheckRangeResponse, HandleCheckRangeResponse);
        /*
        HFunc(
            TEvPartitionCommonPrivate::TEvReadBlobResponse,
            HandleReadBlobResponse);
        */

        HFunc(TEvService::TEvReadBlocksResponse, HandleReadBlocksResponse);

        default:
            HandleUnexpectedEvent(ev, TBlockStoreComponents::PARTITION_WORKER);
            break;
    }
}

void TScanDiskActor::HandleWakeup(
    const TEvents::TEvWakeup::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    SendCheckRangeRequest(ctx);
}

void TScanDiskActor::HandlePoisonPill(
    const TEvents::TEvPoisonPill::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ev);

    const auto error = MakeError(E_REJECTED, "tablet is shutting down");
    Y_UNUSED(ctx);
    Die(ctx);
}

void TScanDiskActor::HandleCheckRangeResponse(
    const TEvVolume::TEvCheckRangeResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! scandisk response "
        "catched ");

    if (FinalBlobId.UniqueId() <= CurrentBlockId) {
        IsCompleted = true;
    } else {
        CurrentBlockId += BlocksPerBatch;
    }

    ReportStatus(ctx);

    Y_UNUSED(msg);
    /*
    if (FAILED(msg->GetStatus())) {
        if (GetErrorKind(msg->Error) == EErrorKind::ErrorRetriable) {
            ctx.Schedule(RetryTimeout, new TEvents::TEvWakeup());
        } else {
            NotifyCompleted(ctx, msg->Error);
        }
        return;
    }

    if (msg->IsScanCompleted) {
        NotifyCompleted(ctx);
        return;
    }

    BlobIdToRead = NextBlobId(msg->LastVisitedBlobId, MaxUniqueId);

    if (msg->BlobsInBatch.empty()) {
        SendScanDiskBatchRequest(ctx);
        return;
    }
    ReadBlobResponsesCounter = 0;

    RequestsInCurrentBatch = std::move(msg->BlobsInBatch);
    for (ui32 requestIndex = 0; requestIndex < RequestsInCurrentBatch.size();
    ++requestIndex) { SendReadBlobRequest(ctx, requestIndex);
    }
    */
}

void TScanDiskActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! readblocks response "
        "catched ");

    if (HasError(msg->Record.GetError())) {
        ErrorBlockId = CurrentBlockId;
        ErrorMessage = msg->Record.GetError().GetMessage();
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "While scanning a disk with an id = " + DiskId +
                " a reading error has occurred: " + ErrorMessage + "   message   " +  msg->Record.GetError().message() );
        HasErrors = true;
    }

    if (FinalBlobId.UniqueId() <= CurrentBlockId) {
        IsCompleted = true;
    } else {
        CurrentBlockId += BlocksPerBatch;
    }

    ReportStatus(ctx);
    SendReadBlocksRequest(ctx);

    Y_UNUSED(msg);
}

void TScanDiskActor::ReportStatus(const TActorContext& ctx)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! reporting status ");

    auto request = std::make_unique<TEvVolume::TEvCheckRangeReportStatus>(
        MakeIntrusive<TCallContext>(),
        CurrentBlockId,
        IsCompleted);

    NCloud::Send(ctx, VolumeId, std::move(request));
}

}   // namespace

}   // namespace NCloud::NBlockStore::NStorage::NVolume

namespace NCloud::NBlockStore::NStorage {
NActors::IActorPtr TVolumeActor::CreateScanDiskActor(
    TString diskId,
    NActors::TActorId tabletId,
    ui64 blobsPerBatch,
    ui64 finalCommitId,
    TDuration retryTimeout)
{
    return std::make_unique<NVolume::TScanDiskActor>(
        std::move(diskId),
        tabletId,
        blobsPerBatch,
        finalCommitId,
        retryTimeout);
}

}   // namespace NCloud::NBlockStore::NStorage
