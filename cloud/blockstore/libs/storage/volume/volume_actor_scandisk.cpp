#include "cloud/blockstore/libs/storage/partition_common/model/blob_markers.h"
#include "cloud/storage/core/libs/common/block_buffer.h"
#include "cloud/storage/core/libs/tablet/model/partial_blob_id.h"
#include "cloud/storage/core/protos/error.pb.h"
#include "contrib/ydb/library/actors/core/log.h"
#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include "volume_actor.h"

namespace NCloud::NBlockStore::NStorage::NVolume {

using namespace NActors;

using namespace NKikimr;
//using namespace NKikimr::NTabletFlatExecutor;

namespace {

////////////////////////////////////////////////////////////////////////////////

//constexpr ui64 MaxUniqueId = std::numeric_limits<ui64>::max();


class TScanDiskActor final
    : public TActorBootstrapped<TScanDiskActor>
{
private:
    const NActors::TActorId  VolumeId;
    const ui32 BlobsPerBatch = 0;
    const TPartialBlobId FinalBlobId;
    const TDuration RetryTimeout;
    bool IsCompleted = false;
    ui64 currentBlobId = 0;

    TVector<NBlobMarkers::TBlobMark> RequestsInCurrentBatch;


    //TVector<ui64> BrokenBlobs;

public:
    TScanDiskActor(
        const NActors::TActorId & volumeId,
        ui32 blobsPerBatch,
        ui64 finalCommitId,
        TDuration retryTimeout
        );

    void Bootstrap(const TActorContext& ctx);

private:
    void SendCheckRangeRequest(const TActorContext& ctx);

    void SendReadBlobRequest(const TActorContext& ctx, ui32 requestIndex);

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
        const NActors::TActorId & volumeId,
        ui32 blobsPerBatch,
        ui64 finalBlobId,
        TDuration retryTimeout
        //TBlockBuffer blockBuffer
        )
    : VolumeId(volumeId)
    , BlobsPerBatch(blobsPerBatch)
    , FinalBlobId(MakePartialBlobId(finalBlobId, Max()))
    , RetryTimeout(retryTimeout)
{}

void TScanDiskActor::Bootstrap(const TActorContext &ctx)
{
    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! scandisk in volume has "
        "started");
    SendCheckRangeRequest(ctx);
    Become(&TThis::StateWork);
}

void TScanDiskActor::SendCheckRangeRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvCheckRange>(
        MakeIntrusive<TCallContext>(),
        currentBlobId,
        BlobsPerBatch);

    NCloud::Send(ctx, VolumeId, std::move(request));

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! scandisk request "
        "sended ");
}


////////////////////////////////////////////////////////////////////////////////

STFUNC(TScanDiskActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {

        HFunc(TEvents::TEvWakeup, HandleWakeup);
        HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);
        HFunc(
            TEvVolume::TEvCheckRangeResponse,
            HandleCheckRangeResponse);
        /*
        HFunc(
            TEvPartitionCommonPrivate::TEvReadBlobResponse,
            HandleReadBlobResponse);
        */

        default:
            HandleUnexpectedEvent(
                ev, TBlockStoreComponents::PARTITION_WORKER);
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

    if (FinalBlobId.CommitId() <= currentBlobId){
        IsCompleted = true;
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
    for (ui32 requestIndex = 0; requestIndex < RequestsInCurrentBatch.size(); ++requestIndex) {
        SendReadBlobRequest(ctx, requestIndex);
    }
    */
}

void TScanDiskActor::ReportStatus(
    const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvCheckRangeReportStatus>(
        MakeIntrusive<TCallContext>(),
        currentBlobId,
        IsCompleted);

    NCloud::Send(ctx, VolumeId, std::move(request));
}

}  // namespace

}  // namespace NCloud::NBlockStore::NStorage::NPartition

namespace NCloud::NBlockStore::NStorage {
NActors::IActorPtr TVolumeActor::CreateScanDiskActor(
    NActors::TActorId  tablet,
    ui64 blobsPerBatch,
    ui64 finalCommitId,
    TDuration retryTimeout)
{
    return std::make_unique<NVolume::TScanDiskActor>(
        std::move(tablet),
        blobsPerBatch,
        finalCommitId,
        retryTimeout);
}

}   // namespace NCloud::NBlockStore::NStorage
