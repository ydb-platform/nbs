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
    const ui64 Size;
    const NActors::TActorId VolumeId;
    const ui64 BlocksPerBatch = 0;
    const TPartialBlobId FinalBlobId;
    TDuration Timeout;

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
        ui64 size,
        const NActors::TActorId& volumeId,
        ui32 blobsPerBatch,
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
};

////////////////////////////////////////////////////////////////////////////////

TScanDiskActor::TScanDiskActor(
    TString diskId,
    ui64 size,
    const NActors::TActorId& volumeId,
    ui32 blobsPerBatch,
    TDuration retryTimeout)
    : DiskId(std::move(diskId))
    , Size(size)
    , VolumeId(volumeId)
    , BlocksPerBatch(blobsPerBatch)
    , Timeout(retryTimeout)

{}

void TScanDiskActor::Bootstrap(const TActorContext& ctx)
{
    TStorageConfigPtr config;
    auto bytesPerStripe = config->GetBytesPerStripe();
    if (Size > bytesPerStripe){
        // send error message
        return;
    }
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


    SendCheckRangeRequest(ctx);
}

void TScanDiskActor::SendCheckRangeRequest(const TActorContext& ctx)
{
    auto request = std::make_unique<TEvVolume::TEvCheckRangeRequest>(
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

    request->Record.SetStartIndex(CurrentBlockId);
    auto blocksCount = Min(BlocksPerBatch, Size-CurrentBlockId);
    request->Record.SetBlocksCount(blocksCount);
    request->Record.SetDiskId(DiskId);

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

    Y_UNUSED(ctx);
    Y_UNUSED(msg);

    if (FinalBlobId.UniqueId() <= CurrentBlockId) {
        IsCompleted = true;
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "is complited  = " + std::to_string(IsCompleted));
    } else {
        CurrentBlockId += BlocksPerBatch;
    }
}

void TScanDiskActor::HandleReadBlocksResponse(
    const TEvService::TEvReadBlocksResponse::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    LOG_ERROR(
        ctx,
        TBlockStoreComponents::VOLUME,
        "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! scandisk response "
        "catched, cur block id = " + std::to_string(CurrentBlockId));

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

    if (Size <= CurrentBlockId) {

    } else {
        CurrentBlockId += BlocksPerBatch;
    }
    IsCompleted = true;
        LOG_ERROR(
            ctx,
            TBlockStoreComponents::VOLUME,
            "is complited  = " + std::to_string(IsCompleted));

    CheckRange(ctx);
}

}   // namespace    , RetryTimeout()


}   // namespace NCloud::NBlockStore::NStorage::NVolume

namespace NCloud::NBlockStore::NStorage {
NActors::IActorPtr TVolumeActor::CreateScanDiskActor(
    TString diskId,
    ui64 diskSize,
    NActors::TActorId tabletId,
    ui64 blobsPerBatch)
{
    return std::make_unique<NVolume::TScanDiskActor>(
        std::move(diskId),
        diskSize,
        tabletId,
        blobsPerBatch);
}

}   // namespace NCloud::NBlockStore::NStorage
