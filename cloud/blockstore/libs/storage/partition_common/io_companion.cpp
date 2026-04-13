#include "io_companion.h"

#include <cloud/storage/core/libs/actors/helpers.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TIOCompanion::TIOCompanion(
        TStorageConfigPtr config,
        const NProto::TPartitionConfig& partitionConfig,
        NKikimr::TTabletStorageInfoPtr tabletStorageInfo,
        ui64 tabletID,
        const NBlockCodecs::ICodec* blobCodec,
        const NActors::TActorId& volumeActorId,
        TDiagnosticsConfigPtr diagnosticsConfig,
        EStorageAccessMode storageAccessMode,
        TPartitionThreadSafeStatePtr sharedState,
        IIOCompanionClient& client,
        TPartitionChannelsState& channelsState,
        TLogTitle& logTitle)
    : Config(std::move(config))
    , PartitionConfig(partitionConfig)
    , TabletStorageInfo(std::move(tabletStorageInfo))
    , TabletID(tabletID)
    , BlobCodec(blobCodec)
    , VolumeActorId(volumeActorId)
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , StorageAccessMode(storageAccessMode)
    , SharedState(std::move(sharedState))
    , Client(client)
    , ChannelsState(channelsState)
    , LogTitle(logTitle)
{}

void TIOCompanion::ProcessIOQueue(const TActorContext& ctx, ui32 channel)
{
    while (auto request = ChannelsState.DequeueIORequest(channel)) {
        auto actorId = NCloud::Register(ctx, std::move(request->Actor));
        LOG_DEBUG(
            ctx,
            TBlockStoreComponents::PARTITION,
            "%s registered request actor with id [%lu]",
            LogTitle.GetWithTime().c_str(),
            actorId);
        Actors.Insert(actorId);
        SharedState->AccessBSGroupOperationTimeTracker()->OnStarted(
            request->BSGroupOperationId,
            request->Group,
            request->OperationType,
            GetCycleCount(),
            request->BlockSize);
    }
}

void TIOCompanion::KillActors(const NActors::TActorContext& ctx)
{
    for (const auto& actor: Actors.GetActors()) {
        NCloud::Send<TEvents::TEvPoisonPill>(ctx, actor);
    }
}

bool TIOCompanion::HandleRequests(STFUNC_SIG, const NActors::TActorContext& ctx)
{
    switch (ev->GetTypeRewrite()) {
        HFuncCtx(
            TEvPartitionCommonPrivate::TEvWriteBlobRequest,
            HandleWriteBlob,
            ctx);

        HFuncCtx(
            TEvPartitionCommonPrivate::TEvWriteBlobCompleted,
            HandleWriteBlobCompleted,
            ctx);

        HFuncCtx(
            TEvPartitionCommonPrivate::TEvLongRunningOperation,
            HandleLongRunningBlobOperation,
            ctx);

        HFuncCtx(
            TEvPartitionCommonPrivate::TEvReadBlobRequest,
            HandleReadBlob,
            ctx);

        HFuncCtx(
            TEvPartitionCommonPrivate::TEvReadBlobCompleted,
            HandleReadBlobCompleted,
            ctx);

        HFuncCtx(
            TEvPartitionCommonPrivate::TEvPatchBlobRequest,
            HandlePatchBlob,
            ctx);

        HFuncCtx(
            TEvPartitionCommonPrivate::TEvPatchBlobCompleted,
            HandlePatchBlobCompleted,
            ctx);

        default:
            return false;
    }

    return true;
}

bool TIOCompanion::RejectRequests(STFUNC_SIG, const NActors::TActorContext& ctx)
{
    switch (ev->GetTypeRewrite()) {
        HFuncCtx(
            TEvPartitionCommonPrivate::TEvWriteBlobRequest,
            RejectWriteBlob,
            ctx);

        HFuncCtx(
            TEvPartitionCommonPrivate::TEvReadBlobRequest,
            RejectReadBlob,
            ctx);

        HFuncCtx(
            TEvPartitionCommonPrivate::TEvPatchBlobRequest,
            RejectPatchBlob,
            ctx);

        IgnoreFunc(TEvPartitionCommonPrivate::TEvWriteBlobCompleted);
        IgnoreFunc(TEvPartitionCommonPrivate::TEvReadBlobCompleted);
        IgnoreFunc(TEvPartitionCommonPrivate::TEvPatchBlobCompleted);

        default:
            return false;
    }

    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
