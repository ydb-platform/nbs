#include "io_companion.h"

#include <cloud/storage/core/libs/actors/helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

TIOCompanion::TIOCompanion(
        TStorageConfigPtr config,
        const NProto::TPartitionConfig& partitionConfig,
        NKikimr::TTabletStorageInfo* tabletStorageInfo,
        ui64 tabletID,
        const NBlockCodecs::ICodec* blobCodec,
        const NActors::TActorId& volumeActorId,
        TDiagnosticsConfigPtr diagnosticsConfig,
        EStorageAccessMode storageAccessMode,
        TBSGroupOperationTimeTracker& bsGroupOperationTimeTracker,
        ui64& bsGroupOperationId,
        IIOCompanionClient& client,
        TPartitionChannelsState& channelsState,
        TLogTitle& logTitle)
    : Config(std::move(config))
    , PartitionConfig(partitionConfig)
    , TabletStorageInfo(tabletStorageInfo)
    , TabletID(tabletID)
    , BlobCodec(blobCodec)
    , VolumeActorId(volumeActorId)
    , DiagnosticsConfig(std::move(diagnosticsConfig))
    , StorageAccessMode(storageAccessMode)
    , BSGroupOperationTimeTracker(bsGroupOperationTimeTracker)
    , BSGroupOperationId(bsGroupOperationId)
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
        BSGroupOperationTimeTracker.OnStarted(
            request->BSGroupOperationId,
            request->Group,
            request->OperationType,
            GetCycleCount(),
            request->BlockSize);
    }
}


}   // namespace NCloud::NBlockStore::NStorage
