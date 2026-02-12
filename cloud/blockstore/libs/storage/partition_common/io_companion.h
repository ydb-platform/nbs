#pragma once

#include <cloud/blockstore/libs/diagnostics/config.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/model/log_title.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>
#include <cloud/blockstore/libs/storage/partition_common/long_running_operation_companion.h>
#include <cloud/blockstore/libs/storage/partition_common/part_channels_state.h>

#include <cloud/storage/core/libs/actors/mortal_actor.h>

#include <contrib/ydb/core/base/blobstorage.h>
#include <contrib/ydb/library/actors/core/actor.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class IIOCompanionClient: public IMortalActor
{
public:
    virtual void UpdateWriteThroughput(
        const TInstant& now,
        const NKikimr::NMetrics::TChannel& channel,
        const NKikimr::NMetrics::TGroupId& group,
        ui64 value) = 0;
    virtual void UpdateReadThroughput(
        const TInstant& now,
        const NKikimr::NMetrics::TChannel& channel,
        const NKikimr::NMetrics::TGroupId& group,
        ui64 value,
        bool isOverlayDisk) = 0;
    virtual void UpdateNetworkStat(const TInstant& now, ui64 value) = 0;

    virtual void ScheduleYellowStateUpdate(
        const NActors::TActorContext& ctx) = 0;
    virtual void UpdateYellowState(const NActors::TActorContext& ctx) = 0;
    virtual void ReassignChannelsIfNeeded(
        const NActors::TActorContext& ctx) = 0;
    virtual void UpdateChannelPermissions(
        const NActors::TActorContext& ctx,
        ui32 channel,
        EChannelPermissions permissions) = 0;

    virtual void RegisterSuccess(TInstant now, ui32 groupId) = 0;
    virtual void RegisterDowntime(TInstant now, ui32 groupId) = 0;

    virtual TPartitionDiskCounters& GetPartCounters() = 0;

    ~IIOCompanionClient() override = default;
};

class TIOCompanion
{
private:
    const TStorageConfigPtr Config;
    const NProto::TPartitionConfig& PartitionConfig;
    const NKikimr::TTabletStorageInfoPtr TabletStorageInfo;
    const ui64 TabletID;
    const NBlockCodecs::ICodec* BlobCodec;
    const NActors::TActorId VolumeActorId;
    const TDiagnosticsConfigPtr DiagnosticsConfig;
    const EStorageAccessMode StorageAccessMode;

    TBSGroupOperationTimeTracker& BSGroupOperationTimeTracker;

    ui64& BSGroupOperationId;

    IIOCompanionClient& Client;

    TPartitionChannelsState& ChannelsState;

    ui32 WriteBlobErrorCount = 0;

    ui32 ReadBlobErrorCount = 0;

    TLogTitle& LogTitle;

    TRunningActors Actors;

public:
    TIOCompanion(
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
        TLogTitle& logTitle);

    void HandleWriteBlob(
        const TEvPartitionCommonPrivate::TEvWriteBlobRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleWriteBlobCompleted(
        const TEvPartitionCommonPrivate::TEvWriteBlobCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleLongRunningBlobOperation(
        const TEvPartitionCommonPrivate::TEvLongRunningOperation::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlob(
        const TEvPartitionCommonPrivate::TEvReadBlobRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlobCompleted(
        const TEvPartitionCommonPrivate::TEvReadBlobCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePatchBlob(
        const TEvPartitionCommonPrivate::TEvPatchBlobRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePatchBlobCompleted(
        const TEvPartitionCommonPrivate::TEvPatchBlobCompleted::TPtr& ev,
        const NActors::TActorContext& ctx);

    void ProcessIOQueue(const NActors::TActorContext& ctx, ui32 channel);

    void KillActors(const NActors::TActorContext& ctx);

private:
    auto Info()
    {
        return TabletStorageInfo;
    }
};

}   // namespace NCloud::NBlockStore::NStorage
