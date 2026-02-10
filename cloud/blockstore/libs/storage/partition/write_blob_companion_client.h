#pragma once

#include <cloud/blockstore/libs/storage/partition_common/writeblob_companion.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TPartitionActor;

struct TWriteBlobCompanionClient: public IWriteBlobCompanionClient
{
    TPartitionActor& Owner;

    explicit TWriteBlobCompanionClient(TPartitionActor& owner)
        : Owner(owner)
    {}

    void UpdateWriteThroughput(
        const TInstant& now,
        const NKikimr::NMetrics::TChannel& channel,
        const NKikimr::NMetrics::TGroupId& group,
        ui64 value) override;

    void UpdateNetworkStat(const TInstant& now, ui64 value) override;

    void ScheduleYellowStateUpdate(const NActors::TActorContext& ctx) override;

    void UpdateYellowState(const NActors::TActorContext& ctx) override;

    void ReassignChannelsIfNeeded(const NActors::TActorContext& ctx) override;

    void UpdateChannelPermissions(
        const NActors::TActorContext& ctx,
        ui32 channel,
        EChannelPermissions permissions) override;

    void RegisterSuccess(TInstant now, ui32 groupId) override;

    void RegisterDowntime(TInstant now, ui32 groupId) override;

    void ProcessIOQueue(
        const NActors::TActorContext& ctx,
        ui32 channel) override;

    TPartitionDiskCounters& GetPartCounters() override;

    // IMortalActor implements

    void Poison(const NActors::TActorContext& ctx) override;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
