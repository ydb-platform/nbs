#pragma once

#include <cloud/blockstore/libs/storage/partition_common/io_companion.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

class TPartitionActor;

struct TIOCompanionClient: public IIOCompanionClient
{
    TPartitionActor& Owner;

    explicit TIOCompanionClient(TPartitionActor& owner)
        : Owner(owner)
    {}

    void ScheduleYellowStateUpdate(const NActors::TActorContext& ctx) override;

    void UpdateYellowState(const NActors::TActorContext& ctx) override;

    void ReassignChannelsIfNeeded(const NActors::TActorContext& ctx) override;

    void UpdateChannelPermissions(
        const NActors::TActorContext& ctx,
        ui32 channel,
        EChannelPermissions permissions) override;

    // IMortalActor implements

    void Poison(const NActors::TActorContext& ctx) override;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
