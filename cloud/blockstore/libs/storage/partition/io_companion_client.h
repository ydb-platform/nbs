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

    void ProcessStorageStatusFlags(
        const NActors::TActorContext& ctx,
        NKikimr::TStorageStatusFlags flags,
        ui32 channel,
        ui32 generation,
        double approximateFreeSpaceShare) override;

    // IMortalActor implements

    void Poison(const NActors::TActorContext& ctx) override;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition
