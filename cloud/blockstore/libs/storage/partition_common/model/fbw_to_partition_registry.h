#pragma once

#include "contrib/ydb/library/actors/core/actorid.h"
#include "util/generic/hash_set.h"

namespace NCloud::NBlockStore::NPartition {

class TFBWToPartitionRegistry
{
    struct TPartitionContext {
        NActors::TActorId PartitionActorId;
        bool IsAlive = true;
    };

private:
    THashMap<NActors::TActorId, TPartitionContext> FbwToPartitionRegistry;
    THashMap<NActors::TActorId, NActors::TActorId> PartitionToFBWRegistry;
    THashSet<NActors::TActorId> DeadPartitionsWithoutFBWs;


public:
    void Add(NActors::TActorId fbwActorId, NActors::TActorId partitionActorId) {
        FbwToPartitionRegistry[fbwActorId] = partitionActorId;
    }

    NActors::TActorId PartitionDead(NActors::TActorId partitionActorId);
    NActors::TActorId FBWDead(NActors::TActorId fbwActorId);

    void DumpLeakedFBWs();
};

}   // namespace NCloud::NBlockStore::NPartition
