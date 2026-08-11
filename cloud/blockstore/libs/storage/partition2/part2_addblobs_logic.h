#pragma once

#include "part2_database.h"
#include "part2_state.h"
#include "part2_tx.h"

#include <cloud/blockstore/libs/storage/model/log_title.h>

namespace NActors {

class TActorSystem;

}   // namespace NActors

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

// actorSystem may be null when debug logging is not needed.
void ExecuteAddBlobsTransaction(
    const NActors::TActorSystem* actorSystem,
    TChildLogTitle logTitle,
    ui64 tabletId,
    TString diskId,
    ui64 deletionCommitId,
    ui32 maxBlocksInBlob,
    TPartitionDatabase& db,
    TTxPartition::TAddBlobs& args,
    TPartitionState& state);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
