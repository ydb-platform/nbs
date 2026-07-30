#pragma once

#include "part2_database.h"
#include "part2_state.h"
#include "part2_tx.h"

#include <cloud/blockstore/libs/storage/model/log_title.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

struct TVerifyBlocksMetaResult
{
    bool Ready = true;
    NProto::TError Error;
};

TVerifyBlocksMetaResult VerifyRecreatedBlobMeta(
    TPartitionDatabase& db,
    TPartialBlobId originalBlobId,
    const NProto::TBlobMeta& blobMeta,
    const NProto::TBlobMeta& recreatedBlobMeta);

bool PrepareCleanupTransaction(
    const ui64 tabletId,
    const TString& diskId,
    TPartitionDatabase& db,
    TTxPartition::TCleanup& args);

void ExecuteCleanupTransaction(
    const NActors::TActorSystem* actorSystem,
    const TLogTitle& logTitle,
    const ui64 tabletId,
    TPartitionDatabase& db,
    TTxPartition::TCleanup& args,
    TPartitionState& state);

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
