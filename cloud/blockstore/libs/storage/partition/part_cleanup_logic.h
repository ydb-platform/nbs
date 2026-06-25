#pragma once

#include "part_database.h"
#include "part_state.h"
#include "part_tx.h"

#include <cloud/blockstore/libs/storage/model/log_title.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

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
    const bool verifyRecreatedBlobMetasOnCleanup,
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

}   // namespace NCloud::NBlockStore::NStorage::NPartition
