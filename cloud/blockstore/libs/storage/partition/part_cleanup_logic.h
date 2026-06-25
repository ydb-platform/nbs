#pragma once

#include "part_state.h"
#include "part_database.h"
#include "part_tx.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

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
    TPartitionDatabase& db,
    TTxPartition::TCleanup& args,
    TPartitionState& state);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
