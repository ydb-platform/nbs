#pragma once

#include "part_mirror_resync_util.h"
#include "part_nonrepl_events_private.h"

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/events.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TChecksumRangeActorCompanion
{
    const TBlockRange64 Range;
    const TVector<TReplicaDescriptor> Replicas;

    ui32 ChecksumsCount = 0;
    bool Finished = false;
    TVector<ui64> Checksums;
    NProto::TError Error;


public:
    TChecksumRangeActorCompanion(
        TBlockRange64 range,
        TVector<TReplicaDescriptor> replicas);

    void ChecksumBlocks(const NActors::TActorContext& ctx);

    void HandleChecksumResponse(
        const TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    bool IsFinished();
    TVector<ui64> GetChecksums();
    NProto::TError GetError();

private:
    void ChecksumReplicaBlocks(const NActors::TActorContext& ctx, int idx);
};

}   // namespace NCloud::NBlockStore::NStorage
