#pragma once

#include <cloud/blockstore/libs/common/block_range.h>

#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/block_handler.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>
#include <cloud/blockstore/libs/storage/partition_common/events_private.h>

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Describes the blocks of the base disk.
// Accepts a map of blocks belonging to the overlay disk (blockMarks). Only
// blocks that do not belong to the overlay disk and marked in the blockMarks as
// a TEmptyMark are described. They are replaced with TFreshMarkOnBaseDisk or
// TBlobMarkOnBaseDisk, or remain TEmptyMark if the block was not written to the
// base disk.
class TDescribeBaseDiskBlocksActor final
    : public NActors::TActorBootstrapped<TDescribeBaseDiskBlocksActor>
{
private:
    const TRequestInfoPtr RequestInfo;

    const TString BaseDiskId;
    const TString BaseDiskCheckpointId;
    const TBlockRange64 BlocksRange;
    const TBlockRange64 BaseDiskBlocksRange;
    const ui32 BlockSize;

    const NActors::TActorId NotifyActorId;

    NBlobMarkers::TBlockMarks BlockMarks;

public:
    TDescribeBaseDiskBlocksActor(
        TRequestInfoPtr requestInfo,
        TString baseDiskId,
        TString baseDiskCheckpointId,
        TBlockRange64 blocksRange,
        TBlockRange64 baseDiskBlocksRange,
        NBlobMarkers::TBlockMarks blockMarks,
        ui32 blockSize,
        NActors::TActorId notifyActorId = {});

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    void DescribeBlocks(const NActors::TActorContext& ctx);
    NProto::TError ValidateDescribeBlocksResponse(
        const TEvVolume::TEvDescribeBlocksResponse& response);
    void ProcessDescribeBlocksResponse(
        TEvVolume::TEvDescribeBlocksResponse&& response);

    void ReplyAndDie(
        const NActors::TActorContext& ctx,
        NProto::TError error = MakeError(S_OK));

private:
    STFUNC(StateWork);

    void HandleDescribeBlocksResponse(
        const TEvVolume::TEvDescribeBlocksResponse::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandlePoisonPill(
        const NKikimr::TEvents::TEvPoisonPill::TPtr& ev,
        const NActors::TActorContext& ctx);
};

}   // namespace NCloud::NBlockStore::NStorage
