#include "part_mirror_resync_util.h"

#include "resync_range.h"
#include "resync_range_block_by_block.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

std::pair<ui32, ui32> BlockRange2RangeId(TBlockRange64 range, ui32 blockSize)
{
    const auto resyncRangeBlockCount = ResyncRangeSize / blockSize;
    ui32 first = range.Start / resyncRangeBlockCount;
    ui32 last = range.End / resyncRangeBlockCount;
    return std::make_pair(first, last);
}

TBlockRange64 RangeId2BlockRange(ui32 rangeId, ui32 blockSize)
{
    const auto resyncRangeBlockCount = ResyncRangeSize / blockSize;
    ui64 start = rangeId * resyncRangeBlockCount;
    return TBlockRange64::WithLength(start, resyncRangeBlockCount);
}


bool CanFixMismatch(bool isMinor, NProto::EResyncPolicy resyncPolicy)
{
    return isMinor || resyncPolicy != NProto::EResyncPolicy::MINOR_4MB;
}

std::unique_ptr<NActors::IActor> MakeResyncRangeActor(
    TRequestInfoPtr requestInfo,
    ui32 blockSize,
    TBlockRange64 range,
    TVector<TReplicaDescriptor> replicas,
    TString writerClientId,
    IBlockDigestGeneratorPtr blockDigestGenerator,
    NProto::EResyncPolicy resyncPolicy)
{
    if (resyncPolicy == NProto::EResyncPolicy::MINOR_4MB ||
        resyncPolicy == NProto::EResyncPolicy::MINOR_AND_MAJOR_4MB)
    {
        return std::make_unique<TResyncRangeActor>(
            std::move(requestInfo),
            blockSize,
            range,
            std::move(replicas),
            std::move(writerClientId),
            blockDigestGenerator);
    }

    return std::make_unique<TResyncRangeBlockByBlockActor>(
        std::move(requestInfo),
        blockSize,
        range,
        std::move(replicas),
        std::move(writerClientId),
        blockDigestGenerator,
        resyncPolicy);
}

}   // namespace NCloud::NBlockStore::NStorage
