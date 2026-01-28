#include "part_fresh_blocks_state.h"

#include <cloud/blockstore/libs/storage/partition/model/operation_status.h>

#include <cloud/storage/core/libs/tablet/model/channels.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/json/proto2json.h>

#include <util/generic/algorithm.h>
#include <util/generic/utility.h>
#include <util/generic/ymath.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NPartition;

using TJsonValue = NJson::TJsonValue;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
T SafeIncrement(T counter, size_t value)
{
    Y_ABORT_UNLESS(counter < Max<T>() - value);
    return counter + value;
}

template <typename T>
T SafeDecrement(T counter, size_t value)
{
    Y_ABORT_UNLESS(counter >= value);
    return counter - value;
}

TJsonValue ToJson(const NPartition::TOperationState& op)
{
    TJsonValue json;
    json["Status"] = ToString(op.Status);
    const auto duration = TInstant::Now() - op.Timestamp;
    json["Duration"] = duration.MicroSeconds();
    return json;
}

void DumpOperationState(
    IOutputStream& out,
    const NPartition::TOperationState& op)
{
    out << ToString(op.Status);

    if (op.Timestamp != TInstant::Zero()) {
        out << " for " << TInstant::Now() - op.Timestamp;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TPartitionFlushState::IncrementUnflushedFreshBlobCount(ui32 value)
{
    UnflushedFreshBlobCount = SafeIncrement(UnflushedFreshBlobCount, value);
}

void TPartitionFlushState::DecrementUnflushedFreshBlobCount(ui32 value)
{
    UnflushedFreshBlobCount = SafeDecrement(UnflushedFreshBlobCount, value);
}

void TPartitionFlushState::IncrementUnflushedFreshBlobByteCount(ui64 value)
{
    UnflushedFreshBlobByteCount =
        SafeIncrement(UnflushedFreshBlobByteCount, value);
}

void TPartitionFlushState::DecrementUnflushedFreshBlobByteCount(ui64 value)
{
    UnflushedFreshBlobByteCount =
        SafeDecrement(UnflushedFreshBlobByteCount, value);
}

ui32 TPartitionFlushState::IncrementFreshBlocksInFlight(size_t value)
{
    FreshBlocksInFlight = SafeIncrement(FreshBlocksInFlight, value);
    return FreshBlocksInFlight;
}

ui32 TPartitionFlushState::DecrementFreshBlocksInFlight(size_t value)
{
    FreshBlocksInFlight = SafeDecrement(FreshBlocksInFlight, value);
    return FreshBlocksInFlight;
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionFreshBlobState::AddFreshBlob(
    TFreshBlobMeta freshBlobMeta,
    ui64 lastTrimFreshLogToCommitId)
{
    Y_ABORT_UNLESS(freshBlobMeta.CommitId > lastTrimFreshLogToCommitId);
    const bool inserted = UntrimmedFreshBlobs.insert(freshBlobMeta).second;
    Y_ABORT_UNLESS(inserted);
    UntrimmedFreshBlobByteCount += freshBlobMeta.BlobSize;
}

void TPartitionFreshBlobState::TrimFreshBlobs(ui64 commitId)
{
    auto& blobs = UntrimmedFreshBlobs;

    while (blobs && blobs.begin()->CommitId <= commitId) {
        Y_ABORT_UNLESS(UntrimmedFreshBlobByteCount >= blobs.begin()->BlobSize);
        UntrimmedFreshBlobByteCount -= blobs.begin()->BlobSize;
        blobs.erase(blobs.begin());
    }
}

////////////////////////////////////////////////////////////////////////////////

ui32 TPartitionFreshBlocksState::IncrementUnflushedFreshBlocksFromChannelCount(
    size_t value)
{
    Y_ABORT_UNLESS(ContextProvider);

    UnflushedFreshBlocksFromChannelCount =
        SafeIncrement(UnflushedFreshBlocksFromChannelCount, value);

    return UnflushedFreshBlocksFromChannelCount;
}

ui32 TPartitionFreshBlocksState::DecrementUnflushedFreshBlocksFromChannelCount(
    size_t value)
{
    Y_ABORT_UNLESS(ContextProvider);

    UnflushedFreshBlocksFromChannelCount =
        SafeDecrement(UnflushedFreshBlocksFromChannelCount, value);

    return UnflushedFreshBlocksFromChannelCount;
}

void TPartitionFreshBlocksState::AddFreshBlob(TFreshBlobMeta freshBlobMeta)
{
    TPartitionFreshBlobState::AddFreshBlob(
        freshBlobMeta,
        GetLastTrimFreshLogToCommitId());
}

void TPartitionFreshBlocksState::InitFreshBlocks(
    const TVector<TOwningFreshBlock>& freshBlocks)
{
    Y_ABORT_UNLESS(ContextProvider);

    for (const auto& freshBlock: freshBlocks) {
        const auto& meta = freshBlock.Meta;

        bool added = Blocks.AddBlock(
            meta.BlockIndex,
            meta.CommitId,
            meta.IsStoredInDb,
            freshBlock.Content);

        Y_ABORT_UNLESS(
            added,
            "Duplicate block detected: %u @%lu",
            meta.BlockIndex,
            meta.CommitId);
    }
}

void TPartitionFreshBlocksState::FindFreshBlocks(
    IFreshBlocksIndexVisitor& visitor,
    const TBlockRange32& readRange,
    ui64 maxCommitId)
{
    Y_ABORT_UNLESS(ContextProvider);

    Blocks.FindBlocks(visitor, readRange, maxCommitId);
}

void TPartitionFreshBlocksState::WriteFreshBlocks(
    const TBlockRange32& writeRange,
    ui64 commitId,
    TSgList sglist)
{
    Y_ABORT_UNLESS(ContextProvider);
    Y_ABORT_UNLESS(writeRange.Size() == sglist.size());

    WriteFreshBlocksImpl(
        writeRange,
        commitId,
        [&](ui32 index) { return sglist[index]; });
}

void TPartitionFreshBlocksState::ZeroFreshBlocks(
    const TBlockRange32& zeroRange,
    ui64 commitId)
{
    Y_ABORT_UNLESS(ContextProvider);

    WriteFreshBlocksImpl(
        zeroRange,
        commitId,
        [](ui32) { return TBlockDataRef(); });
}

void TPartitionFreshBlocksState::DeleteFreshBlock(
    ui32 blockIndex,
    ui64 commitId)
{
    Y_ABORT_UNLESS(ContextProvider);

    bool removed = Blocks.RemoveBlock(
        blockIndex,
        commitId,
        false);   // isStoredInDb

    Y_ABORT_UNLESS(removed);

    DecrementUnflushedFreshBlocksFromChannelCount(1);
}

void TPartitionFreshBlocksState::WriteFreshBlocksImpl(
    const TBlockRange32& writeRange,
    ui64 commitId,
    auto getBlockContent)
{
    TVector<ui64> checkpoints = ContextProvider->GetCheckpointCommitIds();

    TVector<ui64> existingCommitIds;
    TVector<ui64> garbage;

    for (ui32 blockIndex: xrange(writeRange)) {
        ui32 index = blockIndex - writeRange.Start;
        const auto& blockContent = getBlockContent(index);

        Blocks.GetCommitIds(blockIndex, existingCommitIds);

        NCloud::NStorage::FindGarbageVersions(
            checkpoints,
            existingCommitIds,
            garbage);
        for (auto garbageCommitId: garbage) {
            // This block is being flushed; we'll remove it on AddBlobs
            // and we'll release barrier on FlushCompleted
            if (GetFlushedCommitIdsInProgress().contains(garbageCommitId)) {
                continue;
            }

            // Do not remove block if it is stored in db
            // to be able to remove it during flush, otherwise
            // we'll leave garbage in FreshBlocksTable
            auto removed = Blocks.RemoveBlock(
                blockIndex,
                garbageCommitId,
                false);   // isStoredInDb

            if (removed) {
                DecrementUnflushedFreshBlocksFromChannelCount(1);
                GetTrimFreshLogBarriers().ReleaseBarrier(garbageCommitId);
            }
        }

        Blocks.AddBlock(
            blockIndex,
            commitId,
            false,   // isStoredInDb
            blockContent.AsStringBuf());

        existingCommitIds.clear();
        garbage.clear();
    }

    IncrementUnflushedFreshBlocksFromChannelCount(writeRange.Size());
}

////////////////////////////////////////////////////////////////////////////////

void TPartitionFreshBlocksState::DumpHtml(IOutputStream& out) const
{
    Y_ABORT_UNLESS(ContextProvider);

    HTML (out) {
        TABLER () {
            TABLED () {
                out << "FreshBlocks";
            }
            TABLED () {
                out << "Total: " << GetUnflushedFreshBlocksCount()
                    << ", FromDb: "
                    << ContextProvider->GetStats().GetFreshBlocksCount()
                    << ", FromChannel: " << UnflushedFreshBlocksFromChannelCount
                    << ", InFlight: " << GetFreshBlocksInFlight()
                    << ", Queued: " << GetFreshBlocksQueued()
                    << ", UntrimmedBytes: " << GetUntrimmedFreshBlobByteCount();
            }
        }
        TABLER () {
            TABLED () {
                out << "Flush";
            }
            TABLED () {
                DumpOperationState(out, GetFlushState());
            }
        }
    }
}

void TPartitionFreshBlocksState::AsJson(NJson::TJsonValue& state) const
{
    Y_ABORT_UNLESS(ContextProvider);

    state["FreshBlocksTotal"] = GetUnflushedFreshBlocksCount();
    state["FreshBlocksFromDb"] =
        ContextProvider->GetStats().GetFreshBlocksCount();
    state["FreshBlocksFromChannel"] = UnflushedFreshBlocksFromChannelCount;
    state["FreshBlocksInFlight"] = GetFreshBlocksInFlight();
    state["FreshBlocksQueued"] = GetFreshBlocksQueued();
    state["FreshBlobUntrimmedBytes"] = GetUntrimmedFreshBlobByteCount();
    state["FlushState"] = ToJson(GetFlushState());
}

}   // namespace NCloud::NBlockStore::NStorage
