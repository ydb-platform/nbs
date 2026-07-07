#include "tablet_cache_read_bypass.h"

#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <util/generic/utility.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TCacheReadBypass::UpdateLogTag(TString logTag)
{
    LogTag = std::move(logTag);
}

void TCacheReadBypass::Activate(
    ui64 nodeId,
    ui64 commitId,
    const TByteRange& range)
{
    ActiveWritesByNodeId[nodeId].push_back({commitId, range});
}

void TCacheReadBypass::Deactivate(ui64 nodeId, ui64 commitId)
{
    auto nodeIt = ActiveWritesByNodeId.find(nodeId);
    TABLET_VERIFY_C(
        nodeIt != ActiveWritesByNodeId.end(),
        "nodeId: " << nodeId << ", commitId: " << commitId);
    TABLET_VERIFY_C(
        !nodeIt->second.empty(),
        "nodeId: " << nodeId << ", commitId: " << commitId);
    TABLET_VERIFY_C(
        nodeIt->second.front().CommitId == commitId,
        "nodeId: " << nodeId << ", expected commitId: " << commitId
                   << ", actual commitId: " << nodeIt->second.front().CommitId
                   << ", queue size: " << nodeIt->second.size());

    nodeIt->second.pop_front();
    if (nodeIt->second.empty()) {
        ActiveWritesByNodeId.erase(nodeIt);
    }
}

void TCacheReadBypass::SetUnconfirmedRecoveryReady(
    bool unconfirmedRecoveryReady)
{
    UnconfirmedRecoveryReady = unconfirmedRecoveryReady;
}

bool TCacheReadBypass::ShouldBypassRead(ui64 nodeId, ui64 commitId) const
{
    // Reads without a byte range may observe any part of the node, including
    // its size.
    const bool bypass = ShouldBypassReadImpl(nodeId, commitId, nullptr);
    if (bypass) {
        ++BypassedNodeReadCount;
    }
    return bypass;
}

bool TCacheReadBypass::ShouldBypassRead(
    ui64 nodeId,
    ui64 commitId,
    const TByteRange& range) const
{
    const bool bypass = ShouldBypassReadImpl(nodeId, commitId, &range);
    if (bypass) {
        ++BypassedRangeReadCount;
    }
    return bypass;
}

bool TCacheReadBypass::ShouldBypassReadImpl(
    ui64 nodeId,
    ui64 commitId,
    const TByteRange* range) const
{
    // If recovery is in progress, reading from the cache is not possible.
    if (!UnconfirmedRecoveryReady) {
        return true;
    }

    // No records at all. The map is always empty after the recovery phase if
    // unconfirmed data is disabled, as it is the only client of this API for
    // now.
    if (ActiveWritesByNodeId.empty()) {
        return false;
    }

    // If there are no records for the given node, we can read from the cache.
    const auto it = ActiveWritesByNodeId.find(nodeId);
    if (it == ActiveWritesByNodeId.end() || it->second.empty()) {
        return false;
    }

    // A read at "commitId" can observe only writes with commit ids <=
    // "commitId", and commit ids in the queue are monotonically increasing
    // (they are generated when unconfirmed data is materialized by AddBlob,
    // and the queue is activated/deactivated in the same order), so the
    // writes visible to the read form a prefix of the queue. If some visible
    // write intersects the read range, the caches may miss data visible to
    // this read, so the read must bypass them and go through the database
    // path to keep the snapshot consistent. Writes that are newer than the
    // read snapshot or do not intersect the read range cannot affect the
    // result of the read.
    for (const auto& write: it->second) {
        // The InvalidCommitId comparison handles the CommitIdOverflow case.
        const bool visible =
            write.CommitId == InvalidCommitId || write.CommitId <= commitId;
        if (!visible) {
            break;
        }

        if (!range || range->Overlaps(write.Range)) {
            return true;
        }
    }

    return false;
}

ui64 TCacheReadBypass::GetBypassedNodeReadCount() const
{
    return BypassedNodeReadCount;
}

ui64 TCacheReadBypass::GetBypassedRangeReadCount() const
{
    return BypassedRangeReadCount;
}

}   // namespace NCloud::NFileStore::NStorage
