#include "tablet_cache_read_bypass.h"

#include <cloud/filestore/libs/storage/tablet/model/verify.h>

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <utility>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TCacheReadBypass::UpdateLogTag(TString logTag)
{
    LogTag = std::move(logTag);
}

void TCacheReadBypass::Activate(ui64 nodeId, ui64 commitId)
{
    CommitIdsByNodeId[nodeId].push_back(commitId);
}

void TCacheReadBypass::Deactivate(ui64 nodeId, ui64 commitId)
{
    auto nodeIt = CommitIdsByNodeId.find(nodeId);
    TABLET_VERIFY_C(
        nodeIt != CommitIdsByNodeId.end(),
        "nodeId: " << nodeId << ", commitId: " << commitId);
    TABLET_VERIFY_C(
        !nodeIt->second.empty(),
        "nodeId: " << nodeId << ", commitId: " << commitId);
    TABLET_VERIFY_C(
        nodeIt->second.front() == commitId,
        "nodeId: " << nodeId << ", expected commitId: " << commitId
                   << ", actual commitId: " << nodeIt->second.front()
                   << ", queue size: " << nodeIt->second.size());

    nodeIt->second.pop_front();
    if (nodeIt->second.empty()) {
        CommitIdsByNodeId.erase(nodeIt);
    }
}

void TCacheReadBypass::SetUnconfirmedRecoveryReady(
    bool unconfirmedRecoveryReady)
{
    UnconfirmedRecoveryReady = unconfirmedRecoveryReady;
}

bool TCacheReadBypass::ShouldBypassRead(ui64 nodeId, ui64 commitId) const
{
    // If recovery is in progress, reading from the cache is not possible.
    if (!UnconfirmedRecoveryReady) {
        return true;
    }

    // No records at all. The map is always empty after the recovery phase if
    // unconfirmed data is disabled, as it is the only client of this API for
    // now.
    if (CommitIdsByNodeId.empty()) {
        return false;
    }

    // If there are no records for the given node, we can read from the cache.
    const auto it = CommitIdsByNodeId.find(nodeId);
    if (it == CommitIdsByNodeId.end() || it->second.empty()) {
        return false;
    }

    // Commit ids are generated monotonically when unconfirmed data is
    // materialized by AddBlob, and this queue is activated/deactivated in the
    // same order. Thus the front item is the oldest write that may still be
    // missing from in-memory caches.
    //
    // A read at "commitId" can observe only writes with commit ids <=
    // "commitId". If the oldest active write is newer than the read snapshot,
    // all other active writes are newer as well and the cache cannot miss any
    // data visible to this read. Otherwise at least one visible write is still
    // active, so the read must bypass the cache and go through the database
    // path to keep the snapshot consistent.
    const ui64 frontCommitId = it->second.front();
    // The InvalidCommitId comparison handles the CommitIdOverflow case.
    return frontCommitId == InvalidCommitId || frontCommitId <= commitId;
}

}   // namespace NCloud::NFileStore::NStorage
