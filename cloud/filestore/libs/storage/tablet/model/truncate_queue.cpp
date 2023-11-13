#include "truncate_queue.h"

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void TTruncateQueue::EnqueueOperation(ui64 nodeId, TByteRange range)
{
    PendingOps.push_back({nodeId, range});
}

TTruncateQueue::TEntry TTruncateQueue::DequeueOperation()
{
    auto entry = std::move(PendingOps.back());
    PendingOps.pop_back();

    ActiveOps.insert(entry.NodeId);

    return entry;
}

bool TTruncateQueue::HasPendingOperations() const
{
    return !PendingOps.empty();
}

void TTruncateQueue::CompleteOperation(ui64 nodeId)
{
    ActiveOps.erase(nodeId);
}

bool TTruncateQueue::HasActiveOperation(ui64 nodeId) const
{
    return ActiveOps.count(nodeId);
}

}   // namespace NCloud::NFileStore::NStorage
