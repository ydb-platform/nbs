#include "commit_queue.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

void TCommitQueue::Enqueue(TTxPtr tx, ui64 commitId)
{
    if (Items) {
        Y_ABORT_UNLESS(Items.back().CommitId <= commitId);
    }
    Items.emplace_back(commitId, std::move(tx));
}

TCommitQueue::TTxPtr TCommitQueue::Dequeue()
{
    TTxPtr tx;
    if (Items) {
        auto& item = Items.front();
        tx = std::move(item.Tx);
        Items.pop_front();
    }
    return tx;
}

ui64 TCommitQueue::Peek() const
{
    if (Items) {
        return Items.front().CommitId;
    }
    return Max();
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
