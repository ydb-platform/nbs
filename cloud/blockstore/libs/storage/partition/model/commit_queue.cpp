#include "commit_queue.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

template <typename TItem>
void TCommitQueueImpl<TItem>::Enqueue(TItem item, ui64 commitId)
{
    if (Items) {
        Y_ABORT_UNLESS(Items.back().CommitId < commitId);
    }
    Items.emplace_back(commitId, std::move(item));
}

template <typename TItem>
TItem TCommitQueueImpl<TItem>::Dequeue()
{
    TItem item;
    if (Items) {
        auto& entry = Items.front();
        item = std::move(entry.Item);
        Items.pop_front();
    }
    return item;
}

template <typename TItem>
ui64 TCommitQueueImpl<TItem>::Peek() const
{
    if (Items) {
        return Items.front().CommitId;
    }
    return Max();
}

template class TCommitQueueImpl<std::unique_ptr<ITransactionBase>>;

}   // namespace NCloud::NBlockStore::NStorage::NPartition
