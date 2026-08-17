#include "commit_queue.h"

namespace NCloud::NBlockStore::NStorage {

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
template class TCommitQueueImpl<TCommitQueueCallback>;

template <typename TItem>
std::optional<TItem> WaitForCommitsCompleted(
    TCommitQueueImpl<TItem>& commitQueue,
    ui64 commitId,
    TItem item)
{
    ui64 minCommitId = commitQueue.GetMinCommitId();

    if (minCommitId < commitId) {
        // delay execution until all previous commits completed
        commitQueue.Enqueue(std::move(item), commitId);
        return std::nullopt;
    }

    return item;
}

template <typename TItem>
void ProcessCommitQueue(TCommitQueueImpl<TItem>& commitQueue, TVector<TItem>& items)
{
    ui64 minCommitId = commitQueue.GetMinCommitId();
    while (!commitQueue.Empty()) {
        ui64 commitId = commitQueue.Peek();
        if (minCommitId >= commitId) {
            // start execution
            items.push_back(commitQueue.Dequeue());
        } else {
            break;
        }
    }
}

template std::optional<std::unique_ptr<ITransactionBase>>
WaitForCommitsCompleted(
    TCommitQueue& commitQueue,
    ui64 commitId,
    std::unique_ptr<ITransactionBase> item);

template std::optional<TCommitQueueCallback> WaitForCommitsCompleted(
    TCommitQueueWithCallback& commitQueue,
    ui64 commitId,
    TCommitQueueCallback item);

template void ProcessCommitQueue(
    TCommitQueue& commitQueue,
    TVector<std::unique_ptr<ITransactionBase>>& items);

template void ProcessCommitQueue(
    TCommitQueueWithCallback& commitQueue,
    TVector<TCommitQueueCallback>& items);

}   // namespace NCloud::NBlockStore::NStorage
