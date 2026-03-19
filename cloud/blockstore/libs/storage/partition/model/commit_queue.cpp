#include "commit_queue.h"

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

void TCommitQueue::Enqueue(TCallback callback, ui64 commitId)
{
    if (Items) {
        Y_ABORT_UNLESS(Items.back().CommitId < commitId);
    }
    Items.emplace_back(commitId, std::move(callback));
}

TCommitQueue::TCallback TCommitQueue::Dequeue()
{
    TCallback callback;
    if (Items) {
        auto& item = Items.front();
        callback = std::move(item.Callback);
        Items.pop_front();
    }
    return callback;
}

ui64 TCommitQueue::Peek() const
{
    if (Items) {
        return Items.front().CommitId;
    }
    return Max();
}

void ProcessCommitQueue(
    const NActors::TActorContext& ctx,
    TCommitQueue& commitQueue)
{
    ui64 minCommitId = commitQueue.GetMinCommitId();

    while (!commitQueue.Empty()) {
        ui64 commitId = commitQueue.Peek();

        if (minCommitId >= commitId) {
            // start execution
            auto callback = commitQueue.Dequeue();
            callback(ctx);
        } else {
            // delay execution until all previous commits completed
            break;
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
