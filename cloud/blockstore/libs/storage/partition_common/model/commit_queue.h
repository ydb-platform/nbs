#pragma once

#include "barrier.h"

#include <util/generic/deque.h>

#include <utility>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

template <typename TItem>
class TCommitQueueImpl: public TBarriers
{
    struct TItemWithCommitId
    {
        const ui64 CommitId;
        TItem Item;

        TItemWithCommitId(ui64 commitId, TItem item)
            : CommitId(commitId)
            , Item(std::move(item))
        {}
    };

private:
    TDeque<TItemWithCommitId> Items;

public:
    TCommitQueueImpl();
    ~TCommitQueueImpl();

    void Enqueue(TItem item, ui64 commitId);
    TItem Dequeue();

    bool Empty() const
    {
        return Items.empty();
    }

    ui64 Peek() const;
};

////////////////////////////////////////////////////////////////////////////////

template <typename TItem>
TCommitQueueImpl<TItem>::TCommitQueueImpl() = default;

template <typename TItem>
TCommitQueueImpl<TItem>::~TCommitQueueImpl() = default;

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

}   // namespace NCloud::NBlockStore::NStorage
