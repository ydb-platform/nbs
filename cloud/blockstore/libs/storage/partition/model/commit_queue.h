#pragma once

#include "public.h"

#include "barrier.h"

#include <cloud/blockstore/libs/storage/core/tablet.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

class TCommitQueue: public TBarriers
{
    using TCallback = std::function<void(const NActors::TActorContext& ctx)>;

    struct TItem
    {
        const ui64 CommitId;
        TCallback Callback;

        TItem(ui64 commitId, TCallback callback)
            : CommitId(commitId)
            , Callback(std::move(callback))
        {}
    };

private:
    TDeque<TItem> Items;

public:
    void Enqueue(TCallback callback, ui64 commitId);
    TCallback Dequeue();

    bool Empty() const
    {
        return Items.empty();
    }

    ui64 Peek() const;
};

void ProcessCommitQueue(
    const NActors::TActorContext& ctx,
    TCommitQueue& commitQueue);

}   // namespace NCloud::NBlockStore::NStorage::NPartition
