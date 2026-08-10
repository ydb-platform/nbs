#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/blockstore/libs/storage/partition_common/model/barrier.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

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
    void Enqueue(TItem item, ui64 commitId);
    TItem Dequeue();

    bool Empty() const
    {
        return Items.empty();
    }

    ui64 Peek() const;
};

using TCommitQueueCallback = std::function<void(const NActors::TActorSystem* actorSystem)>;
using TCommitQueue = TCommitQueueImpl<std::unique_ptr<ITransactionBase>>;
using TCommitQueueWithCallback = TCommitQueueImpl<TCommitQueueCallback>;

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
