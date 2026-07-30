#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/tablet.h>
#include <cloud/blockstore/libs/storage/partition_common/model/barrier.h>

#include <util/generic/deque.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

class TCommitQueue
    : public TBarriers
{
    using TTxPtr = std::unique_ptr<ITransactionBase>;

    struct TItem
    {
        const ui64 CommitId;
        TTxPtr Tx;

        TItem(ui64 commitId, TTxPtr tx)
            : CommitId(commitId)
            , Tx(std::move(tx))
        {}
    };

private:
    TDeque<TItem> Items;

public:
    void Enqueue(TTxPtr tx, ui64 commitId);
    TTxPtr Dequeue();

    bool Empty() const
    {
        return Items.empty();
    }

    ui64 Peek() const;
};

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
