#include "commit_queue.h"

#include <cloud/blockstore/libs/storage/core/tablet.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Keep the transaction definition out of the model library.
template class TCommitQueueImpl<std::unique_ptr<ITransactionBase>>;
template class TCommitQueueImpl<TCommitQueueCallback>;

}   // namespace NCloud::NBlockStore::NStorage
