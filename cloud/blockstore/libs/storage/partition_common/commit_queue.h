#pragma once

#include "model/commit_queue.h"

#include <functional>
#include <memory>

namespace NActors {

class TActorSystem;

}   // namespace NActors

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct ITransactionBase;

using TCommitQueueCallback =
    std::function<void(const NActors::TActorSystem* actorSystem)>;
using TCommitQueue = TCommitQueueImpl<std::unique_ptr<ITransactionBase>>;
using TCommitQueueWithCallback = TCommitQueueImpl<TCommitQueueCallback>;

extern template class TCommitQueueImpl<std::unique_ptr<ITransactionBase>>;
extern template class TCommitQueueImpl<TCommitQueueCallback>;

}   // namespace NCloud::NBlockStore::NStorage
