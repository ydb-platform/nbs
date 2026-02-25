#include "commit_ids_state.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TCommitIdsState::TCommitIdsState(TCommitIdGeneratorPtr generator)
    : CommitIdGenerator(std::move(generator))
{}


TCommitIdsState::TCommitIdsState(ui64 generation, ui64 lastCommitId)
    : CommitIdGenerator(
          std::make_shared<TCommitIdGenerator>(generation, lastCommitId))
{}

}   // namespace NCloud::NBlockStore::NStorage
