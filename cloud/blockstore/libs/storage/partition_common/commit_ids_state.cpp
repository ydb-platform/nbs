#include "commit_ids_state.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

TCommitIdsState::TCommitIdsState(ui64 generation, ui64 lastCommitId)
    : Generation(generation)
    , LastCommitId(lastCommitId)
{}

}   // namespace NCloud::NBlockStore::NStorage
