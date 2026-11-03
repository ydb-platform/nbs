#include "dynamic_config_update.h"

namespace NCloud::NBlockStore::NStorage {

// Reject duplicate, stale and foreign publications for this consumer lifetime.
bool TDynamicConfigUpdateState::TryUpdate(NActors::TActorId owner,
                                          ui64 revision)
{
    if (!owner || (Owner && (Owner != owner || revision <= Revision))) {
        return false;
    }

    Owner = owner;
    Revision = revision;
    return true;
}

}   // namespace NCloud::NBlockStore::NStorage
