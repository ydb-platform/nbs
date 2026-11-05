/*******************************************************************************

Volume-local ordering of dynamic configuration updates for one consumer actor.
Construct one ordering state per actor and validate each update before replacing
configuration references. The first accepted Volume actor remains the owner
until the consumer is destroyed; revisions are local to that Volume lifetime.

*******************************************************************************/

#pragma once

#include <contrib/ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

// The publication order accepted by one actor from its owning Volume.
// Call TryUpdate before changing configuration or forwarding an update.
class TDynamicConfigUpdateState
{
public:
    // Accept an initial owner or a strictly newer revision from that owner.
    bool TryUpdate(NActors::TActorId owner, ui64 revision);

private:
    // The originating Volume actor; empty before the first accepted update.
    NActors::TActorId Owner;

    // The last accepted Volume-local revision; meaningful after Owner is set.
    ui64 Revision = 0;
};

}   // namespace NCloud::NBlockStore::NStorage
