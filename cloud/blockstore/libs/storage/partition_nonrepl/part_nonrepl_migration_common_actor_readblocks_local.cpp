#include "part_nonrepl_migration_common_actor.h"

#include <cloud/blockstore/libs/storage/api/undelivered.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardRequestWithNondeliveryTracking(
        ctx,
        UserSrcActorId,
        *ev);
}

}   // namespace NCloud::NBlockStore::NStorage
