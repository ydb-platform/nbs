#include "part_nonrepl_migration_common_actor.h"

#include <cloud/blockstore/libs/storage/api/undelivered.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TNonreplicatedPartitionMigrationCommonActor::HandleChecksumBlocks(
    const TEvNonreplPartitionPrivate::TEvChecksumBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardRequestWithNondeliveryTracking(ctx, SrcActorId, *ev);
}

}   // namespace NCloud::NBlockStore::NStorage
