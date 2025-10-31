#include "part2_actor.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleGetUsedBlocks(
    const TEvVolume::TEvGetUsedBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo<TEvVolume::TDescribeBlocksMethod>(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    // TODO(NBS-2364): support correct get used blocks for partition2
    auto response = std::make_unique<TEvVolume::TEvGetUsedBlocksResponse>();

    NCloud::Reply(ctx, *requestInfo, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
