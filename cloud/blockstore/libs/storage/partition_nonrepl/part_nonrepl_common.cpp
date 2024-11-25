#include "part_nonrepl_common.h"

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

void DeclineGetChangedBlocks(
    const TEvService::TEvGetChangedBlocksRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto response = std::make_unique<TEvService::TEvGetChangedBlocksResponse>(
        MakeError(E_ARGUMENT, "GetChangedBlocks not supported"));
    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NStorage
