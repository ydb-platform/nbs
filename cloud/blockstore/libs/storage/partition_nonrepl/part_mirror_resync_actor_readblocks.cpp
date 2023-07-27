#include "part_mirror_resync_actor.h"

#include "part_mirror_resync_util.h"

#include <cloud/blockstore/libs/kikimr/components.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMirrorPartitionResyncActor::ProcessReadRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto range = BuildRequestBlockRange(
        *ev->Get(),
        PartConfig->GetBlockSize());

    if (ResyncFinished || State.IsResynced(range)) {
        ForwardRequestWithNondeliveryTracking(ctx, MirrorActorId, *ev);
        return;
    }

    const auto rangeId = BlockRange2RangeId(range, PartConfig->GetBlockSize());
    for (ui32 id = rangeId.first; id <= rangeId.second; ++id) {
        const auto blockRange =
            RangeId2BlockRange(id, PartConfig->GetBlockSize());
        if (!State.IsResynced(blockRange) && State.AddPendingResyncRange(id)) {
            ResyncNextRange(ctx);
        }
    }

    PostponedReads.push_back({
        NActors::IEventHandlePtr(ev.Release()),
        range});
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandleReadBlocks(
    const TEvService::TEvReadBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ProcessReadRequest<TEvService::TReadBlocksMethod>(ev, ctx);
}

void TMirrorPartitionResyncActor::HandleReadBlocksLocal(
    const TEvService::TEvReadBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ProcessReadRequest<TEvService::TReadBlocksLocalMethod>(ev, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
