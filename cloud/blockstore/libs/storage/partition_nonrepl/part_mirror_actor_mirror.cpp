#include "part_mirror_actor.h"

#include "mirror_request_actor.h"

#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/probes.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleWriteOrZeroCompleted(
    const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    const auto requestIdentityKey = ev->Get()->RequestCounter;
    RequestsInProgress.RemoveRequest(requestIdentityKey);
    DrainActorCompanion.ProcessDrainRequests(ctx);

    if (ResyncActorId) {
        ForwardRequestWithNondeliveryTracking(
            ctx,
            ResyncActorId,
            *ev);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMirrorPartitionActor::MirrorRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto requestInfo = CreateRequestInfo(
        ev->Sender,
        ev->Cookie,
        msg->CallContext);

    if (HasError(Status)) {
        Reply(
            ctx,
            *requestInfo,
            std::make_unique<typename TMethod::TResponse>(Status)
        );

        return;
    }

    const auto range = BuildRequestBlockRange(
        *ev->Get(),
        State.GetBlockSize());
    const auto requestIdentityKey = ev->Cookie;
    RequestsInProgress.AddWriteRequest(requestIdentityKey, range);

    if (RangeId2BlockRange(ScrubbingRangeId, State.GetBlockSize())
            .Overlaps(range))
    {
        WriteIntersectsWithScrubbing = true;
    }

    NCloud::Register<TMirrorRequestActor<TMethod>>(
        ctx,
        std::move(requestInfo),
        State.GetReplicaActors(),
        std::move(msg->Record),
        State.GetReplicaInfos()[0].Config->GetName(),
        SelfId(),
        requestIdentityKey,
        true // shouldProcessError
    );
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TWriteBlocksMethod>(ev, ctx);
}

void TMirrorPartitionActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TWriteBlocksLocalMethod>(ev, ctx);
}

void TMirrorPartitionActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    MirrorRequest<TEvService::TZeroBlocksMethod>(ev, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
