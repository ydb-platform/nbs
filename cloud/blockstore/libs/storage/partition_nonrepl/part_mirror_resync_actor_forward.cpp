#include "part_mirror_resync_actor.h"

#include "part_mirror_resync_util.h"

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandleWriteOrZeroCompleted(
    const TEvNonreplPartitionPrivate::TEvWriteOrZeroCompleted::TPtr& ev,
    const TActorContext& ctx)
{
    Y_UNUSED(ctx);

    const auto requestIdentityKey = ev->Get()->RequestId;
    if (!WriteAndZeroRequestsInProgress.RemoveRequest(requestIdentityKey)) {
        ReportResyncUnexpectedWriteOrZeroCounter(
            {{"disk", PartConfig->GetName()},
             {"requestIdentityKey", requestIdentityKey}});
    }

    DrainActorCompanion.ProcessDrainRequests(ctx);
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
void TMirrorPartitionResyncActor::ForwardRequest(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto replyError = [&] (ui32 errorCode, TString errorMessage)
    {
        auto response = std::make_unique<typename TMethod::TResponse>(
            MakeError(errorCode, std::move(errorMessage)));
        NCloud::Reply(ctx, *ev, std::move(response));
    };

    const auto range = BuildRequestBlockRange(
        *ev->Get(),
        PartConfig->GetBlockSize());

    for (const auto activeRangeId: State.GetActiveResyncRangeSet()) {
        const auto resyncRange =
            RangeId2BlockRange(activeRangeId, PartConfig->GetBlockSize());
        if (range.Overlaps(resyncRange)) {
            replyError(E_REJECTED, TStringBuilder()
                << "Request " << TMethod::Name
                << " intersects with currently resyncing range");
            return;
        }
    }

    const auto requestIdentityKey =
        ev->Get()->Record.GetHeaders().GetVolumeRequestId();
    WriteAndZeroRequestsInProgress.AddWriteRequest(requestIdentityKey, range);

    ForwardRequestWithNondeliveryTracking(ctx, MirrorActorId, *ev);
}

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionResyncActor::HandleWriteBlocks(
    const TEvService::TEvWriteBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardRequest<TEvService::TWriteBlocksMethod>(ev, ctx);
}

void TMirrorPartitionResyncActor::HandleWriteBlocksLocal(
    const TEvService::TEvWriteBlocksLocalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardRequest<TEvService::TWriteBlocksLocalMethod>(ev, ctx);
}

void TMirrorPartitionResyncActor::HandleZeroBlocks(
    const TEvService::TEvZeroBlocksRequest::TPtr& ev,
    const TActorContext& ctx)
{
    ForwardRequest<TEvService::TZeroBlocksMethod>(ev, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
