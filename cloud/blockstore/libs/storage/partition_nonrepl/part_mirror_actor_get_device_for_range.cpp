#include "part_mirror_actor.h"

#include <cloud/blockstore/libs/storage/core/forward_helpers.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TMirrorPartitionActor::HandleGetDeviceForRange(
    const TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    auto* msg = ev->Get();

    auto replyError = [&](NProto::TError error)
    {
        NCloud::Reply(
            ctx,
            *ev,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse>(
                std::move(error)));
    };

    if (ResyncRangeStarted && GetScrubbingRange().Overlaps(msg->BlockRange)) {
        replyError(MakeError(
            E_REJECTED,
            TStringBuilder() << "Request GetDeviceForRange intersects with "
                                "currently resyncing range"));
        return;
    }

    TActorId replicaActorId;
    auto error = State.NextReadReplica(msg->BlockRange, &replicaActorId);

    if (HasError(error)) {
        replyError(std::move(error));
        return;
    }

    GetDeviceForRangeCompanion.SetDelegate(replicaActorId);
    GetDeviceForRangeCompanion.HandleGetDeviceForRange(ev, ctx);
}

}   // namespace NCloud::NBlockStore::NStorage
