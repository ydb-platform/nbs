#include "service_actor.h"

#include "service.h"

#include <cloud/blockstore/libs/kikimr/trace.h>
#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <ydb/core/tablet/tablet_setup.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleUnmountVolume(
    const TEvService::TEvUnmountVolumeRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();
    const auto& diskId = GetDiskId(*msg);

    auto volume = State.GetVolume(diskId);

    if (!volume || !volume->VolumeSessionActor) {

        BLOCKSTORE_TRACE_RECEIVED(ctx, &ev->TraceId, this, msg);

        auto response = std::make_unique<TEvService::TEvUnmountVolumeResponse>(
            MakeError(
                S_ALREADY,
                TStringBuilder() << "Volume not mounted: " << diskId.Quote()));

        BLOCKSTORE_TRACE_SENT(ctx, &ev->TraceId, this, response);
        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    ForwardRequestWithNondeliveryTracking(ctx, volume->VolumeSessionActor, *ev);
}

}   // namespace NCloud::NBlockStore::NStorage
