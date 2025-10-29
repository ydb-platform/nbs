#include "service_actor.h"

#include "service.h"

#include <cloud/blockstore/libs/storage/api/undelivered.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>

#include <ydb/core/tablet/tablet_setup.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

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

        auto response = std::make_unique<TEvService::TEvUnmountVolumeResponse>(
            MakeError(
                S_ALREADY,
                TStringBuilder() << "Volume not mounted: " << diskId.Quote()));

        NCloud::Reply(ctx, *ev, std::move(response));
        return;
    }

    ForwardRequestWithNondeliveryTracking(ctx, volume->VolumeSessionActor, *ev);
}

}   // namespace NCloud::NBlockStore::NStorage
