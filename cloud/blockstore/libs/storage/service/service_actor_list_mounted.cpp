#include "service_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

void TServiceActor::HandleListMountedVolumes(
    const TEvServicePrivate::TEvListMountedVolumesRequest::TPtr& ev,
    const TActorContext& ctx)
{
    LOG_DEBUG(ctx, TBlockStoreComponents::SERVICE, "Listing mounted volumes");

    auto response =
        std::make_unique<TEvServicePrivate::TEvListMountedVolumesResponse>();
    response->MountedVolumes.reserve(State.GetVolumes().size());
    for (const auto& [id, volume]: State.GetVolumes()) {
        Y_UNUSED(id);

        if (!volume->IsMounted()) {
            continue;
        }

        response->MountedVolumes.emplace_back(
            volume->DiskId,
            volume->VolumeActor);
    }

    NCloud::Reply(ctx, *ev, std::move(response));
}

}   // namespace NCloud::NBlockStore::NStorage
