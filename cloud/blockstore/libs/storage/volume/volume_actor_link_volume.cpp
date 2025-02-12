#include "volume_actor.h"

#include <cloud/blockstore/libs/storage/core/probes.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

LWTRACE_USING(BLOCKSTORE_STORAGE_PROVIDER);

////////////////////////////////////////////////////////////////////////////////

void TVolumeActor::HandleLinkLeaderVolumeToFollower(
    const TEvVolume::TEvLinkLeaderVolumeToFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Reply(
        ctx,
        *ev,
        std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerResponse>(
            MakeError(E_NOT_IMPLEMENTED)));
}

void TVolumeActor::HandleUnlinkLeaderVolumeFromFollower(
    const TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest::TPtr& ev,
    const TActorContext& ctx)
{
    Reply(
        ctx,
        *ev,
        std::make_unique<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerResponse>(
            MakeError(E_NOT_IMPLEMENTED)));
}

}   // namespace NCloud::NBlockStore::NStorage
