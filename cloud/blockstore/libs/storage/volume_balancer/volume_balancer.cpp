#include "volume_balancer.h"

#include "volume_balancer_actor.h"

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

IActorPtr CreateVolumeBalancerActor(
    TStorageConfigPtr storageConfig,
    IVolumeStatsPtr volumeStats,
    ICgroupStatsFetcherPtr cgroupStatFetcher,
    IVolumeBalancerSwitchPtr volumeBalancerSwitch,
    NActors::TActorId serviceActorId)
{
    return std::make_unique<TVolumeBalancerActor>(
        std::move(storageConfig),
        std::move(volumeStats),
        std::move(cgroupStatFetcher),
        std::move(volumeBalancerSwitch),
        serviceActorId);
}

IActorPtr CreateVolumeBalancerActorStub()
{
    return std::make_unique<TVolumeBalancerActor>();
}

}   // namespace NCloud::NBlockStore::NStorage
