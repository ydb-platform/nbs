#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/discovery/public.h>
#include <cloud/blockstore/libs/kikimr/helpers.h>
#include <cloud/blockstore/libs/kikimr/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/ydbstats/public.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/maybe.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateVolumeBalancerActor(
    TStorageConfigPtr storageConfig,
    IVolumeStatsPtr volumeStats,
    NCloud::NStorage::IStatsFetcherPtr cgroupStatFetcher,
    IVolumeBalancerSwitchPtr volumeBalancerSwitch,
    NActors::TActorId serviceActorId);

NActors::IActorPtr CreateVolumeBalancerActorStub();

}   // namespace NCloud::NBlockStore::NStorage
