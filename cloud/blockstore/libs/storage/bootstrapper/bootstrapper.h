#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/public.h>

#include <ydb/library/actors/core/actorid.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TBootstrapperConfig
{
    ui32 SuggestedGeneration = 0;
    ui32 BootAttemptsThreshold = 10;
    TDuration CoolDownTimeout = TDuration::Seconds(10);
    bool RestartAlways = false;
};

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateBootstrapper(
    const TBootstrapperConfig& config,
    const NActors::TActorId& owner,
    NKikimr::TTabletStorageInfoPtr tabletStorageInfo,
    NKikimr::TTabletSetupInfoPtr tabletSetupInfo);

}   // namespace NCloud::NBlockStore::NStorage
