#pragma once

#include "public.h"

#include <cloud/blockstore/libs/storage/core/public.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateThrottlingManager(TDuration cycleTime);

}   // namespace NCloud::NBlockStore::NStorage
