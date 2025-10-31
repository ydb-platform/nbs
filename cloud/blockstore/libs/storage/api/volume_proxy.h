#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/public.h>

#include <ydb/library/actors/core/actorid.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::TActorId MakeVolumeProxyServiceId();

}   // namespace NCloud::NBlockStore::NStorage
