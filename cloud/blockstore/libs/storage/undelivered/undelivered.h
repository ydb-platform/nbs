#pragma once

#include "public.h"

#include <cloud/blockstore/libs/kikimr/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateUndeliveredHandler();

}   // namespace NCloud::NBlockStore::NStorage
