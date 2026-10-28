#pragma once

#include <cloud/blockstore/libs/kikimr/public.h>

namespace NCloud::NBlockStore::NStorage {

NActors::IActorPtr CreateCellProxy();

}   // namespace NCloud::NBlockStore::NStorage
