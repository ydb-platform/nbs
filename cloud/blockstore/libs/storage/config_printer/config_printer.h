#pragma once

#include <cloud/storage/core/libs/actors/public.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateConfigPrinter();

}   // namespace NCloud::NBlockStore::NStorage
