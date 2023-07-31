#pragma once

#include "public.h"

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/public.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateHiveProxy(THiveProxyConfig config);

}   // namespace NCloud::NStorage
