#pragma once

#include "public.h"

#include <cloud/storage/core/libs/actors/public.h>
#include <cloud/storage/core/libs/common/public.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

NActors::IActorPtr CreateHiveProxy(THiveProxyConfig config);
NActors::IActorPtr CreateHiveProxy(
    THiveProxyConfig config,
    NMonitoring::TDynamicCounterPtr counters);

}   // namespace NCloud::NStorage
