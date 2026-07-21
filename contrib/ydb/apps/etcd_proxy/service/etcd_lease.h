#pragma once

#include "etcd_shared.h"
#include <contrib/ydb/library/actors/core/actor.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NEtcd {

NActors::IActor* BuildHolderHouse(TIntrusivePtr<NMonitoring::TDynamicCounters> counters, TSharedStuff::TPtr stuff);

}


