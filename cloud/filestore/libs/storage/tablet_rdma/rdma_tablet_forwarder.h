#pragma once

#include "rdma_counters.h"

#include <contrib/ydb/library/actors/core/actor.h>

#include <util/datetime/base.h>

#include <memory>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// Per-node actor that receives the tablets' responses to RDMA-injected
// requests and resolves their promises. requestTimeout of zero disables the
// timeout. Later we want to scale it by the number of tablets
std::unique_ptr<NActors::IActor> CreateRdmaTabletForwarderActor(
    TDuration requestTimeout,
    TRdmaForwarderCountersPtr counters);

}   // namespace NCloud::NFileStore::NStorage
