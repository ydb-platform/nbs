#pragma once

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <ydb/library/actors/core/actor.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> CreateSessionCacheActor(
    TString cachePath,
    TDuration releaseInactiveSessionsTimeout);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
