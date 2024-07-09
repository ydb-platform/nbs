#pragma once

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> CreateConfigCacheActor(
    TString cachePath);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
