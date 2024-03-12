#pragma once

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/blockstore/libs/storage/core/request_info.h>

#include <library/cpp/actors/core/actor.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> CreateSessionCacheActor(
    TVector<NProto::TDiskAgentDeviceSession> sessions,
    TString cachePath,
    TRequestInfoPtr requestInfo,
    NActors::IEventBasePtr response);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
