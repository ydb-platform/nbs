#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <contrib/ydb/library/actors/core/actor.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NActors::IActor> CreateDeviceHealthCheckActor(
    const NActors::TActorId& diskAgent,
    TVector<NProto::TDeviceConfig> devices);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
