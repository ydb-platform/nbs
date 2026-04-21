#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

struct TDeviceIntegrityCheckParams
{
    TDuration HealthCheckInterval;
    TDuration SymlinkCheckInterval;
};

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration DefaultSymlinkCheckInterval = TDuration::Minutes(5);

std::unique_ptr<NActors::IActor> CreateDeviceIntegrityCheckActor(
    const NActors::TActorId& diskAgent,
    TVector<NProto::TDeviceConfig> devices,
    TDeviceIntegrityCheckParams params);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
