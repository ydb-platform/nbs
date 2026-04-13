#pragma once

#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <contrib/ydb/library/actors/core/actor.h>

#include <memory>

namespace NCloud::NBlockStore::NStorage::NDiskAgent {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration DefaultPartlabelCheckInterval = TDuration::Minutes(5);

std::unique_ptr<NActors::IActor> CreateDeviceIntegrityCheckActor(
    const NActors::TActorId& diskAgent,
    TVector<NProto::TDeviceConfig> devices,
    TDuration healthCheckDelay,
    NNvme::INvmeManagerPtr nvmeManager,
    TDuration partlabelCheckInterval);

}   // namespace NCloud::NBlockStore::NStorage::NDiskAgent
