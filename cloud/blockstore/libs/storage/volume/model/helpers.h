#pragma once

#include "cloud/blockstore/libs/storage/protos_ydb/volume.pb.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

namespace NCloud::NBlockStore::NStorage {

struct TLaggingDeviceIndexCmp
{
    bool operator()(
        const NProto::TLaggingDevice& lhs,
        const NProto::TLaggingDevice& rhs) const;
};

[[nodiscard]] const NProto::TDeviceConfig* FindDeviceConfig(
    const NProto::TVolumeMeta& meta, const TString& deviceUUID);

[[nodiscard]] std::optional<ui32> GetAgentDevicesIndexes(
    const NProto::TVolumeMeta& meta,
    ui32 agentNodeId,
    TVector<NProto::TLaggingDevice>& laggingDevices);

[[nodiscard]] TSet<ui32> ReplicaIndexesWithFreshDevices(
    const NProto::TVolumeMeta& meta,
    NProto::TLaggingDevice device);

[[nodiscard]] bool CheckReplicasPlacementAreCorrect(
    const NProto::TVolumeMeta& meta,
    ui32 agentNodeId);

void RemoveLaggingDevicesFromMeta(
    NProto::TVolumeMeta& meta,
    const TVector<TString> laggingDeviceIds);

}   // namespace NCloud::NBlockStore::NStorage
