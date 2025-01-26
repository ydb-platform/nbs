#pragma once

#include "cloud/blockstore/libs/storage/protos_ydb/volume.pb.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <span>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TLaggingDeviceIndexCmp
{
    bool operator()(
        const NProto::TLaggingDevice& lhs,
        const NProto::TLaggingDevice& rhs) const;
};

[[nodiscard]] const NProto::TDeviceConfig* FindDeviceConfig(
    const NProto::TVolumeMeta& meta,
    TStringBuf deviceUUID);

[[nodiscard]] std::optional<ui32> FindReplicaIndexByAgentNodeId(
    const NProto::TVolumeMeta& meta,
    ui32 agentNodeId);
[[nodiscard]] std::optional<ui32> FindReplicaIndexByAgentId(
    const NProto::TVolumeMeta& meta,
    TStringBuf agentId);

[[nodiscard]] TVector<NProto::TLaggingDevice> CollectLaggingDevices(
    const NProto::TVolumeMeta& meta,
    ui32 replicaIndex,
    ui32 agentNodeId);
[[nodiscard]] TVector<NProto::TLaggingDevice> CollectLaggingDevices(
    const NProto::TVolumeMeta& meta,
    ui32 replicaIndex,
    TStringBuf agentId);

[[nodiscard]] bool RowHasFreshDevices(
    const NProto::TVolumeMeta& meta,
    ui32 rowIndex,
    ui32 timeoutedDeviceReplicaIndex);

[[nodiscard]] bool HaveCommonRows(
    const TVector<NProto::TLaggingDevice>& laggingCandidates,
    const google::protobuf::RepeatedPtrField<NProto::TLaggingDevice>&
        alreadyLagging);

void RemoveLaggingDevicesFromMeta(
    NProto::TVolumeMeta& meta,
    const TVector<TString>& laggingDeviceIds);

void UpdateLaggingDevicesAfterMetaUpdate(NProto::TVolumeMeta& meta);

}   // namespace NCloud::NBlockStore::NStorage
