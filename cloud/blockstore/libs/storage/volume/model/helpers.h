#pragma once

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/protos_ydb/volume.pb.h>

#include <util/generic/map.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] const NProto::TDeviceConfig* FindDeviceConfig(
    const NProto::TVolumeMeta& meta,
    TStringBuf deviceUUID);

[[nodiscard]] std::optional<ui32> FindReplicaIndexByAgentId(
    const NProto::TVolumeMeta& meta,
    TStringBuf agentId);

[[nodiscard]] TVector<NProto::TLaggingDevice> CollectLaggingDevices(
    const NProto::TVolumeMeta& meta,
    ui32 replicaIndex,
    TStringBuf agentId);

[[nodiscard]] bool RowHasFreshDevices(
    const NProto::TVolumeMeta& meta,
    ui32 rowIndex,
    ui32 timedOutDeviceReplicaIndex);

[[nodiscard]] bool HaveCommonRows(
    const google::protobuf::RepeatedPtrField<NProto::TLaggingDevice>&
        laggingCandidates,
    const google::protobuf::RepeatedPtrField<NProto::TLaggingDevice>&
        alreadyLagging);

void UpdateLaggingDevicesAfterMetaUpdate(
    NProto::TVolumeMeta& meta,
    const TVector<TString>& removedLaggingDeviceIds);

[[nodiscard]] TVector<NProto::TDeviceConfig> GetReplacedDevices(
    const NProto::TVolumeMeta& oldMeta,
    const NProto::TVolumeMeta& newMeta);

[[nodiscard]] TMap<TString, TString> ParseTags(const TString& tags);

}   // namespace NCloud::NBlockStore::NStorage
