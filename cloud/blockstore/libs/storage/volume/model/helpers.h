#pragma once

#include "cloud/blockstore/libs/storage/protos_ydb/volume.pb.h"

#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct TLaggingDeviceIndexCmp
{
    bool operator()(
        const NProto::TLaggingDevice& lhs,
        const NProto::TLaggingDevice& rhs) const;
};

[[nodiscard]] const NProto::TDeviceConfig* FindDeviceConfig(
    const NProto::TVolumeMeta& meta, const TStringBuf& deviceUUID);

[[nodiscard]] std::optional<ui32> GetDevicesIndexesByNodeId(
    const NProto::TVolumeMeta& meta,
    ui32 agentNodeId,
    TVector<NProto::TLaggingDevice>* laggingDevices);

[[nodiscard]] bool RowHasFreshDevices(
    const NProto::TVolumeMeta& meta,
    ui32 rowIndex,
    ui32 timeoutedDeviceReplicaIndex);

void RemoveLaggingDevicesFromMeta(
    NProto::TVolumeMeta& meta,
    const TVector<TString>& laggingDeviceIds);

void UpdateLaggingDevicesAfterMetaUpdate(NProto::TVolumeMeta& meta);

}   // namespace NCloud::NBlockStore::NStorage
