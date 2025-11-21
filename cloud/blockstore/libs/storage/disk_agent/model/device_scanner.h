#pragma once

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/storage/core/libs/common/error.h>

#include <functional>

class TLog;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TDeviceCallback = std::function<NProto::TError(
    const TString& path,
    const NProto::TStorageDiscoveryConfig::TPoolConfig& poolConfig,
    ui32 deviceNumber,
    ui32 maxDeviceCount,
    ui32 blockSize,
    ui64 fileSize)>;

const NProto::TStorageDiscoveryConfig::TPoolConfig* FindPoolConfig(
    const NProto::TStorageDiscoveryConfig::TPathConfig& pathConfig,
    ui64 fileSize);

ui64 GetFileLengthWithSeek(const TString& path);

NProto::TError FindDevices(
    const NProto::TStorageDiscoveryConfig& config,
    const THashSet<TString>& pathsAllowedList,
    TDeviceCallback callback);

}   // namespace NCloud::NBlockStore::NStorage
