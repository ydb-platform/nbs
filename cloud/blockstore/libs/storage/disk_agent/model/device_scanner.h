#pragma once

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/storage/core/libs/common/error.h>

#include <functional>

class TLog;

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TDeviceCallback = std::function <void (
    const TString& path,
    const NProto::TStorageDiscoveryConfig::TPoolConfig& poolConfig,
    ui32 deviceNumber,
    ui32 maxDeviceCount,
    ui32 blockSize,
    ui64 fileSize)>;

NProto::TError FindDevices(
    const NProto::TStorageDiscoveryConfig& config,
    TDeviceCallback callback);

}   // namespace NCloud::NBlockStore::NStorage
