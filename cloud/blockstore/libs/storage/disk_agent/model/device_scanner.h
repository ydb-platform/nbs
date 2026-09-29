#pragma once

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

#include <functional>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

using TDeviceCallback = std::function<NProto::TError(
    const TString& path,
    const NProto::TStorageDiscoveryConfig::TPathConfig& pathConfig,
    ui32 deviceNumber,
    ui32 blockSize,
    ui64 fileSize)>;

NProto::TError FindDevices(
    const NProto::TStorageDiscoveryConfig& config,
    const THashSet<TString>& allowedPaths,
    TDeviceCallback callback);

}   // namespace NCloud::NBlockStore::NStorage
