#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>

#include <cloud/blockstore/config/disk.pb.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

// `expectedConfig` and `currentConfig` must be sorted by `DeviceId`.
NProto::TError CompareConfigs(
    const TVector<NProto::TFileDeviceArgs>& expectedConfig,
    const TVector<NProto::TFileDeviceArgs>& currentConfig,
    bool strictCompare = false);

}   // namespace NCloud::NBlockStore::NStorage
