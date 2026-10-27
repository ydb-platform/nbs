#pragma once

#include <cloud/blockstore/config/storage.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

// Reconstructs the ascending slot sizes of a new YDB huge heap.
// Invalid settings and slots exceeding ui32 return E_ARGUMENT.
[[nodiscard]] TResultOrError<TVector<ui32>> CalculateHugeSlotSizes(
    const NProto::THugeSlotConfig& config);

}   // namespace NCloud::NBlockStore::NStorage
