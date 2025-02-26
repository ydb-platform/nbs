#pragma once

#include <cloud/storage/core/protos/error.pb.h>

namespace NCloud::NBlockStore::NStorage {

std::optional<NProto::TError> ValidateBlocksCount(
    ui64 blocksCount,
    ui64 bytesPerStripe,
    ui64 blockSize,
    ui64 checkRangeMaxRangeSize);

}   // namespace NCloud::NBlockStore::NStorage
