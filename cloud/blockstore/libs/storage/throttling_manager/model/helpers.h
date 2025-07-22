#pragma once

#include <cloud/blockstore/libs/storage/api/throttling_manager.h>

namespace NCloud::NBlockStore::NStorage {

NProto::TError ValidateThrottlingConfig(
    const NProto::TThrottlingConfig& config);

}   // namespace NCloud::NBlockStore::NStorage
