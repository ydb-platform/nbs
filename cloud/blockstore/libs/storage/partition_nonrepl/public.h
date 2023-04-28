#pragma once

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TNonreplicatedPartitionConfig;
using TNonreplicatedPartitionConfigPtr =
    std::shared_ptr<TNonreplicatedPartitionConfig>;

}   // namespace NCloud::NBlockStore::NStorage
