#pragma once

#include <memory>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

struct IVolumeBalancer;
using IIVolumeBalancerPtr = std::shared_ptr<IVolumeBalancer>;

}   // namespace NCloud::NBlockStore::NStorage
