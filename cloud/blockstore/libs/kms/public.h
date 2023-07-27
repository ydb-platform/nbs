#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IKmsClient;
using IKmsClientPtr = std::shared_ptr<IKmsClient>;

struct IComputeClient;
using IComputeClientPtr = std::shared_ptr<IComputeClient>;

}   // namespace NCloud::NBlockStore
