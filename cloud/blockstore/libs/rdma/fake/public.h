#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct IFakeRdmaClient;
using IFakeRdmaClientPtr = std::shared_ptr<IFakeRdmaClient>;

}   // namespace NCloud::NBlockStore
