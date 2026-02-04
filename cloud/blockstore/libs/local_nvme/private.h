#pragma once

#include <memory>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ISysFs;
using ISysFsPtr = std::shared_ptr<ISysFs>;

}   // namespace NCloud::NBlockStore
