#pragma once

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointStorage;
using IEndpointStoragePtr = std::shared_ptr<IEndpointStorage>;

struct IMutableEndpointStorage;
using IMutableEndpointStoragePtr = std::shared_ptr<IMutableEndpointStorage>;

}   // namespace NCloud
