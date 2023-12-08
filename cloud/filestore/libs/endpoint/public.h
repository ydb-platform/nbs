#pragma once

#include <memory>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct IEndpoint;
using IEndpointPtr = std::shared_ptr<IEndpoint>;

struct IEndpointListener;
using IEndpointListenerPtr = std::shared_ptr<IEndpointListener>;

}   // namespace NCloud::NFileStore
