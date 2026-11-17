#pragma once

#include <memory>

namespace NCloud::NBlockStore::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct IDataEndpointFactory;
using IDataEndpointFactoryPtr = std::shared_ptr<IDataEndpointFactory>;

}   // namespace NCloud::NBlockStore::NIcV2
