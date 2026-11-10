#pragma once

#include <memory>

namespace NCloud::NBlockStore::NIcV2 {

struct IDataEndpointFactory;
using IDataEndpointFactoryPtr = std::shared_ptr<IDataEndpointFactory>;

struct IHostConnectionHandler;
using IHostConnectionHandlerPtr = std::shared_ptr<IHostConnectionHandler>;

}   // namespace NCloud::NBlockStore::NIcV2
