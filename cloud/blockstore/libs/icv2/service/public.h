#pragma once

#include <memory>

namespace NCloud::NBlockStore::NIcV2 {

struct IHostConnectionHandler;
using IHostConnectionHandlerPtr = std::shared_ptr<IHostConnectionHandler>;

struct IService;
using IServicePtr = std::shared_ptr<IService>;

}   // namespace NCloud::NBlockStore::NIcV2
