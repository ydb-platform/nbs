#pragma once

#include <memory>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct IPeer;
using IPeerPtr = std::shared_ptr<IPeer>;

struct IChannelFactory;
using IChannelFactoryPtr = std::shared_ptr<IChannelFactory>;

}   // namespace NCloud::NIcV2
