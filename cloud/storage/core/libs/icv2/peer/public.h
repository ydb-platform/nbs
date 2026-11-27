#pragma once

#include <memory>

namespace NCloud::NIcV2 {

////////////////////////////////////////////////////////////////////////////////

struct IPeer;
using IPeerPtr = std::shared_ptr<IPeer>;

struct IRequestChannelFactory;
using IRequestChannelFactoryPtr = std::shared_ptr<IRequestChannelFactory>;

}   // namespace NCloud::NIcV2
