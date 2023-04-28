#pragma once

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct ISocketEndpointListener;
using ISocketEndpointListenerPtr = std::shared_ptr<ISocketEndpointListener>;

}   // namespace NCloud::NBlockStore::NServer
