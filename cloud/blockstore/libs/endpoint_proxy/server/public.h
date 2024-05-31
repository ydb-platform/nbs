#pragma once

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointProxyServer;
using IEndpointProxyServerPtr = std::shared_ptr<IEndpointProxyServer>;

}   // namespace NCloud::NBlockStore::NServer
