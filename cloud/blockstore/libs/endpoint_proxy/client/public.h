#pragma once

#include <memory>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct IEndpointProxyClient;
using IEndpointProxyClientPtr = std::shared_ptr<IEndpointProxyClient>;

}   // namespace NCloud::NBlockStore::NClient
