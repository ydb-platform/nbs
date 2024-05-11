#pragma once

#include <memory>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct IServer;
using IServerPtr = std::shared_ptr<IServer>;

}   // namespace NCloud::NBlockStore::NServer
