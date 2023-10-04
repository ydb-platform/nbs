#pragma once

#include <memory>

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

class TServerConfig;
using TServerConfigPtr = std::shared_ptr<TServerConfig>;

struct IServer;
using IServerPtr = std::shared_ptr<IServer>;

}   // namespace NCloud::NFileStore::NServer
