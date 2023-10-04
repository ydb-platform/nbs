#pragma once

#include <memory>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsServer;
using TOptionsServerPtr = std::shared_ptr<TOptionsServer>;

struct TConfigInitializerServer;
using TConfigInitializerServerPtr = std::shared_ptr<TConfigInitializerServer>;

}   // namespace NCloud::NFileStore::NDaemon
