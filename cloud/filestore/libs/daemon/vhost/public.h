#pragma once

#include <memory>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsVhost;
using TOptionsVhostPtr = std::shared_ptr<TOptionsVhost>;

class TConfigInitializerVhost;
using TConfigInitializerVhostPtr = std::shared_ptr<TConfigInitializerVhost>;

struct TVhostModuleFactories;
using TVhostModuleFactoriesPtr = std::shared_ptr<TVhostModuleFactories>;

}   // namespace NCloud::NFileStore::NDaemon
