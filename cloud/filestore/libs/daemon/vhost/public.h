#pragma once

#include <memory>

namespace NCloud::NFileStore::NDaemon {

////////////////////////////////////////////////////////////////////////////////

struct TOptionsVhost;
using TOptionsVhostPtr = std::shared_ptr<TOptionsVhost>;

struct TConfigInitializerVhost;
using TConfigInitializerVhostPtr = std::shared_ptr<TConfigInitializerVhost>;

class TVhostModuleFactories;
using TVhostModuleFactoriesPtr = std::shared_ptr<TVhostModuleFactories>;

}   // namespace NCloud::NFileStore::NDaemon
