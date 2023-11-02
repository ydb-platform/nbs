#pragma once

#include <memory>

namespace NCloud::NBlockStore::NPlugin {

////////////////////////////////////////////////////////////////////////////////

struct IPlugin;
using IPluginPtr = std::shared_ptr<IPlugin>;

}   // namespace NCloud::NBlockStore::NPlugin
