#pragma once

#include <cloud/blockstore/config/client.pb.h>
#include <cloud/blockstore/config/plugin.pb.h>

namespace NCloud::NBlockStore::NPlugin {

////////////////////////////////////////////////////////////////////////////////

NProto::TClientAppConfig ParseClientAppConfig(
    const NProto::TPluginConfig& pluginConfig,
    const TString& defaultClietnConfigPath,
    const TString& fallbackClientConfigPath);

}   // namespace NCloud::NBlockStore::NPlugin
