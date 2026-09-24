#include "helpers.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void RemoveStaticOnlyBlockstoreFields(NProto::TBlockstoreConfig& config)
{
    if (config.HasServer() && config.GetServer().HasServerConfig()) {
        auto* serverConfig = config.MutableServer()->MutableServerConfig();
        serverConfig->ClearDynamicYamlConfigurationEnabled();
        if (serverConfig->ByteSizeLong() == 0) {
            config.MutableServer()->ClearServerConfig();
        }
    }

    if (config.HasServer() && config.GetServer().ByteSizeLong() == 0) {
        config.ClearServer();
    }

    if (config.HasStorageService()) {
        auto* storageConfig = config.MutableStorageService();
        storageConfig->ClearConfigDispatcherSettings();
        storageConfig->ClearSchemeShardDir();
        storageConfig->ClearNodeType();
        if (storageConfig->ByteSizeLong() == 0) {
            config.ClearStorageService();
        }
    }
}

}   // namespace NCloud::NBlockStore
