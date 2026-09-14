#include "helpers.h"

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

void RemoveStaticOnlyBlockstoreFields(NProto::TBlockstoreConfig& config)
{
    if (config.HasServer() && config.GetServer().HasServerConfig()) {
        config.MutableServer()
            ->MutableServerConfig()
            ->ClearDynamicYamlConfigurationEnabled();
    }

    if (config.HasStorageService()) {
        auto* storageConfig = config.MutableStorageService();
        storageConfig->ClearConfigDispatcherSettings();
        storageConfig->ClearSchemeShardDir();
        storageConfig->ClearNodeType();
    }
}

}   // namespace NCloud::NBlockStore
