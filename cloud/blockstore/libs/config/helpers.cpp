#include "helpers.h"

#include <util/string/builder.h>

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

TResultOrError<NProto::TBlockstoreConfig> ExtractBlockstoreConfig(
    const google::protobuf::Message& privateDatabaseConfig)
{
    // Preserve parser diagnostics while classifying every TError as a failure.
    const auto* descriptor = privateDatabaseConfig.GetDescriptor();
    if (descriptor == NCloud::NProto::TError::descriptor()) {
        NCloud::NProto::TError error;
        error.CopyFrom(privateDatabaseConfig);
        return MakeError(
            E_ARGUMENT,
            TStringBuilder()
                << "Failed to parse PrivateDatabaseConfig from CMS: "
                << FormatError(error));
    }

    // Reject unexpected types without including their configuration values.
    if (descriptor != NProto::TBlockstoreConfig::descriptor()) {
        return MakeError(
            E_INVALID_STATE,
            TStringBuilder()
                << "Internal error: received an unexpected "
                   "PrivateDatabaseConfig payload type "
                << descriptor->full_name() << "; expected "
                << NProto::TBlockstoreConfig::descriptor()->full_name());
    }

    // Filter a copy to leave the payload owned by the producer unchanged.
    NProto::TBlockstoreConfig config;
    config.CopyFrom(privateDatabaseConfig);
    RemoveStaticOnlyBlockstoreFields(config);
    return config;
}

}   // namespace NCloud::NBlockStore
