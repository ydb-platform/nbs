#include "helper.h"

#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>

namespace NCloud::NBlockStore::NPlugin {

////////////////////////////////////////////////////////////////////////////////

NProto::TClientAppConfig ParseClientAppConfig(
    const NProto::TPluginConfig& pluginConfig, 
    const TString& defaultClientConfigPath, 
    const TString& fallbackClientConfigPath)
{
    NProto::TClientAppConfig appConfig;
    
    auto configPath = pluginConfig.GetClientConfig();
    if (!configPath) {
        configPath = defaultClientConfigPath;
    }
    if (!TryParseFromTextFormat(configPath, appConfig)) {
        // Use default config for all clusters when deploying with k8s
        // see https://st.yandex-team.ru/NBS-3250
        if (!TryParseFromTextFormat(fallbackClientConfigPath, appConfig)) {
            ythrow yexception() << "can't parse client config from file";
        }
    }

    return appConfig;
}

}   // namespace NCloud::NBlockStore::NPlugin
