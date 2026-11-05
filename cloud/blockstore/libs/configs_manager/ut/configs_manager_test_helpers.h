/*******************************************************************************

Inject one configuration preparation failure while retaining the underlying
configuration. Share the same failure wrapper between manager and renderer
tests so both exercise the actual adapter preparation path.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/libs/config/blockstore_config.h>

#include <util/generic/yexception.h>

#include <utility>

namespace NCloud::NBlockStore {

#define FORWARDED_CONFIG_GETTERS(xxx)                                          \
    xxx(GetServerConfig)                                                       \
    xxx(GetFeaturesConfig)                                                     \
    xxx(GetStorageConfig)                                                      \
    xxx(GetDiagnosticsConfig)                                                  \
    xxx(GetDiscoveryServiceConfig)                                             \
    xxx(GetEndpointConfig)                                                     \
    xxx(GetDiskRegistryProxyConfig)                                            \
    xxx(GetSpdkEnvConfig)                                                      \
    xxx(GetRdmaConfig)                                                         \
    xxx(GetYdbStatsConfig)                                                     \
    xxx(GetLogbrokerConfig)                                                    \
    xxx(GetNotifyConfig)                                                       \
    xxx(GetIamClientConfig)                                                    \
    xxx(GetKmsClientConfig)                                                    \
    xxx(GetComputeClientConfig)                                                \
    xxx(GetRootKmsConfig)                                                      \
    xxx(GetCellsConfig)                                                        \
    xxx(GetLocalNVMeConfig)

// A configuration wrapper that fails once while the manager reads adapter
// inputs. Publish it through the holder to exercise recovery without changing
// the actor API. All other reads delegate to the owned configuration.
class TFailingBlockstoreConfig final: public IBlockstoreConfig
{
public:
    explicit TFailingBlockstoreConfig(IBlockstoreConfigConstPtr config);

#define DECLARE_GETTER(name)                                                   \
    decltype(std::declval<IBlockstoreConfig>().name()) name() const override;
    FORWARDED_CONFIG_GETTERS(DECLARE_GETTER)
#undef DECLARE_GETTER

    // Fail on the first read, then allow the same update to be retried.
    const NStorage::TDiskAgentConfigConstPtr&
    GetDiskAgentConfig() const override;

private:
    // The retained configuration supplying every section and shared controls.
    const IBlockstoreConfigConstPtr Config;

    // A pending injected failure, consumed by the first DiskAgent getter call.
    mutable bool FailNextRead = true;
};

inline TFailingBlockstoreConfig::TFailingBlockstoreConfig(
    IBlockstoreConfigConstPtr config)
    : Config(std::move(config))
{}

#define DEFINE_GETTER(name)                                                    \
    inline decltype(std::declval<IBlockstoreConfig>().name())                  \
    TFailingBlockstoreConfig::name() const                                     \
    {                                                                          \
        return Config->name();                                                 \
    }
FORWARDED_CONFIG_GETTERS(DEFINE_GETTER)
#undef DEFINE_GETTER
#undef FORWARDED_CONFIG_GETTERS

// Throw once before adapter construction, leaving the retained data unchanged.
inline const NStorage::TDiskAgentConfigConstPtr&
TFailingBlockstoreConfig::GetDiskAgentConfig() const
{
    if (std::exchange(FailNextRead, false)) {
        ythrow yexception() << "test configuration preparation failure";
    }
    return Config->GetDiskAgentConfig();
}

}   // namespace NCloud::NBlockStore
