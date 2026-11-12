/*******************************************************************************

Shared ConfigsManager test infrastructure for update and monitoring tests.
Start one manager explicitly per fixture and use the same startup preparation,
delivery protocol, and logging setup in both test modules. The failure wrapper
injects an adapter preparation error without changing the actor API.

*******************************************************************************/

#pragma once

#include <cloud/blockstore/config/blockstore.pb.h>
#include <cloud/blockstore/libs/config/blockstore_config.h>
#include <cloud/blockstore/libs/config/blockstore_config_holder.h>

#include <contrib/ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/yexception.h>
#include <util/stream/str.h>

#include <utility>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

// Create a minimal storage configuration with the requested blob threshold.
NProto::TBlockstoreConfig MakeConfig(ui32 writeBlobThreshold);

// A per-test ConfigsManager environment owning its runtime, captured logs,
// configuration holder, and ICB controls. Start one manager after SetUp;
// subscription and monitoring helpers send events only when called.
class TConfigsManagerFixture: public NUnitTest::TBaseFixture
{
protected:
    // Critical-event messages captured until TearDown resets the global logger.
    TStringStream CriticalEventsLog;

    // Actor log messages captured while Runtime owns the logging backend.
    TStringStream ConfigLog;

    // The actor runtime under test.
    NActors::TTestActorRuntime Runtime;

    // The edge actor receiving subscription requests and acknowledgements.
    NActors::TActorId Dispatcher;

    // The ConfigsManager actor address registered for the current test.
    NActors::TActorId Manager;

    // The shared holder inspected after each delivered update.
    TBlockstoreConfigHolderPtr ConfigHolder;

    // The shared controls required when rebuilding storage adapters.
    NStorage::TStorageConfigControlsPtr Controls;

    // Reasons for dynamic sections rejected before manager creation;
    // empty when all startup sections were accepted.
    TString StartupRejectedSectionsReason;

    // Initialize logging, runtime, dispatcher, and controls without a manager.
    void SetUp(NUnitTest::TTestContext&) override;

    // Release the global logger before the captured output is destroyed.
    void TearDown(NUnitTest::TTestContext&) override;

    // Start one manager with empty static and dynamic configuration sources.
    void StartManager();

    // Start one manager with predefined storage and DiskAgent test settings.
    void StartManagerDefault();

    // Build startup state, initialize ICB defaults, and start one manager.
    // Call once per test; preserve the original dynamic source for comparison.
    void StartManager(
        NProto::TBlockstoreConfig staticConfig,
        NProto::TBlockstoreConfig dynamicConfig);

    // Send a dispatcher update; omit the section for a null payload.
    void SendNotification(
        std::shared_ptr<const google::protobuf::Message> config,
        ui64 cookie);

    // Wait for the dispatcher response and verify its cookie.
    void WaitForAck(ui64 cookie);

    // Read the manager's monitoring page without creating an HTTP server.
    TString GetMonitoringPage();

    // Wait for the actor logger to process the expected configuration message.
    void WaitForConfigLog(const TString& message);

    // Register the chosen subscriber and check the response to the requester.
    void Subscribe(
        NActors::TActorId requester,
        NActors::TActorId subscriber = {});

    // Verify that the manager sends a change notification to one subscriber.
    IBlockstoreConfigPtr WaitForConfigChanged(NActors::TActorId subscriber);

    // Check that a subscriber receives no change notification.
    void AssertNoConfigChanged(NActors::TActorId subscriber);

    // Remove the chosen subscriber and check the response to the requester.
    void Unsubscribe(
        NActors::TActorId requester,
        NActors::TActorId subscriber = {});
};

////////////////////////////////////////////////////////////////////////////////

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
    xxx(GetLocalNVMeConfig)                                                    \
    xxx(GetVolumeThrottlingConfig)

// A configuration wrapper that fails once while the manager reads adapter
// inputs. Publish it through the holder to exercise recovery without changing
// the actor API. All other reads delegate to the owned configuration.
class TFailingBlockstoreConfig final: public IBlockstoreConfig
{
public:
    explicit TFailingBlockstoreConfig(IBlockstoreConfigPtr config);

#define DECLARE_GETTER(name)                                                   \
    decltype(std::declval<IBlockstoreConfig>().name()) name() const override;
    FORWARDED_CONFIG_GETTERS(DECLARE_GETTER)
#undef DECLARE_GETTER

    // Fail on the first read, then allow the same update to be retried.
    const NStorage::TDiskAgentConfigPtr&
    GetDiskAgentConfig() const override;

private:
    // The retained configuration supplying every section and shared controls.
    const IBlockstoreConfigPtr Config;

    // A pending injected failure, consumed by the first DiskAgent getter call.
    mutable bool FailNextRead = true;
};

inline TFailingBlockstoreConfig::TFailingBlockstoreConfig(
    IBlockstoreConfigPtr config)
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
inline const NStorage::TDiskAgentConfigPtr&
TFailingBlockstoreConfig::GetDiskAgentConfig() const
{
    if (std::exchange(FailNextRead, false)) {
        ythrow yexception() << "test configuration preparation failure";
    }
    return Config->GetDiskAgentConfig();
}

}   // namespace NCloud::NBlockStore
