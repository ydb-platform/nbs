#include "configs_manager_test_helpers.h"

#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/configs_manager/configs_manager.h>
#include <cloud/blockstore/libs/configs_manager/events.h>
#include <cloud/blockstore/libs/kikimr/components.h>

#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/mon/mon.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/logger/stream.h>

namespace NCloud::NBlockStore {

using namespace NActors;
using namespace NKikimr::NConsole;

namespace {

constexpr ui32 PrivateDatabaseConfigKind =
    NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem;

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NProto::TBlockstoreConfig MakeConfig(ui32 writeBlobThreshold)
{
    NProto::TBlockstoreConfig config;
    config.MutableStorageService()->SetWriteBlobThreshold(writeBlobThreshold);
    return config;
}

////////////////////////////////////////////////////////////////////////////////

// Initialize logging, the runtime, dispatcher, and shared controls.
void TConfigsManagerFixture::SetUp(NUnitTest::TTestContext&)
{
    SetCriticalEventsLog(
        TLog(MakeHolder<TStreamLogBackend>(&CriticalEventsLog)));
    Runtime.SetLogBackend(new TStreamLogBackend(&ConfigLog));
    Runtime.AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);
    Runtime.SetLogPriority(
        TBlockStoreComponents::CONFIGS_MANAGER,
        NLog::PRI_INFO);
    NKikimr::SetupTabletServices(Runtime);
    Dispatcher = Runtime.AllocateEdgeActor();

    Controls = std::make_shared<NStorage::TStorageConfigControls>();
}

// Release the global logger before the captured output is destroyed.
void TConfigsManagerFixture::TearDown(NUnitTest::TTestContext&)
{
    SetCriticalEventsLog(TLog());
}

void TConfigsManagerFixture::StartManager()
{
    StartManager(/*staticConfig=*/{}, /*dynamicConfig=*/{});
}

void TConfigsManagerFixture::StartManagerDefault()
{
    auto staticConfig = MakeConfig(100);
    staticConfig.MutableStorageService()->SetListVolumesConcurrency(10);
    staticConfig.MutableDiskAgent()->SetDedicatedDiskAgent(true);
    staticConfig.MutableDiskAgent()->SetEnabled(false);
    staticConfig.MutableStorageService()->SetNodeType("nbs");
    staticConfig.MutableStorageService()->SetSchemeShardDir("/Root/nbs");
    auto* settings =
        staticConfig.MutableStorageService()->MutableConfigDispatcherSettings();
    settings->MutableAllowList()->AddNames("PrivateDatabaseConfigItem");
    auto* label = settings->AddAdditionalNodeLabels();
    label->SetKey("zone");
    label->SetValue("static");
    StartManager(
        std::move(staticConfig),
        /*dynamicConfig=*/MakeConfig(200));
}

void TConfigsManagerFixture::StartManager(
    NProto::TBlockstoreConfig staticConfig,
    NProto::TBlockstoreConfig dynamicConfig)
{
    UNIT_ASSERT(!Manager);

    // Build the startup snapshot without changing the received dynamic source.
    auto startupConfig = MakeStartupBlockstoreConfig(
        staticConfig,
        dynamicConfig,
        Controls,
        /*extraParameters=*/{});
    ConfigHolder = std::make_shared<TBlockstoreConfigHolder>(
        std::move(startupConfig.Config));

    // Initialize shared controls before the manager can process notifications.
    Controls->UpdateDefaults(startupConfig.Proto.GetStorageService());

    // Register one manager and verify its subscription before sending updates.
    Manager = Runtime.Register(CreateConfigsManager({
        .ConfigHolder = ConfigHolder,
        .StaticConfig = std::move(staticConfig),
        .StartupConfig = std::move(startupConfig.Proto),
        .InitialDynamicConfig = std::move(dynamicConfig),
        .StartupDynamicConfigNormalizationDiagnostics =
            std::move(startupConfig.NormalizationDiagnostics),
        .StorageConfigControls = Controls,
        .ConfigsDispatcherId = Dispatcher,
    }));

    TAutoPtr<IEventHandle> handle;
    auto* subscription = Runtime.GrabEdgeEventRethrow<
        TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest>(handle);
    UNIT_ASSERT(subscription);
    UNIT_ASSERT_VALUES_EQUAL(Manager, handle->Sender);
    UNIT_ASSERT_VALUES_EQUAL(1, subscription->ConfigItemKinds.size());
    UNIT_ASSERT_VALUES_EQUAL(
        PrivateDatabaseConfigKind,
        subscription->ConfigItemKinds.front());
}

// Send a dispatcher update; omit the section for a null payload.
void TConfigsManagerFixture::SendNotification(
    std::shared_ptr<const google::protobuf::Message> config,
    ui64 cookie)
{
    auto event = std::make_unique<TEvConsole::TEvConfigNotificationRequest>();
    if (config) {
        event->OpaqueConfigs.emplace(
            PrivateDatabaseConfigKind,
            std::move(config));
    }

    Runtime.Send(
        new IEventHandle(Manager, Dispatcher, event.release(), 0, cookie));
}

// Wait for the dispatcher response and verify its cookie.
void TConfigsManagerFixture::WaitForAck(ui64 cookie)
{
    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(
        Runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationResponse>(
            handle));
    UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
}

// Wait for the actor logger to process the expected configuration message.
void TConfigsManagerFixture::WaitForConfigLog(const TString& message)
{
    TDispatchOptions options;
    options.CustomFinalCondition = [&]
    {
        return ConfigLog.Str().Contains(message);
    };
    Runtime.DispatchEvents(options, TDuration::Seconds(1));
    UNIT_ASSERT_STRING_CONTAINS(ConfigLog.Str(), message);
    UNIT_ASSERT_STRING_CONTAINS(ConfigLog.Str(), "BLOCKSTORE_CONFIGS_MANAGER");
}

// Register the chosen subscriber and check the response to the requester.
void TConfigsManagerFixture::Subscribe(
    TActorId requester,
    TActorId subscriber)
{
    Runtime.Send(new IEventHandle(
        Manager,
        requester,
        new TEvConfigsManager::TEvSetConfigSubscriptionRequest(subscriber),
        0,
        41));
    auto response = Runtime.GrabEdgeEventRethrow<
        TEvConfigsManager::TEvSetConfigSubscriptionResponse>(
        requester,
        TDuration::Seconds(1));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(41, response->Cookie);
}

// Verify that the manager sends a change notification to one subscriber.
IBlockstoreConfigPtr TConfigsManagerFixture::WaitForConfigChanged(
    TActorId subscriber)
{
    auto event =
        Runtime.GrabEdgeEventRethrow<TEvConfigsManager::TEvConfigChanged>(
            subscriber,
            TDuration::Seconds(1));
    UNIT_ASSERT(event);
    UNIT_ASSERT_VALUES_EQUAL(Manager, event->Sender);
    const auto& config = event->Get()->Config;
    UNIT_ASSERT(config);
    return config;
}

// Check that a subscriber receives no change notification.
void TConfigsManagerFixture::AssertNoConfigChanged(TActorId subscriber)
{
    UNIT_ASSERT(
        !Runtime.GrabEdgeEventRethrow<TEvConfigsManager::TEvConfigChanged>(
            subscriber,
            TDuration::MilliSeconds(10)));
}

// Remove the chosen subscriber and check the response to the requester.
void TConfigsManagerFixture::Unsubscribe(
    TActorId requester,
    TActorId subscriber)
{
    Runtime.Send(new IEventHandle(
        Manager,
        requester,
        new TEvConfigsManager::TEvRemoveConfigSubscriptionRequest(subscriber),
        0,
        42));
    auto response = Runtime.GrabEdgeEventRethrow<
        TEvConfigsManager::TEvRemoveConfigSubscriptionResponse>(
        requester,
        TDuration::Seconds(1));
    UNIT_ASSERT(response);
    UNIT_ASSERT_VALUES_EQUAL(42, response->Cookie);
}

// Read the page through the actor without creating an HTTP server.
TString TConfigsManagerFixture::GetMonitoringPage()
{
    TStringStream output;
    NMonitoring::TMonService2HttpRequest
        request(&output, nullptr, nullptr, nullptr, {}, nullptr);
    const auto edge = Runtime.AllocateEdgeActor();
    Runtime.Send(
        new IEventHandle(Manager, edge, new NMon::TEvHttpInfo(request)));
    const auto response = Runtime.GrabEdgeEventRethrow<NMon::TEvHttpInfoRes>(
        edge,
        TDuration::Seconds(1));
    UNIT_ASSERT(response);
    response->Get()->Output(output);
    return output.Str();
}

}   // namespace NCloud::NBlockStore
