#include <cloud/blockstore/libs/config/opaque_config_parser.h>
#include <cloud/blockstore/libs/configs_manager/configs_manager.h>
#include <cloud/blockstore/libs/configs_manager/events.h>
#include <cloud/blockstore/libs/kikimr/components.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/critical_events.h>

#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/logger/stream.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

namespace NCloud::NBlockStore {

using namespace NActors;
using namespace NKikimr::NConsole;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 PrivateDatabaseConfigKind =
    NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem;

NProto::TBlockstoreConfig MakeConfig(ui32 writeBlobThreshold)
{
    NProto::TBlockstoreConfig config;
    config.MutableStorageService()->SetWriteBlobThreshold(writeBlobThreshold);
    return config;
}

// A ConfigsManager fixture with an edge actor representing ConfigsDispatcher.
// Each test receives the shared holder and ICB controls.
class TFixture: public NUnitTest::TBaseFixture
{
protected:
    // Critical-event messages captured until TearDown resets the global logger.
    TStringStream CriticalEventsLog;

    // Actor log messages captured while Runtime owns the logging backend.
    TStringStream ConfigLog;

    // The actor runtime under test.
    TTestActorRuntime Runtime;

    // The edge actor receiving subscription requests and acknowledgements.
    TActorId Dispatcher;

    // The ConfigsManager actor address registered for the current test.
    TActorId Manager;

    // The shared holder inspected after each delivered update.
    TBlockstoreConfigHolderPtr ConfigHolder;

    // The shared controls required when rebuilding storage adapters.
    NStorage::TStorageConfigControlsPtr Controls;

    // Initialize logging, source configurations, and the manager actor.
    void SetUp(NUnitTest::TTestContext&) override;

    // Release the global logger before the captured output is destroyed.
    void TearDown(NUnitTest::TTestContext&) override;

    // Send a dispatcher update; omit the section for a null payload.
    void SendNotification(
        std::shared_ptr<const google::protobuf::Message> config,
        ui64 cookie);

    // Wait for the dispatcher response and verify its cookie.
    void WaitForAck(ui64 cookie);

    // Wait for the actor logger to process the expected configuration message.
    void WaitForConfigLog(const TString& message);

    // Register the chosen subscriber and check the response to the requester.
    void Subscribe(TActorId requester, TActorId subscriber = {});

    // Verify that the manager sends a change notification to one subscriber.
    void WaitForConfigChanged(TActorId subscriber);

    // Check that a subscriber receives no change notification.
    void AssertNoConfigChanged(TActorId subscriber);

    // Remove the chosen subscriber and check the response to the requester.
    void Unsubscribe(TActorId requester, TActorId subscriber = {});
};

////////////////////////////////////////////////////////////////////////////////

// Initialize logging, source configurations, and the manager actor.
void TFixture::SetUp(NUnitTest::TTestContext&)
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
    auto staticConfig = MakeConfig(100);
    staticConfig.MutableStorageService()->SetVolumePreemptionType(
        NProto::PREEMPTION_MOVE_MOST_HEAVY);
    staticConfig.MutableStorageService()->SetNodeType("nbs");
    staticConfig.MutableStorageService()->SetSchemeShardDir("/Root/nbs");
    auto* settings =
        staticConfig.MutableStorageService()->MutableConfigDispatcherSettings();
    settings->MutableAllowList()->AddNames("PrivateDatabaseConfigItem");
    auto* label = settings->AddAdditionalNodeLabels();
    label->SetKey("zone");
    label->SetValue("static");
    auto dynamicConfig = MakeConfig(200);

    // Keep a CMS value outside PrivateDatabaseConfig to distinguish startup
    // configuration from the static configuration used for later updates.
    auto startupConfig = staticConfig;
    startupConfig.MutableStorageService()->SetVolumePreemptionType(
        NProto::PREEMPTION_MOVE_LEAST_HEAVY);
    ConfigHolder = std::make_shared<TBlockstoreConfigHolder>(
        MakeBlockstoreConfig(startupConfig, dynamicConfig, Controls));

    Manager = Runtime.Register(CreateConfigsManager({
        .ConfigHolder = ConfigHolder,
        .StaticConfig = std::move(staticConfig),
        .InitialDynamicConfig = std::move(dynamicConfig),
        .StorageConfigControls = Controls,
        .ConfigsDispatcherId = Dispatcher,
    }));

    TAutoPtr<IEventHandle> handle;
    auto* subscription = Runtime.GrabEdgeEventRethrow<
        TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest>(handle);
    UNIT_ASSERT(subscription);
    UNIT_ASSERT_VALUES_EQUAL(1, subscription->ConfigItemKinds.size());
    UNIT_ASSERT_VALUES_EQUAL(
        PrivateDatabaseConfigKind,
        subscription->ConfigItemKinds.front());
}

// Release the global logger before the captured output is destroyed.
void TFixture::TearDown(NUnitTest::TTestContext&)
{
    SetCriticalEventsLog(TLog());
}

// Send a dispatcher update; omit the section for a null payload.
void TFixture::SendNotification(
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
void TFixture::WaitForAck(ui64 cookie)
{
    TAutoPtr<IEventHandle> handle;
    UNIT_ASSERT(
        Runtime.GrabEdgeEventRethrow<TEvConsole::TEvConfigNotificationResponse>(
            handle));
    UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
}

// Wait for the actor logger to process the expected configuration message.
void TFixture::WaitForConfigLog(const TString& message)
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
void TFixture::Subscribe(TActorId requester, TActorId subscriber)
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
void TFixture::WaitForConfigChanged(TActorId subscriber)
{
    auto event =
        Runtime.GrabEdgeEventRethrow<TEvConfigsManager::TEvConfigChanged>(
            subscriber,
            TDuration::Seconds(1));
    UNIT_ASSERT(event);
    UNIT_ASSERT_VALUES_EQUAL(Manager, event->Sender);
}

// Check that a subscriber receives no change notification.
void TFixture::AssertNoConfigChanged(TActorId subscriber)
{
    UNIT_ASSERT(
        !Runtime.GrabEdgeEventRethrow<TEvConfigsManager::TEvConfigChanged>(
            subscriber,
            TDuration::MilliSeconds(10)));
}

// Remove the chosen subscriber and check the response to the requester.
void TFixture::Unsubscribe(TActorId requester, TActorId subscriber)
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigsManagerTest)
{
    // Check initial delivery and independent updates without subscriber ACKs.
    Y_UNIT_TEST_F(
        ShouldNotifySubscribersAfterPublicationWithoutWaitingForAck,
        TFixture)
    {
        // Register two consumers and consume only their initial notifications.
        const auto first = Runtime.AllocateEdgeActor();
        const auto second = Runtime.AllocateEdgeActor();
        Subscribe(first);
        Subscribe(second);
        WaitForConfigChanged(first);
        WaitForConfigChanged(second);

        // Publish and observe the new snapshot as soon as the notice arrives.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            31);
        WaitForConfigChanged(first);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        WaitForAck(31);

        // Let the second consumer lag; it must not block removal or upstream
        // ACK.
        SendNotification({}, 32);
        WaitForAck(32);
        WaitForConfigChanged(first);
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        WaitForConfigChanged(second);
        WaitForConfigChanged(second);
    }

    // Check delegated registration, refresh, and removal without duplicate
    // sends.
    Y_UNIT_TEST_F(ShouldRegisterAndRemoveExplicitSubscriber, TFixture)
    {
        // Deliver registration responses to the requester and notices to the
        // owner.
        const auto requester = Runtime.AllocateEdgeActor();
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(requester, subscriber);
        WaitForConfigChanged(subscriber);
        AssertNoConfigChanged(requester);

        // Refresh the subscription without adding a second recipient entry.
        Subscribe(requester, subscriber);
        WaitForConfigChanged(subscriber);
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            33);
        WaitForAck(33);
        WaitForConfigChanged(subscriber);
        AssertNoConfigChanged(subscriber);

        // Remove the subscription idempotently before the next publication.
        Unsubscribe(requester, subscriber);
        Unsubscribe(requester, subscriber);
        SendNotification({}, 34);
        WaitForAck(34);
        AssertNoConfigChanged(subscriber);
        AssertNoConfigChanged(requester);
    }

    // Check that re-registration after self-removal sends an initial notice.
    Y_UNIT_TEST_F(ShouldNotifyAfterResubscription, TFixture)
    {
        // Stop notifications for the sender through the default subscriber
        // field.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        Unsubscribe(subscriber);

        // Publish while unsubscribed, then notify the consumer on registration.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            35);
        WaitForAck(35);
        AssertNoConfigChanged(subscriber);
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
    }

    // Check that duplicate and rejected sources do not create publications or
    // notices.
    Y_UNIT_TEST_F(ShouldNotNotifyOnDuplicateOrRejectedConfig, TFixture)
    {
        // Subscribe to the startup snapshot before sending unchanged input.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200)),
            36);
        WaitForAck(36);
        AssertNoConfigChanged(subscriber);

        // Reject invalid YAML and preserve the last published configuration.
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        SendNotification(parser("storage_service: invalid"), 37);
        AssertNoConfigChanged(subscriber);

        // Accept a correction as the first runtime publication.
        SendNotification(
            parser("storage_service:\n  write_blob_threshold: 300"),
            38);
        WaitForAck(38);
        WaitForConfigChanged(subscriber);
    }

    // Check that an accepted update is published before it is acknowledged.
    Y_UNIT_TEST_F(
        ShouldPublishValidConfigAndAcknowledgeAfterPublication,
        TFixture)
    {
        const auto previousConfig = ConfigHolder->Get();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            17);
        WaitForAck(17);
        WaitForConfigLog("Applied PrivateDatabaseConfig");
        UNIT_ASSERT_STRING_CONTAINS(
            ConfigLog.Str(),
            "Received YAML configuration from ConfigsDispatcher");

        auto config = ConfigHolder->Get();
        UNIT_ASSERT_UNEQUAL(previousConfig.Get(), config.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            config->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Check that runtime updates preserve static-only fields and ignore changes
    // limited to those fields without publishing or notifying subscribers.
    Y_UNIT_TEST_F(ShouldIgnoreStaticOnlyOverrides, TFixture)
    {
        // Observe publications and retain the original dispatcher settings.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto initialSettings = ConfigHolder->Get()
                                         ->GetStorageConfig()
                                         ->GetConfigDispatcherSettings();

        // Mix a mutable value with overrides of every static-only storage
        // field.
        auto dynamicConfig =
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
        auto* storageConfig = dynamicConfig->MutableStorageService();
        storageConfig->SetNodeType("other");
        storageConfig->SetSchemeShardDir("/Root/other");
        auto* settings = storageConfig->MutableConfigDispatcherSettings();
        settings->MutableDenyList()->AddNames("PrivateDatabaseConfigItem");
        auto* label = settings->AddAdditionalNodeLabels();
        label->SetKey("zone");
        label->SetValue("dynamic");
        SendNotification(dynamicConfig, 60);
        WaitForAck(60);
        WaitForConfigChanged(subscriber);

        // Publish the mutable value while keeping the static node identity and
        // the complete dispatcher settings, including rules and labels.
        const auto publishedConfig = ConfigHolder->Get();
        const auto publishedStorage = publishedConfig->GetStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            publishedStorage->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL("nbs", publishedStorage->GetNodeType());
        UNIT_ASSERT_VALUES_EQUAL(
            "/Root/nbs",
            publishedStorage->GetSchemeShardDir());
        UNIT_ASSERT_VALUES_EQUAL(
            initialSettings.SerializeAsString(),
            publishedStorage->GetConfigDispatcherSettings()
                .SerializeAsString());

        // Acknowledge an update that changes only ignored fields as a
        // duplicate.
        storageConfig->SetNodeType("another");
        storageConfig->SetSchemeShardDir("/Root/another");
        settings->MutableAllowList()->AddNames("LogConfigItem");
        label->SetValue("another");
        SendNotification(dynamicConfig, 61);
        WaitForAck(61);
        UNIT_ASSERT_EQUAL(publishedConfig.Get(), ConfigHolder->Get().Get());
        AssertNoConfigChanged(subscriber);
    }

    // Check that a duplicate is acknowledged without replacing the config.
    Y_UNIT_TEST_F(ShouldAcknowledgeDuplicateWithoutRepublishing, TFixture)
    {
        const auto previousConfig = ConfigHolder->Get();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200)),
            18);
        WaitForAck(18);

        UNIT_ASSERT_EQUAL(previousConfig.Get(), ConfigHolder->Get().Get());
    }

    // Check that empty and missing configs restore static values once and then
    // receive ACKs without publishing or notifying again.
    Y_UNIT_TEST_F(ShouldTreatEmptyAndMissingConfigAsDuplicates, TFixture)
    {
        // Observe publications while replacing a non-empty startup config.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto parser = CreateBlockstoreOpaqueConfigParser();

        // Apply an empty config as a reset to static values.
        SendNotification(parser("{}"), 50);
        WaitForAck(50);
        WaitForConfigChanged(subscriber);
        const auto emptyConfig = ConfigHolder->Get();
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            emptyConfig->GetStorageConfig()->GetWriteBlobThreshold());

        // Alternate absence and empty input without replacing the snapshot.
        ui64 cookie = 51;
        for (const auto& payload:
             {std::shared_ptr<const google::protobuf::Message>{},
              parser(""),
              parser("{}")})
        {
            SendNotification(payload, cookie);
            WaitForAck(cookie++);
            UNIT_ASSERT_EQUAL(emptyConfig.Get(), ConfigHolder->Get().Get());
            AssertNoConfigChanged(subscriber);
        }
        WaitForConfigLog("Reset PrivateDatabaseConfig; restored static config");
    }

    // Check that a parser failure preserves the config and receives no ACK.
    Y_UNIT_TEST_F(ShouldRetainConfigAndHoldAckOnParserFailure, TFixture)
    {
        const auto previousConfig = ConfigHolder->Get();
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        SendNotification(parser("storage_service: invalid"), 19);
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEventRethrow<
                     TEvConsole::TEvConfigNotificationResponse>(
            handle,
            TDuration::MilliSeconds(10)));

        UNIT_ASSERT_EQUAL(previousConfig.Get(), ConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "CRITICAL_EVENT:AppCriticalEvents/GetConfigsFromCmsYamlParseError");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "Failed to parse PrivateDatabaseConfig from CMS");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "expected json map");

        // Include recovery advice for invalid configuration content.
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "Fix or roll back the cluster configuration");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "will be lost after a restart");

        // Apply a corrected config after the error notification received no
        // ACK.
        SendNotification(
            parser("storage_service:\n  write_blob_threshold: 300"),
            22);
        WaitForAck(22);

        UNIT_ASSERT_VALUES_EQUAL(
            300,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Check that removing PrivateDatabaseConfig restores the pre-CMS value,
    // excluding both startup CMS and PrivateDatabaseConfig values.
    Y_UNIT_TEST_F(ShouldTreatMissingPayloadAsRemoval, TFixture)
    {
        // Observe the startup CMS value before removing PrivateDatabaseConfig.
        UNIT_ASSERT_EQUAL(
            NProto::PREEMPTION_MOVE_LEAST_HEAVY,
            ConfigHolder->Get()->GetStorageConfig()->GetVolumePreemptionType());

        SendNotification({}, 20);
        WaitForAck(20);
        WaitForConfigLog("Reset PrivateDatabaseConfig; restored static config");

        // Restore both the overridden YAML field and the startup CMS field
        // from the static snapshot.
        auto config = ConfigHolder->Get();
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            config->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_EQUAL(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            config->GetStorageConfig()->GetVolumePreemptionType());
    }

    // Check that a wrong message type preserves the config and receives no ACK.
    Y_UNIT_TEST_F(ShouldRejectUnexpectedOpaqueMessageType, TFixture)
    {
        // Supply an unexpected type with a value that must not appear in logs.
        const auto previousConfig = ConfigHolder->Get();
        auto payload = std::make_shared<NProto::TStorageServiceConfig>();
        payload->SetNodeType("private-secret-value");
        SendNotification(payload, 21);

        // Keep the published snapshot and withhold the acknowledgement.
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEventRethrow<
                     TEvConsole::TEvConfigNotificationResponse>(
            handle,
            TDuration::MilliSeconds(10)));

        UNIT_ASSERT_EQUAL(previousConfig.Get(), ConfigHolder->Get().Get());

        // Identify the protocol violation without exposing the payload
        // contents.
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "CRITICAL_EVENT:AppCriticalEvents/GetConfigsFromCmsYamlParseError");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "unexpected PrivateDatabaseConfig payload type");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            NProto::TStorageServiceConfig::descriptor()->full_name());
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "Keeping the last successfully applied configuration");
        UNIT_ASSERT(!CriticalEventsLog.Str().Contains("private-secret-value"));
        UNIT_ASSERT(!CriticalEventsLog.Str().Contains("Fix or roll back"));
        UNIT_ASSERT(
            !CriticalEventsLog.Str().Contains("will be lost after a restart"));
    }

    // Check that a null payload reports a critical event without removal or
    // ACK.
    Y_UNIT_TEST_F(ShouldRejectNullOpaquePayload, TFixture)
    {
        // Distinguish a null payload from an absent PrivateDatabaseConfig.
        const auto previousConfig = ConfigHolder->Get();
        auto event =
            std::make_unique<TEvConsole::TEvConfigNotificationRequest>();
        event->OpaqueConfigs.emplace(PrivateDatabaseConfigKind, nullptr);
        Runtime.Send(
            new IEventHandle(Manager, Dispatcher, event.release(), 0, 23));

        // Reject the update while retaining the last published configuration.
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEventRethrow<
                     TEvConsole::TEvConfigNotificationResponse>(
            handle,
            TDuration::MilliSeconds(10)));
        UNIT_ASSERT_EQUAL(previousConfig.Get(), ConfigHolder->Get().Get());

        // Report the internal failure without suggesting a rollback.
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "CRITICAL_EVENT:AppCriticalEvents/GetConfigsFromCmsYamlParseError");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "null PrivateDatabaseConfig payload from ConfigsDispatcher");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "Keeping the last successfully applied configuration");
        UNIT_ASSERT(!CriticalEventsLog.Str().Contains("Fix or roll back"));
        UNIT_ASSERT(
            !CriticalEventsLog.Str().Contains("will be lost after a restart"));
    }
}

}   // namespace NCloud::NBlockStore
