#include "configs_manager_test_helpers.h"

#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/config/opaque_config_parser.h>
#include <cloud/blockstore/libs/configs_manager/events.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/base/appdata.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/control/immediate_control_board_impl.h>

#include <library/cpp/testing/unittest/registar.h>

#include <utility>

namespace NCloud::NBlockStore {

using namespace NActors;
using namespace NKikimr::NConsole;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 PrivateDatabaseConfigKind =
    NKikimrConsole::TConfigItem::PrivateDatabaseConfigItem;

// A runtime-owned subscriber that discards notices and stops on PoisonPill
// without unsubscribing. Register it to exercise failed notification delivery.
class TSubscriberActor final: public TActor<TSubscriberActor>
{
public:
    TSubscriberActor();

private:
    // Consume notifications and stop without removing the subscription.
    STFUNC(StateWork);
};

TSubscriberActor::TSubscriberActor()
    : TActor(&TThis::StateWork)
{}

// Consume notifications and stop without removing the subscription.
STFUNC(TSubscriberActor::StateWork)
{
    switch (ev->GetTypeRewrite()) {
        IgnoreFunc(TEvConfigsManager::TEvConfigChanged);
        cFunc(TEvents::TSystem::PoisonPill, PassAway);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigsManagerTest)
{
    // Check startup and runtime enum values, explicit zero, and static fallback
    // after replacing or removing the complete private configuration.
    Y_UNIT_TEST_F(
        ShouldApplyEnumValuesFromPrivateConfig,
        TConfigsManagerFixture)
    {
        // Build the startup snapshot from a static enum and parsed private YAML.
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableStorageService()->SetVolumePreemptionType(
            NProto::PREEMPTION_MOVE_MOST_HEAVY);
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        NProto::TBlockstoreConfig dynamicConfig;
        dynamicConfig.CopyFrom(*parser(
            "storage_service: "
            "{volume_preemption_type: PREEMPTION_MOVE_LEAST_HEAVY}"));
        StartManager(std::move(staticConfig), std::move(dynamicConfig));

        // Observe the accepted startup enum through the published snapshot.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_EQUAL(
            NProto::PREEMPTION_MOVE_LEAST_HEAVY,
            ConfigHolder->Get()->GetStorageConfig()->GetVolumePreemptionType());

        // Apply explicit zero, restore an omitted field, and set the enum again.
        const std::pair<TString, NProto::EVolumePreemptionType> updates[] = {
            {"storage_service: {volume_preemption_type: PREEMPTION_NONE}",
             NProto::PREEMPTION_NONE},
            {"storage_service: {write_blob_threshold: 300}",
             NProto::PREEMPTION_MOVE_MOST_HEAVY},
            {"storage_service: "
             "{volume_preemption_type: PREEMPTION_MOVE_LEAST_HEAVY}",
             NProto::PREEMPTION_MOVE_LEAST_HEAVY},
        };
        ui64 cookie = 130;
        for (const auto& [yaml, expected]: updates) {
            SendNotification(parser(yaml), cookie);
            WaitForAck(cookie++);
            WaitForConfigChanged(subscriber);
            UNIT_ASSERT_EQUAL(
                expected,
                ConfigHolder->Get()
                    ->GetStorageConfig()
                    ->GetVolumePreemptionType());
        }

        // Restore the static enum when the dispatcher removes the source.
        SendNotification({}, cookie);
        WaitForAck(cookie);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_EQUAL(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            ConfigHolder->Get()->GetStorageConfig()->GetVolumePreemptionType());
    }

    // Verify that runtime changes and removal preserve the startup RO value
    // while updating RW values and acknowledging the complete source.
    Y_UNIT_TEST_F(ShouldPreserveStartupOnlyParameters, TConfigsManagerFixture)
    {
        // Apply the startup RO value through the parsed private source.
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetListVolumesConcurrency(10);
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        NProto::TBlockstoreConfig privateConfig;
        privateConfig.CopyFrom(*parser(
            "storage_service: "
            "{write_blob_threshold: 200, list_volumes_concurrency: 20}"));
        StartManager(std::move(staticConfig), std::move(privateConfig));

        // Retain the accepted startup RO value before a mixed update.
        const auto initialConfig = ConfigHolder->Get();
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            initialConfig->GetStorageConfig()->GetListVolumesConcurrency());

        // Accept the RW change while keeping the startup RO value.
        auto dynamicConfig =
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
        dynamicConfig->MutableStorageService()->SetListVolumesConcurrency(30);
        SendNotification(dynamicConfig, 101);
        WaitForAck(101);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            ConfigHolder->Get()
                ->GetStorageConfig()
                ->GetListVolumesConcurrency());
        UNIT_ASSERT(CriticalEventsLog.Str().empty());

        // Remove the source and restore only the RW static fallback.
        SendNotification(nullptr, 102);
        WaitForAck(102);
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            ConfigHolder->Get()
                ->GetStorageConfig()
                ->GetListVolumesConcurrency());
    }

    // Check that publication resets only changed ICB defaults and that removal
    // restores the static base without changing retained snapshot protos.
    Y_UNIT_TEST_F(ShouldUpdateOnlyChangedIcbDefaults, TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/MakeConfig(100),
            /*dynamicConfig=*/MakeConfig(200));

        // Retain the startup snapshot and install an operator override.
        auto& board = *Runtime.GetAppData().Icb;
        Controls->Register(board);
        NKikimr::TControlWrapper control;
        UNIT_ASSERT(!board.RegisterSharedControl(
            control,
            "BlockStore_WriteBlobThreshold"));
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        const auto initialConfig = WaitForConfigChanged(subscriber);
        TAtomic previousValue = {};
        board.SetValue("BlockStore_WriteBlobThreshold", 400, previousValue);

        // Replace the configured value and expose the new native default.
        auto dynamicConfig = MakeConfig(300);
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(dynamicConfig),
            70);
        WaitForAck(70);
        const auto updatedConfig = WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(300, control.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(300, static_cast<i64>(control));
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            updatedConfig->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            initialConfig->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(200, initialConfig->GetStorageServiceConfig()->GetWriteBlobThreshold());

        // Publish another field without overwriting the unchanged field's ICB.
        board.SetValue("BlockStore_WriteBlobThreshold", 500, previousValue);
        dynamicConfig.MutableStorageService()->SetMaxMigrationIoDepth(7);
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(dynamicConfig),
            71);
        WaitForAck(71);
        const auto publishedConfig = WaitForConfigChanged(subscriber);
        UNIT_ASSERT_UNEQUAL(updatedConfig.Get(), publishedConfig.Get());
        UNIT_ASSERT_VALUES_EQUAL(300, control.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(500, static_cast<i64>(control));
        UNIT_ASSERT_VALUES_EQUAL(
            500,
            publishedConfig->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            500,
            initialConfig->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            initialConfig->GetStorageServiceConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            "300",
            publishedConfig->GetStorageConfig()
                ->GetValueByName("WriteBlobThreshold")
                .Value);

        // Preserve the override and snapshot for duplicate and rejected input.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(dynamicConfig),
            72);
        WaitForAck(72);
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        SendNotification(parser("storage_service: invalid"), 73);
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEventRethrow<
                     TEvConsole::TEvConfigNotificationResponse>(
            handle,
            TDuration::MilliSeconds(10)));
        UNIT_ASSERT_EQUAL(publishedConfig.Get(), ConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(300, control.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(500, static_cast<i64>(control));

        // Remove the dynamic source and reset only the changed baselines.
        SendNotification({}, 74);
        WaitForAck(74);
        const auto resetConfig = WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(100, control.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(100, static_cast<i64>(control));
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            resetConfig->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            publishedConfig->GetStorageServiceConfig()
                ->GetWriteBlobThreshold());
    }

    // Check initial delivery and independent updates without subscriber ACKs.
    Y_UNIT_TEST_F(
        ShouldNotifySubscribersAfterPublicationWithoutWaitingForAck,
        TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/MakeConfig(100),
            /*dynamicConfig=*/MakeConfig(200));

        // Register two consumers and leave the second initial notification
        // queued.
        const auto first = Runtime.AllocateEdgeActor();
        const auto second = Runtime.AllocateEdgeActor();
        Subscribe(first);
        Subscribe(second);
        const auto initial = WaitForConfigChanged(first);
        UNIT_ASSERT_EQUAL(initial.Get(), ConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            initial->GetStorageConfig()->GetWriteBlobThreshold());

        // Publish and observe the new snapshot as soon as the notice arrives.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            31);
        const auto updated = WaitForConfigChanged(first);
        UNIT_ASSERT_EQUAL(updated.Get(), ConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            updated->GetStorageConfig()->GetWriteBlobThreshold());
        WaitForAck(31);

        // Let the second consumer lag; it must not block removal or upstream
        // ACK.
        SendNotification({}, 32);
        WaitForAck(32);
        const auto reset = WaitForConfigChanged(first);
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            reset->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_EQUAL(reset.Get(), ConfigHolder->Get().Get());
        const auto delayedInitial = WaitForConfigChanged(second);
        UNIT_ASSERT_EQUAL(initial.Get(), delayedInitial.Get());
        const auto delayedUpdate = WaitForConfigChanged(second);
        UNIT_ASSERT_EQUAL(updated.Get(), delayedUpdate.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            delayedUpdate->GetStorageServiceConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_EQUAL(delayedUpdate->GetStorageConfig(),
                          reset->GetStorageConfig());
        UNIT_ASSERT_VALUES_EQUAL(
            100, delayedUpdate->GetStorageConfig()->GetWriteBlobThreshold());
        const auto delayedReset = WaitForConfigChanged(second);
        UNIT_ASSERT_EQUAL(reset.Get(), delayedReset.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            initial->GetStorageServiceConfig()->GetWriteBlobThreshold());
    }

    // Keep both published sections from A while the startup pointer follows B
    // and ICB.
    Y_UNIT_TEST_F(ShouldKeepStorageAndFeaturesOfDelayedPublication,
                  TConfigsManagerFixture)
    {
        // Start with an absent threshold and a feature addressed to the first
        // disk.
        NProto::TBlockstoreConfig startup;
        auto* feature = startup.MutableFeatures()->AddFeatures();
        feature->SetName("UseRdma");
        feature->MutableWhitelist()->AddEntityIds("first");
        StartManager({}, startup);
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        const auto first = WaitForConfigChanged(subscriber);
        const auto storage = first->GetStorageConfig();
        UNIT_ASSERT(!first->GetStorageServiceConfig()->HasWriteBlobThreshold());
        UNIT_ASSERT(storage->IsUseRdmaFeatureEnabled({}, {}, "first"));

        // Publish an explicit zero and replace the scope of the retained
        // feature.
        auto next = std::make_shared<NProto::TBlockstoreConfig>(startup);
        next->MutableStorageService()->SetWriteBlobThreshold(0);
        next->MutableFeatures()
            ->MutableFeatures(0)
            ->MutableWhitelist()
            ->SetEntityIds(0, "second");
        SendNotification(next, 110);
        WaitForAck(110);
        const auto second = WaitForConfigChanged(subscriber);
        UNIT_ASSERT_EQUAL(storage, second->GetStorageConfig());
        UNIT_ASSERT(!first->GetStorageServiceConfig()->HasWriteBlobThreshold());
        UNIT_ASSERT(second->GetStorageServiceConfig()->HasWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(0, storage->GetWriteBlobThreshold());
        UNIT_ASSERT(first->GetFeaturesConfig()->IsFeatureEnabled({}, {},
                                                                 "first",
                                                                 "UseRdma"));
        UNIT_ASSERT(!storage->IsUseRdmaFeatureEnabled({}, {}, "first"));
        UNIT_ASSERT(storage->IsUseRdmaFeatureEnabled({}, {}, "second"));

        // Operator changes affect the retained working pointer and preserve
        // both raw sections.
        auto& board = *Runtime.GetAppData().Icb;
        Controls->Register(board);
        TAtomic previous = 0;
        board.SetValue("BlockStore_WriteBlobThreshold", 7, previous);
        UNIT_ASSERT_VALUES_EQUAL(7, storage->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            0, second->GetStorageServiceConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT(!first->GetStorageServiceConfig()->HasWriteBlobThreshold());
        board.RestoreDefault("BlockStore_WriteBlobThreshold");
        UNIT_ASSERT_VALUES_EQUAL(0, storage->GetWriteBlobThreshold());
    }

    // Check delegated registration, refresh, and removal without duplicate
    // sends.
    Y_UNIT_TEST_F(
        ShouldRegisterAndRemoveExplicitSubscriber,
        TConfigsManagerFixture)
    {
        StartManager();

        // Deliver registration responses to the requester and notices to the
        // owner.
        const auto requester = Runtime.AllocateEdgeActor();
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(requester, subscriber);
        const auto initial = WaitForConfigChanged(subscriber);
        UNIT_ASSERT_EQUAL(ConfigHolder->Get().Get(), initial.Get());
        AssertNoConfigChanged(requester);

        // Refresh the subscription without adding a second recipient entry.
        Subscribe(requester, subscriber);
        const auto refreshed = WaitForConfigChanged(subscriber);
        UNIT_ASSERT_EQUAL(ConfigHolder->Get().Get(), refreshed.Get());
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

    // Check that failed update and initial deliveries remove a dead subscriber
    // while publications continue for live subscribers with upstream ACKs.
    Y_UNIT_TEST_F(ShouldRemoveUndeliveredSubscribers, TConfigsManagerFixture)
    {
        StartManager();

        // Observe delivery attempts to an independently stopped subscriber.
        const auto live = Runtime.AllocateEdgeActor();
        const auto subscriber = Runtime.Register(new TSubscriberActor());
        ui32 notifications = 0;
        ui32 undelivered = 0;
        auto observer = Runtime.AddObserver(
            [&](TAutoPtr<IEventHandle>& ev)
            {
                if (ev->GetTypeRewrite() ==
                        TEvConfigsManager::TEvConfigChanged::EventType &&
                    ev->Recipient == subscriber)
                {
                    ++notifications;
                }
                if (ev->GetTypeRewrite() ==
                        TEvents::TEvUndelivered::EventType &&
                    ev->Recipient == Manager && ev->Sender == subscriber)
                {
                    ++undelivered;
                }
            });

        // Wait until the manager has processed the runtime's delivery failure.
        const auto waitForUndelivered = [&](ui32 count)
        {
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return undelivered == count;
            };
            Runtime.DispatchEvents(options, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(count, undelivered);
        };

        // Establish both subscriptions before stopping one without unsubscribe.
        Subscribe(live);
        WaitForConfigChanged(live);
        Subscribe(live, subscriber);
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return notifications == 1;
        };
        Runtime.DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(1, notifications);
        Runtime.Send(
            new IEventHandle(subscriber, live, new TEvents::TEvPoisonPill()));
        UNIT_ASSERT(!Runtime.FindActor(subscriber));

        // Remove the dead recipient without blocking updates to the live one.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            70);
        WaitForAck(70);
        WaitForConfigChanged(live);
        waitForUndelivered(1);
        UNIT_ASSERT_VALUES_EQUAL(2, notifications);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());

        // Remove a delegated subscription after its initial delivery fails.
        Subscribe(live, subscriber);
        waitForUndelivered(2);
        UNIT_ASSERT_VALUES_EQUAL(3, notifications);

        // Publish again without retrying the dead recipient or reporting
        // errors.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(400)),
            71);
        WaitForAck(71);
        WaitForConfigChanged(live);
        AssertNoConfigChanged(live);
        UNIT_ASSERT_VALUES_EQUAL(3, notifications);
        UNIT_ASSERT_VALUES_EQUAL(2, undelivered);
        UNIT_ASSERT_VALUES_EQUAL(
            400,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT(CriticalEventsLog.Str().empty());
    }

    // Check that re-registration after self-removal sends an initial notice.
    Y_UNIT_TEST_F(ShouldNotifyAfterResubscription, TConfigsManagerFixture)
    {
        StartManager();

        // Stop notifications for the sender through the default subscriber
        // field.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        const auto initial = WaitForConfigChanged(subscriber);
        UNIT_ASSERT_EQUAL(ConfigHolder->Get().Get(), initial.Get());
        Unsubscribe(subscriber);

        // Publish while unsubscribed, then notify the consumer on registration.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            35);
        WaitForAck(35);
        AssertNoConfigChanged(subscriber);
        Subscribe(subscriber);
        const auto resubscribed = WaitForConfigChanged(subscriber);
        UNIT_ASSERT_EQUAL(ConfigHolder->Get().Get(), resubscribed.Get());
    }

    // Check that duplicate and rejected sources do not create publications or
    // notices.
    Y_UNIT_TEST_F(
        ShouldNotNotifyOnDuplicateOrRejectedConfig,
        TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/{},
            /*dynamicConfig=*/MakeConfig(200));

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

    // Verify that runtime updates log only ignored static-only paths without
    // values or deferred changes. Do not repeat startup warnings in this actor.
    Y_UNIT_TEST_F(ShouldLogStaticOnlyDynamicFields, TConfigsManagerFixture)
    {
        // Start with an ignored field whose warning belongs to bootstrap.
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableStorageService()->SetNodeType("static-node");
        staticConfig.MutableStorageService()->SetListVolumesConcurrency(10);
        NProto::TBlockstoreConfig dynamicConfig;
        dynamicConfig.MutableStorageService()->SetNodeType("startup-secret");
        dynamicConfig.MutableStorageService()->SetListVolumesConcurrency(20);
        StartManager(staticConfig, dynamicConfig);

        // Report a different path from an accepted runtime source, retaining
        // the static value and keeping source values and deferred paths out
        // of the warnings.
        dynamicConfig.MutableStorageService()->ClearNodeType();
        dynamicConfig.MutableStorageService()->SetListVolumesConcurrency(30);
        dynamicConfig.MutableStorageService()->SetSchemeShardDir(
            "runtime-secret");
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(dynamicConfig),
            170);
        WaitForAck(170);
        WaitForConfigLog(
            "Ignoring static-only field from dynamic PrivateDatabaseConfig: "
            "StorageService.SchemeShardDir");
        UNIT_ASSERT(!ConfigLog.Str().Contains(
            "Ignoring static-only field from dynamic PrivateDatabaseConfig: "
            "StorageService.NodeType"));
        UNIT_ASSERT(!ConfigLog.Str().Contains("startup-secret"));
        UNIT_ASSERT(!ConfigLog.Str().Contains("runtime-secret"));
        UNIT_ASSERT(!ConfigLog.Str().Contains(
            "Ignoring static-only field from dynamic PrivateDatabaseConfig: "
            "StorageService.ListVolumesConcurrency"));
        UNIT_ASSERT(CriticalEventsLog.Str().empty());
        UNIT_ASSERT_VALUES_EQUAL(
            "static-node",
            ConfigHolder->Get()->GetStorageConfig()->GetNodeType());
        UNIT_ASSERT_VALUES_EQUAL(
            20,
            ConfigHolder->Get()
                ->GetStorageConfig()
                ->GetListVolumesConcurrency());
    }

    // Check that an accepted update is published before it is acknowledged.
    Y_UNIT_TEST_F(
        ShouldPublishValidConfigAndAcknowledgeAfterPublication,
        TConfigsManagerFixture)
    {
        StartManager();

        const auto previousConfig = ConfigHolder->Get();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            17);
        WaitForAck(17);
        WaitForConfigLog("Accepted PrivateDatabaseConfig");
        UNIT_ASSERT_STRING_CONTAINS(
            ConfigLog.Str(),
            "Received YAML configuration from ConfigsDispatcher");

        auto config = ConfigHolder->Get();
        UNIT_ASSERT_UNEQUAL(previousConfig.Get(), config.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            config->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Check that a large double from YAML is published and acknowledged with
    // a saturated ICB default while the storage getter retains the raw value.
    Y_UNIT_TEST_F(
        ShouldPublishDoubleWithSaturatedIcbDefault,
        TConfigsManagerFixture)
    {
        StartManager();

        // Observe the live control and notifications before applying YAML.
        auto& board = *Runtime.GetAppData().Icb;
        Controls->Register(board);
        NKikimr::TControlWrapper control;
        UNIT_ASSERT(!board.RegisterSharedControl(
            control,
            "BlockStore_DiskRegistryInitialAgentRejectionThreshold"));
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto previousConfig = ConfigHolder->Get();

        // Accept a representable protobuf double beyond the integer ICB range.
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        SendNotification(
            parser(R"(
storage_service:
  disk_registry_initial_agent_rejection_threshold: 1.0e100
)"),
            75);
        WaitForAck(75);
        WaitForConfigChanged(subscriber);

        // Publish the original double with the largest native integer default.
        const auto config = ConfigHolder->Get();
        UNIT_ASSERT_UNEQUAL(previousConfig.Get(), config.Get());
        UNIT_ASSERT_VALUES_EQUAL(Max<TAtomicBase>(), control.GetDefault());
        UNIT_ASSERT_VALUES_EQUAL(
            Max<TAtomicBase>(),
            static_cast<TAtomicBase>(control));
        UNIT_ASSERT_VALUES_EQUAL(
            1e100,
            config->GetStorageConfig()
                ->GetDiskRegistryInitialAgentRejectionThreshold());
    }

    // Check that a changed configuration from ConfigsDispatcher causes a new
    // publication while static-only fields keep their startup values.
    // Repeated delivery of the same input must not cause another publication.
    Y_UNIT_TEST_F(
        ShouldPublishUpdatesWhilePreservingStaticOnlyFields,
        TConfigsManagerFixture)
    {
        StartManagerDefault();

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

        // Accept a changed source even when only ignored fields differ.
        storageConfig->SetNodeType("another");
        storageConfig->SetSchemeShardDir("/Root/another");
        settings->MutableAllowList()->AddNames("LogConfigItem");
        label->SetValue("another");
        SendNotification(dynamicConfig, 61);
        WaitForAck(61);
        UNIT_ASSERT_UNEQUAL(publishedConfig.Get(), ConfigHolder->Get().Get());
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(
            "nbs",
            ConfigHolder->Get()->GetStorageConfig()->GetNodeType());
        UNIT_ASSERT_VALUES_EQUAL(
            "/Root/nbs",
            ConfigHolder->Get()->GetStorageConfig()->GetSchemeShardDir());

        // Remember the original source, including all ignored fields.
        const auto updatedConfig = ConfigHolder->Get();
        SendNotification(dynamicConfig, 62);
        WaitForAck(62);
        UNIT_ASSERT_EQUAL(updatedConfig.Get(), ConfigHolder->Get().Get());
        AssertNoConfigChanged(subscriber);
    }

    // Check that a changed configuration from ConfigsDispatcher causes a new
    // publication even when the effective configuration remains unchanged.
    // Removing a dynamic override must restore the corresponding static value.
    Y_UNIT_TEST_F(
        ShouldPublishUpdatesEvenWhenEffectiveConfigIsUnchanged,
        TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/MakeConfig(100),
            /*dynamicConfig=*/MakeConfig(200));

        // Observe the snapshot built from the initial private configuration.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto startupConfig = ConfigHolder->Get();
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        ui64 cookie = 70;

        // Publish each change to ignored Server fields or section presence.
        for (const TString yaml:
             {"storage_service: {write_blob_threshold: 200}\n"
              "server: {server_config: "
              "{dynamic_yaml_configuration_enabled: false}}",
              "storage_service: {write_blob_threshold: 200}",
              "storage_service: {write_blob_threshold: 200}\n"
              "server: {server_config: {}}",
              "storage_service: {write_blob_threshold: 200}\n"
              "server: {}",
              "storage_service: {write_blob_threshold: 200}"})
        {
            const auto previousConfig = ConfigHolder->Get();
            SendNotification(parser(yaml), cookie);
            WaitForAck(cookie++);
            WaitForConfigChanged(subscriber);
            UNIT_ASSERT_VALUES_EQUAL(
                200,
                ConfigHolder->Get()
                    ->GetStorageConfig()
                    ->GetWriteBlobThreshold());
            UNIT_ASSERT_UNEQUAL(
                previousConfig.Get(),
                ConfigHolder->Get().Get());
        }

        // Publish a real removal even when ignored fields remain in its source.
        SendNotification(parser("storage_service: {node_type: other}"), cookie);
        WaitForAck(cookie++);
        WaitForConfigChanged(subscriber);
        const auto emptyConfig = ConfigHolder->Get();
        UNIT_ASSERT_UNEQUAL(startupConfig.Get(), emptyConfig.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            emptyConfig->GetStorageConfig()->GetWriteBlobThreshold());

        // Distinguish absent, empty and static-only sections in the source.
        for (const TString yaml:
             {"{}",
              "storage_service: {node_type: other, "
              "scheme_shard_dir: /Root/other, "
              "config_dispatcher_settings: {}}",
              "{}",
              "storage_service: {}",
              "{}",
              "server: {server_config: "
              "{dynamic_yaml_configuration_enabled: true}}",
              "{}"})
        {
            const auto previousConfig = ConfigHolder->Get();
            SendNotification(parser(yaml), cookie);
            WaitForAck(cookie++);
            WaitForConfigChanged(subscriber);
            UNIT_ASSERT_UNEQUAL(
                previousConfig.Get(),
                ConfigHolder->Get().Get());
            UNIT_ASSERT_VALUES_EQUAL(
                100,
                ConfigHolder->Get()
                    ->GetStorageConfig()
                    ->GetWriteBlobThreshold());

            // Suppress a repeat of exactly the same received proto.
            const auto publishedConfig = ConfigHolder->Get();
            SendNotification(parser(yaml), cookie);
            WaitForAck(cookie++);
            UNIT_ASSERT_EQUAL(publishedConfig.Get(), ConfigHolder->Get().Get());
            AssertNoConfigChanged(subscriber);
        }
    }

    // Check that a changed configuration from ConfigsDispatcher causes a new
    // publication while DiskAgent.Enabled and DiskAgent.DedicatedDiskAgent
    // keep their startup values. Allowed runtime changes must still take
    // effect.
    Y_UNIT_TEST_F(
        ShouldPublishUpdatesWhilePreservingStartupOnlyDiskAgentSettings,
        TConfigsManagerFixture)
    {
        StartManagerDefault();

        // Retain the startup snapshot and observe subsequent publications.
        const auto startupConfig = ConfigHolder->Get();
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto parser = CreateBlockstoreOpaqueConfigParser();

        // Try to enable the embedded agent and override its static role.
        SendNotification(
            parser(
                "storage_service: {write_blob_threshold: 200}\n"
                "disk_agent: {enabled: true, dedicated_disk_agent: false}"),
            80);
        WaitForAck(80);

        // Publish with the forbidden overrides removed.
        UNIT_ASSERT(!ConfigHolder->Get()->GetDiskAgentConfig()->GetEnabled());
        UNIT_ASSERT(
            ConfigHolder->Get()->GetDiskAgentConfig()->GetDedicatedDiskAgent());
        UNIT_ASSERT_UNEQUAL(startupConfig.Get(), ConfigHolder->Get().Get());
        WaitForConfigChanged(subscriber);

        // Apply an independent setting without enabling the embedded agent.
        SendNotification(
            parser(
                "storage_service: {write_blob_threshold: 300}\n"
                "disk_agent: {enabled: true, dedicated_disk_agent: false}"),
            81);
        WaitForAck(81);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT(!ConfigHolder->Get()->GetDiskAgentConfig()->GetEnabled());
        UNIT_ASSERT(
            ConfigHolder->Get()->GetDiskAgentConfig()->GetDedicatedDiskAgent());
    }

    // Verify that runtime updates preserve startup discovery ports while
    // changed sources publish even when normalization makes them equivalent.
    Y_UNIT_TEST_F(
        ShouldPreserveStartupDiscoveryPortsAcrossUpdates,
        TConfigsManagerFixture)
    {
        StartManager();

        // Retain the static ports and subscribe to configuration publications.
        const auto startupConfig = ConfigHolder->Get();
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto parser = CreateBlockstoreOpaqueConfigParser();

        // Normalize the requested ports, then preserve their startup values.
        SendNotification(
            parser(
                "server: {server_config: {port: 12000, secure_port: 12001}}"),
            90);
        WaitForAck(90);
        WaitForConfigChanged(subscriber);
        const auto publishedConfig = ConfigHolder->Get();
        UNIT_ASSERT_VALUES_EQUAL(
            startupConfig->GetDiscoveryServiceConfig()
                ->GetConductorInstancePort(),
            publishedConfig->GetDiscoveryServiceConfig()
                ->GetConductorInstancePort());
        UNIT_ASSERT_VALUES_EQUAL(
            startupConfig->GetDiscoveryServiceConfig()
                ->GetConductorSecureInstancePort(),
            publishedConfig->GetDiscoveryServiceConfig()
                ->GetConductorSecureInstancePort());

        // Accept explicitly specified ports as a change to the received source.
        SendNotification(
            parser(
                "server: {server_config: {port: 12000, secure_port: 12001}}\n"
                "discovery_service: {conductor_instance_port: 12000, "
                "conductor_secure_instance_port: 12001}"),
            91);
        WaitForAck(91);
        UNIT_ASSERT_UNEQUAL(publishedConfig.Get(), ConfigHolder->Get().Get());
        WaitForConfigChanged(subscriber);

        // Remove both supplied and derived overrides by removing the source.
        SendNotification(parser("{}"), 92);
        WaitForAck(92);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(
            startupConfig->GetServerConfig()->GetPort(),
            ConfigHolder->Get()->GetServerConfig()->GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            startupConfig->GetDiscoveryServiceConfig()
                ->GetConductorInstancePort(),
            ConfigHolder->Get()
                ->GetDiscoveryServiceConfig()
                ->GetConductorInstancePort());
        UNIT_ASSERT_VALUES_EQUAL(
            startupConfig->GetDiscoveryServiceConfig()
                ->GetConductorSecureInstancePort(),
            ConfigHolder->Get()
                ->GetDiscoveryServiceConfig()
                ->GetConductorSecureInstancePort());
    }

    // Check that receiving the original startup CMS configuration again does
    // not cause a new publication. Removing fields ignored by normalization
    // from the next input must cause a publication despite unchanged effective
    // values.
    Y_UNIT_TEST_F(
        ShouldNotRepublishUnchangedStartupCmsConfig,
        TConfigsManagerFixture)
    {
        // Start a manager with ignored fields present in its received source.
        NProto::TBlockstoreConfig staticConfig;
        staticConfig.MutableStorageService()->SetSchemeShardDir("/static");
        auto dynamicConfig = MakeConfig(200);
        dynamicConfig.MutableStorageService()->SetSchemeShardDir("/cms");
        dynamicConfig.MutableServer()
            ->MutableServerConfig()
            ->SetDynamicYamlConfigurationEnabled(false);
        StartManager(std::move(staticConfig), dynamicConfig);

        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);

        // Acknowledge the unmodified startup source without rebuilding it.
        const auto initialConfig = ConfigHolder->Get();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(dynamicConfig),
            120);
        WaitForAck(120);
        UNIT_ASSERT_EQUAL(initialConfig.Get(), ConfigHolder->Get().Get());
        AssertNoConfigChanged(subscriber);

        // Treat removal of ignored fields as a change to the original source.
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200)),
            121);
        WaitForAck(121);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_UNEQUAL(initialConfig.Get(), ConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(
            "/static",
            ConfigHolder->Get()->GetStorageConfig()->GetSchemeShardDir());
    }

    // Verify that reordered map entries are duplicates while a changed map
    // value is accepted even when its runtime override is forbidden.
    Y_UNIT_TEST_F(ShouldCompareCmsMapsWithoutEntryOrder, TConfigsManagerFixture)
    {
        StartManager();

        // Publish a source with map entries in a known reflection order.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto makeConfig = [](bool reverse, ui32 writeThreshold)
        {
            auto config =
                std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
            auto* threshold =
                config->MutableDiagnostics()->AddRequestThresholds();
            const auto* field =
                threshold->GetDescriptor()->FindFieldByName("ByRequestType");
            for (const TString name: {"ReadBlocks", "WriteBlocks"}) {
                auto* entry =
                    threshold->GetReflection()->AddMessage(threshold, field);
                entry->GetReflection()->SetString(
                    entry,
                    entry->GetDescriptor()->FindFieldByName("key"),
                    name);
                entry->GetReflection()->SetUInt32(
                    entry,
                    entry->GetDescriptor()->FindFieldByName("value"),
                    name == "ReadBlocks" ? 10 : writeThreshold);
            }
            if (reverse) {
                threshold->GetReflection()
                    ->SwapElements(threshold, field, 0, 1);
            }
            const auto& entry = threshold->GetReflection()->GetRepeatedMessage(
                *threshold,
                field,
                0);
            UNIT_ASSERT_VALUES_EQUAL(
                reverse ? "WriteBlocks" : "ReadBlocks",
                entry.GetReflection()->GetString(
                    entry,
                    entry.GetDescriptor()->FindFieldByName("key")));
            return config;
        };
        const auto first = makeConfig(false, 20);
        const auto reordered = makeConfig(true, 20);
        SendNotification(first, 130);
        WaitForAck(130);
        WaitForConfigChanged(subscriber);

        // Compare map values by key, independently of their entry order.
        const auto publishedConfig = ConfigHolder->Get();
        SendNotification(reordered, 131);
        WaitForAck(131);
        UNIT_ASSERT_EQUAL(publishedConfig.Get(), ConfigHolder->Get().Get());
        AssertNoConfigChanged(subscriber);

        // Detect a changed map value in the next received source.
        SendNotification(makeConfig(true, 30), 132);
        WaitForAck(132);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_UNEQUAL(publishedConfig.Get(), ConfigHolder->Get().Get());
    }

    // Check that a duplicate is acknowledged without replacing the config.
    Y_UNIT_TEST_F(
        ShouldAcknowledgeDuplicateWithoutRepublishing,
        TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/{},
            /*dynamicConfig=*/MakeConfig(200));

        const auto previousConfig = ConfigHolder->Get();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200)),
            18);
        WaitForAck(18);

        UNIT_ASSERT_EQUAL(previousConfig.Get(), ConfigHolder->Get().Get());
    }

    // Check that empty and missing configs restore static values once and then
    // receive ACKs without publishing or notifying again.
    Y_UNIT_TEST_F(
        ShouldTreatEmptyAndMissingConfigAsDuplicates,
        TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/MakeConfig(100),
            /*dynamicConfig=*/MakeConfig(200));

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
        WaitForConfigLog("Reset PrivateDatabaseConfig");
    }

    // Check that a parser failure preserves the config and receives no ACK.
    Y_UNIT_TEST_F(
        ShouldRetainConfigAndHoldAckOnParserFailure,
        TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/{},
            /*dynamicConfig=*/MakeConfig(200));

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
            "CRITICAL_EVENT:AppCriticalEvents/DynamicConfigError");
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

    // Verify that preparation exceptions preserve the publication and controls,
    // emit a critical event, withhold ACK, and allow retrying the same update.
    Y_UNIT_TEST_F(
        ShouldRetainConfigOnPreparationException,
        TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/{},
            /*dynamicConfig=*/MakeConfig(200));

        // Inject one failure before construction and observe publications.
        ConfigHolder->Set(
            MakeIntrusive<TFailingBlockstoreConfig>(ConfigHolder->Get()));
        const auto previousConfig = ConfigHolder->Get();
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto update =
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
        SendNotification(update, 19);
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEventRethrow<
                     TEvConsole::TEvConfigNotificationResponse>(
            handle,
            TDuration::MilliSeconds(10)));

        // Keep the same publication and its live control values on failure.
        UNIT_ASSERT_EQUAL(previousConfig.Get(), ConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            previousConfig->GetStorageConfig()->GetWriteBlobThreshold());
        AssertNoConfigChanged(subscriber);
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "CRITICAL_EVENT:AppCriticalEvents/DynamicConfigError");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "test configuration preparation failure");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "Keeping the last successfully applied configuration");

        // Retry the identical source: the failed attempt must not become the
        // remembered dynamic configuration or suppress a retry as a duplicate.
        SendNotification(update, 22);
        WaitForAck(22);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT(previousConfig.Get() != ConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Check that missing required fields report a critical event without
    // publication or ACK, and a corrected update is accepted.
    Y_UNIT_TEST_F(ShouldRejectMissingRequiredFields, TConfigsManagerFixture)
    {
        StartManager();

        // Observe publications and retain the configuration before rejection.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        const auto previousConfig = ConfigHolder->Get();
        const auto parser = CreateBlockstoreOpaqueConfigParser();

        // Deliver the actual parser result for an incomplete repeated message.
        SendNotification(
            parser("diagnostics: {execution_time_size_classes: [null]}"),
            19);
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEventRethrow<
                     TEvConsole::TEvConfigNotificationResponse>(
            handle,
            TDuration::MilliSeconds(10)));

        // Preserve the snapshot and report the input error without notifying.
        UNIT_ASSERT_EQUAL(previousConfig.Get(), ConfigHolder->Get().Get());
        AssertNoConfigChanged(subscriber);
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "CRITICAL_EVENT:AppCriticalEvents/DynamicConfigError");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "Diagnostics.ExecutionTimeSizeClasses[0].Start");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "Diagnostics.ExecutionTimeSizeClasses[0].End");

        // Accept and publish a valid configuration after the rejected update.
        SendNotification(
            parser("storage_service: {write_blob_threshold: 300}"),
            22);
        WaitForAck(22);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Check that removing PrivateDatabaseConfig restores the static value.
    Y_UNIT_TEST_F(ShouldTreatMissingPayloadAsRemoval, TConfigsManagerFixture)
    {
        StartManager(
            /*staticConfig=*/MakeConfig(100),
            /*dynamicConfig=*/MakeConfig(200));

        // Observe the initial private value before removing its source.
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());

        // Remove the private source and wait for its acknowledgement.
        SendNotification({}, 20);
        WaitForAck(20);
        WaitForConfigLog("Reset PrivateDatabaseConfig");

        // Restore the overridden field from the static snapshot.
        auto config = ConfigHolder->Get();
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            config->GetStorageConfig()->GetWriteBlobThreshold());
    }

    // Check that a wrong message type preserves the config and receives no ACK.
    Y_UNIT_TEST_F(
        ShouldRejectUnexpectedOpaqueMessageType,
        TConfigsManagerFixture)
    {
        StartManager();

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
            "CRITICAL_EVENT:AppCriticalEvents/DynamicConfigError");
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
    Y_UNIT_TEST_F(ShouldRejectNullOpaquePayload, TConfigsManagerFixture)
    {
        StartManager();

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
            "CRITICAL_EVENT:AppCriticalEvents/DynamicConfigError");
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

    // Verify that invalid VolumeThrottling preserves its complete previous
    // section while other settings advance and corrected rules are accepted.
    Y_UNIT_TEST_F(
        ShouldRetainThrottlingOnInvalidSection,
        TConfigsManagerFixture)
    {
        StartManager();

        // Accept two rules before attempting to replace one and delete another.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        auto previous = std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(250));
        auto* rule = previous->MutableVolumeThrottling()->AddRules();
        rule->MutableDisks()->AddDiskIds("private-disk-id");
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.75);
        rule = previous->MutableVolumeThrottling()->AddRules();
        rule->MutableDisks()->AddDiskIds("deleted-disk-id");
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.8);
        SendNotification(previous, 18);
        WaitForAck(18);
        WaitForConfigChanged(subscriber);
        const auto previousRules =
            ConfigHolder->Get()->GetVolumeThrottlingConfig()->SerializeAsString();

        // Reject both throttling operations but publish the unrelated update.
        auto update = std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
        rule = update->MutableVolumeThrottling()->AddRules();
        rule->MutableDisks()->AddDiskIds("private-disk-id");
        rule->MutableCoefficients()->SetMaxReadBandwidth(2);
        update->MutableVolumeThrottling()
            ->AddRules()
            ->MutableDisks()
            ->AddDiskIds("deleted-disk-id");
        SendNotification(update, 19);
        WaitForAck(19);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(
            300, ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            previousRules,
            ConfigHolder->Get()->GetVolumeThrottlingConfig()->SerializeAsString());
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "CRITICAL_EVENT:AppCriticalEvents/DynamicConfigError");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "Rejected invalid VolumeThrottling configuration");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "VolumeThrottling.Rules[0].Coefficients.MaxReadBandwidth = 2");
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(), "expected a finite value in [0, 1]");
        UNIT_ASSERT(!CriticalEventsLog.Str().Contains("private-disk-id"));

        // Acknowledge a repeated rejection without republishing accepted values.
        const auto accepted = ConfigHolder->Get();
        SendNotification(update, 19);
        WaitForAck(19);
        UNIT_ASSERT_EQUAL(accepted.Get(), ConfigHolder->Get().Get());
        AssertNoConfigChanged(subscriber);

        // Reject a duplicate selector without blocking another storage update.
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.5);
        auto* duplicate = update->MutableVolumeThrottling()->AddRules();
        duplicate->CopyFrom(*rule);
        update->MutableStorageService()->SetWriteBlobThreshold(400);
        SendNotification(update, 20);
        WaitForAck(20);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(
            400, ConfigHolder->Get()->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(
            previousRules,
            ConfigHolder->Get()->GetVolumeThrottlingConfig()->SerializeAsString());
        UNIT_ASSERT_STRING_CONTAINS(
            CriticalEventsLog.Str(),
            "VolumeThrottling.Rules[2].Disks: same selector as "
            "VolumeThrottling.Rules[0].Disks");

        // Accept the corrected replacement and deletion together.
        update->MutableVolumeThrottling()->MutableRules()->RemoveLast();
        SendNotification(update, 22);
        WaitForAck(22);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(
            1, ConfigHolder->Get()->GetVolumeThrottlingConfig()->RulesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            0.5, ConfigHolder->Get()->GetVolumeThrottlingConfig()
                ->GetRules(0).GetCoefficients().GetMaxReadBandwidth());

        // Remove the valid dynamic section instead of retaining its old rules.
        update->ClearVolumeThrottling();
        SendNotification(update, 23);
        WaitForAck(23);
        WaitForConfigChanged(subscriber);
        UNIT_ASSERT_VALUES_EQUAL(
            0, ConfigHolder->Get()->GetVolumeThrottlingConfig()->RulesSize());
    }

    // Verify that VolumeThrottling skips unsupported or empty rules with
    // warnings while publishing the remaining rules and acknowledging delivery.
    Y_UNIT_TEST_F(
        ShouldWarnAboutSkippedVolumeThrottlingRules,
        TConfigsManagerFixture)
    {
        StartManager();

        // Mix an unsupported selector and empty coefficients with a valid rule.
        const auto subscriber = Runtime.AllocateEdgeActor();
        Subscribe(subscriber);
        WaitForConfigChanged(subscriber);
        auto update = std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300));
        update->MutableVolumeThrottling()
            ->AddRules()
            ->MutableCoefficients()
            ->SetMaxReadIops(2);
        auto* empty = update->MutableVolumeThrottling()->AddRules();
        empty->MutableDisks()->AddDiskIds("private-disk-id");
        empty->MutableCoefficients();
        auto* rule = update->MutableVolumeThrottling()->AddRules();
        rule->MutableFilter();
        rule->MutableCoefficients()->SetMaxReadBandwidth(0.5);

        // Accept the remaining rule and report the original skipped indices.
        SendNotification(update, 23);
        WaitForAck(23);
        WaitForConfigChanged(subscriber);
        WaitForConfigLog("Skip VolumeThrottling.Rules[1]");
        UNIT_ASSERT_STRING_CONTAINS(
            ConfigLog.Str(),
            "Skip VolumeThrottling.Rules[0]: no known selector (Filter or Disks)");
        UNIT_ASSERT_STRING_CONTAINS(
            ConfigLog.Str(),
            "Skip VolumeThrottling.Rules[1]: Coefficients contains no known fields");
        UNIT_ASSERT(!ConfigLog.Str().Contains("private-disk-id"));
        const auto& config = *ConfigHolder->Get()->GetVolumeThrottlingConfig();
        UNIT_ASSERT_VALUES_EQUAL(1, config.RulesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            0.5, config.GetRules(0).GetCoefficients().GetMaxReadBandwidth());
    }
}

}   // namespace NCloud::NBlockStore
