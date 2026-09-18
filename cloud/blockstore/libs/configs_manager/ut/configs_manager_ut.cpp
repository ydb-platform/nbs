#include <cloud/blockstore/libs/configs_manager/configs_manager.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/core/cms/console/configs_dispatcher.h>
#include <contrib/ydb/core/cms/console/console.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

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
// Each test receives the shared holder, ICB controls, and monitoring counters.
class TFixture: public NUnitTest::TBaseFixture
{
protected:
    // The actor runtime under test and its node-local counters.
    TTestActorRuntime Runtime;

    // The edge actor receiving subscription requests and acknowledgements.
    TActorId Dispatcher;

    // The ConfigsManager actor address registered for the current test.
    TActorId Manager;

    // The shared holder inspected after each delivered update.
    TBlockstoreConfigHolderPtr BlockstoreConfigHolder;

    // The shared controls required when rebuilding storage adapters.
    NStorage::TStorageConfigControlsPtr Controls;

    void SetUp(NUnitTest::TTestContext&) override
    {
        NKikimr::SetupTabletServices(Runtime);
        Dispatcher = Runtime.AllocateEdgeActor();

        Controls = std::make_shared<NStorage::TStorageConfigControls>();
        auto staticConfig = MakeConfig(100);
        staticConfig.MutableStorageService()->SetVolumePreemptionType(
            NProto::PREEMPTION_MOVE_MOST_HEAVY);
        auto dynamicConfig = MakeConfig(200);

        // Keep a CMS value outside private YAML to distinguish the startup
        // configuration from the local configuration used for later updates.
        auto startupConfig = staticConfig;
        startupConfig.MutableStorageService()->SetVolumePreemptionType(
            NProto::PREEMPTION_MOVE_LEAST_HEAVY);
        BlockstoreConfigHolder = std::make_shared<TBlockstoreConfigHolder>(
            MakeBlockstoreConfig(
                startupConfig,
                dynamicConfig,
                Controls));

        Manager = Runtime.Register(CreateConfigsManager({
            .BlockstoreConfigHolder = BlockstoreConfigHolder,
            .StaticConfig = std::move(staticConfig),
            .InitialDynamicConfig = std::move(dynamicConfig),
            .InitialDynamicConfigPresent = true,
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

    void SendNotification(
        std::shared_ptr<const google::protobuf::Message> config,
        ui64 cookie)
    {
        auto event = std::make_unique<
            TEvConsole::TEvConfigNotificationRequest>();
        if (config) {
            event->OpaqueConfigs.emplace(
                PrivateDatabaseConfigKind,
                std::move(config));
        }

        Runtime.Send(new IEventHandle(
            Manager,
            Dispatcher,
            event.release(),
            0,
            cookie));
    }

    void WaitForAck(ui64 cookie)
    {
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(Runtime.GrabEdgeEventRethrow<
            TEvConsole::TEvConfigNotificationResponse>(handle));
        UNIT_ASSERT_VALUES_EQUAL(cookie, handle->Cookie);
    }

    ui64 GetCounter(TStringBuf name)
    {
        return *Runtime.GetAppData().Counters
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "configs_manager")
            ->GetCounter(TString(name));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigsManagerTest)
{
    // Check valid YAML conversion and rejection of an invalid section value.
    Y_UNIT_TEST(ShouldUseDefaultOpaqueConfigParser)
    {
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        const auto message = parser(R"(
storage_service:
  write_blob_threshold: 300
)");

        UNIT_ASSERT(message);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::TBlockstoreConfig::descriptor(),
            message->GetDescriptor());

        NProto::TBlockstoreConfig config;
        config.CopyFrom(*message);
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            config.GetStorageService().GetWriteBlobThreshold());

        const auto result = parser("storage_service: invalid");
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(
            NCloud::NProto::TError::descriptor(),
            result->GetDescriptor());
    }

    // Check that invalid and empty inputs return errors without exposing values.
    Y_UNIT_TEST(ShouldReturnErrorsForInvalidOpaqueConfig)
    {
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        for (const TString yaml: {
                 "",
                 " \t\n",
                 "---\n",
                 "storage_service: [",
                 "storage_service: private-secret-value"})
        {
            const auto message = parser(yaml);
            UNIT_ASSERT(message);
            UNIT_ASSERT_C(
                NCloud::NProto::TError::descriptor() == message->GetDescriptor(),
                yaml);

            NCloud::NProto::TError error;
            error.CopyFrom(*message);
            UNIT_ASSERT(FAILED(error.GetCode()));
            UNIT_ASSERT(!error.GetMessage().empty());
            UNIT_ASSERT(!error.GetMessage().Contains("private-secret-value"));
        }
    }

    // Check that empty mappings and unknown fields retain successful parsing.
    Y_UNIT_TEST(ShouldAcceptEmptyAndUnknownOpaqueConfig)
    {
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        for (const TString yaml: {"{}", "future_option: 1"}) {
            const auto message = parser(yaml);
            UNIT_ASSERT(message);
            UNIT_ASSERT_VALUES_EQUAL(
                NProto::TBlockstoreConfig::descriptor(),
                message->GetDescriptor());
            UNIT_ASSERT_VALUES_EQUAL(0, message->ByteSizeLong());
        }
    }

    // Check that an accepted update is published before it is acknowledged.
    Y_UNIT_TEST_F(ShouldPublishValidConfigAndAcknowledgeAfterPublication, TFixture)
    {
        const auto previousConfig = BlockstoreConfigHolder->Get();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(300)),
            17);
        WaitForAck(17);

        auto config = BlockstoreConfigHolder->Get();
        UNIT_ASSERT_UNEQUAL(previousConfig.Get(), config.Get());
        UNIT_ASSERT_VALUES_EQUAL(
            300,
            config->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(1, GetCounter("SuccessfulUpdates"));
    }

    // Check that a duplicate is acknowledged without replacing the config.
    Y_UNIT_TEST_F(ShouldAcknowledgeDuplicateWithoutRepublishing, TFixture)
    {
        const auto previousConfig = BlockstoreConfigHolder->Get();
        SendNotification(
            std::make_shared<NProto::TBlockstoreConfig>(MakeConfig(200)),
            18);
        WaitForAck(18);

        UNIT_ASSERT_EQUAL(
            previousConfig.Get(),
            BlockstoreConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(0, GetCounter("SuccessfulUpdates"));
    }

    // Check that a parser failure preserves the config and receives no ACK.
    Y_UNIT_TEST_F(ShouldRetainConfigAndHoldAckOnParserFailure, TFixture)
    {
        const auto previousConfig = BlockstoreConfigHolder->Get();
        const auto parser = CreateBlockstoreOpaqueConfigParser();
        SendNotification(parser("storage_service: invalid"), 19);
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEventRethrow<
            TEvConsole::TEvConfigNotificationResponse>(
                handle,
                TDuration::MilliSeconds(10)));

        UNIT_ASSERT_EQUAL(
            previousConfig.Get(),
            BlockstoreConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(
            200,
            BlockstoreConfigHolder->Get()
                ->GetStorageConfig()
                ->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(1, GetCounter("RejectedUpdates"));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(EConfigsManagerUpdateStatus::Rejected),
            GetCounter("LastUpdateStatus"));

        // Apply a corrected config after the error notification received no ACK.
        SendNotification(
            parser("storage_service:\n  write_blob_threshold: 300"),
            22);
        WaitForAck(22);

        UNIT_ASSERT_VALUES_EQUAL(
            300,
            BlockstoreConfigHolder->Get()
                ->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_VALUES_EQUAL(1, GetCounter("SuccessfulUpdates"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetCounter("RejectedUpdates"));
    }

    // Check that removing the private section restores the pre-CMS value,
    // excluding both startup CMS and private YAML values.
    Y_UNIT_TEST_F(ShouldTreatMissingPayloadAsRemoval, TFixture)
    {
        // Observe the startup CMS value before removing the private section.
        UNIT_ASSERT_EQUAL(
            NProto::PREEMPTION_MOVE_LEAST_HEAVY,
            BlockstoreConfigHolder->Get()
                ->GetStorageConfig()->GetVolumePreemptionType());

        SendNotification({}, 20);
        WaitForAck(20);

        // Restore both the overridden YAML field and the startup CMS field
        // from the local snapshot.
        auto config = BlockstoreConfigHolder->Get();
        UNIT_ASSERT_VALUES_EQUAL(
            100,
            config->GetStorageConfig()->GetWriteBlobThreshold());
        UNIT_ASSERT_EQUAL(
            NProto::PREEMPTION_MOVE_MOST_HEAVY,
            config->GetStorageConfig()->GetVolumePreemptionType());
        UNIT_ASSERT_VALUES_EQUAL(0, GetCounter("DynamicConfigPresent"));
        UNIT_ASSERT_VALUES_EQUAL(1, GetCounter("SuccessfulUpdates"));
    }

    // Check that a wrong message type preserves the config and receives no ACK.
    Y_UNIT_TEST_F(ShouldRejectUnexpectedOpaqueMessageType, TFixture)
    {
        const auto previousConfig = BlockstoreConfigHolder->Get();
        SendNotification(
            std::make_shared<NProto::TStorageServiceConfig>(),
            21);
        TAutoPtr<IEventHandle> handle;
        UNIT_ASSERT(!Runtime.GrabEdgeEventRethrow<
            TEvConsole::TEvConfigNotificationResponse>(
                handle,
                TDuration::MilliSeconds(10)));

        UNIT_ASSERT_EQUAL(
            previousConfig.Get(),
            BlockstoreConfigHolder->Get().Get());
        UNIT_ASSERT_VALUES_EQUAL(1, GetCounter("RejectedUpdates"));
    }

    // Check that private config cannot change DynamicYamlConfigurationEnabled.
    Y_UNIT_TEST(ShouldRemoveDynamicYamlFlagFromPrivateConfig)
    {
        auto dynamicConfig = MakeConfig(300);
        dynamicConfig.MutableServer()
            ->MutableServerConfig()
            ->SetDynamicYamlConfigurationEnabled(false);

        RemoveStaticOnlyBlockstoreFields(&dynamicConfig);

        UNIT_ASSERT_VALUES_EQUAL(
            300,
            dynamicConfig
                .GetStorageService()
                .GetWriteBlobThreshold());
        UNIT_ASSERT(!dynamicConfig
            .GetServer()
            .GetServerConfig()
            .HasDynamicYamlConfigurationEnabled());
    }
}

}   // namespace NCloud::NBlockStore
