#include "volume_throttling_manager.h"

#include <cloud/blockstore/libs/configs_manager/events.h>
#include <cloud/blockstore/libs/configs_manager/testlib/dynconfig.h>

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume_throttling_manager.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/test_tablet.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/tablet/tablet_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <limits>
#include <optional>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui32 defaultCycleSeconds = 5;
const ui32 nodeCount = 2;
const TString localVolumeActor = "actor-local";
const TString remoteVolumeActor = "actor-remote";

////////////////////////////////////////////////////////////////////////////////

class TDummyActor final: public TActor<TDummyActor>
{
public:
    TDummyActor()
        : TActor(&TThis::StateWork)
    {}

private:
    STFUNC(StateWork)
    {
        Y_UNUSED(ev);
    }
};

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime(
    NProto::TVolumeThrottlingServiceConfig initialConfig = {})
{
    auto runtime = std::make_unique<TTestBasicRuntime>(nodeCount);

    auto throttlingManager =
        CreateVolumeThrottlingManager(
            TDuration::Seconds(defaultCycleSeconds),
            std::make_shared<const NProto::TVolumeThrottlingServiceConfig>(
                std::move(initialConfig)));

    runtime->AddLocalService(
        MakeVolumeThrottlingManagerServiceId(),
        TActorSetupCmd(throttlingManager.release(), TMailboxType::Simple, 0));

    // Don't care what actor is behind StorageServiceId as we'll emulate
    // responses from it
    runtime->AddLocalService(
        MakeStorageServiceId(),
        TActorSetupCmd(new TDummyActor(), TMailboxType::Simple, 0));

    // Don't care what actors are behind this volume actor ID's as we don't test
    // their behaviour here
    runtime->AddLocalService(
        TActorId(runtime->GetNodeId(0), localVolumeActor),
        TActorSetupCmd(new TDummyActor(), TMailboxType::Simple, 0));

    runtime->AddLocalService(
        TActorId(runtime->GetNodeId(1), remoteVolumeActor),
        TActorSetupCmd(new TDummyActor(), TMailboxType::Simple, 0));

    SetupTabletServices(*runtime);

    return runtime;
}

// Route the manager's local-volume notifications to the selected edge actor.
void RouteNotifications(TTestActorRuntime& runtime, TActorId recipient)
{
    runtime.SetEventFilter(
        [recipient](TTestActorRuntimeBase& runtime,
                    TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() !=
                TEvServicePrivate::EvListMountedVolumesRequest)
            {
                return false;
            }

            auto response = std::make_unique<
                TEvServicePrivate::TEvListMountedVolumesResponse>();
            response->MountedVolumes = {{"disk", recipient}};
            runtime.Send(new IEventHandle(
                event->Sender, event->Recipient, response.release()));
            return true;
        });
}

// Submit one manual replacement and return the manager's acceptance result.
NProto::TError SetManualConfig(
    TTestActorRuntime& runtime,
    ui32 version,
    std::optional<double> coefficient = {})
{
    auto request = std::make_unique<TEvVolumeThrottlingManager::
                                       TEvUpdateVolumeThrottlingConfigRequest>();
    request->ThrottlingConfig.SetVersion(version);
    if (coefficient) {
        request->ThrottlingConfig.AddRules()->MutableCoefficients()
            ->SetMaxReadBandwidth(*coefficient);
    }

    const auto sender = runtime.AllocateEdgeActor();
    runtime.Send(new IEventHandle(
        MakeVolumeThrottlingManagerServiceId(), sender, request.release()));
    return runtime.GrabEdgeEvent<TEvVolumeThrottlingManager::
                                     TEvUpdateVolumeThrottlingConfigResponse>(
        sender)->Get()->Error;
}

// Read the next effective configuration delivered to the local volume edge.
NProto::TVolumeThrottlingConfig WaitForConfig(
    TTestActorRuntime& runtime,
    TActorId recipient)
{
    return runtime.GrabEdgeEvent<TEvVolumeThrottlingManager::
                                     TEvVolumeThrottlingConfigNotification>(
        recipient)->Get()->Config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeThrottlingManagerTest)
{
    Y_UNIT_TEST(ShouldAcceptNewerConfig)
    {
        auto runtime = PrepareTestActorRuntime();

        // Send update config request with version 1
        auto updateRequest =
            std::make_unique<TEvVolumeThrottlingManager::
                                 TEvUpdateVolumeThrottlingConfigRequest>();
        updateRequest->ThrottlingConfig.SetVersion(1);

        auto sender = runtime->AllocateEdgeActor();
        runtime->Send(
            new IEventHandle(
                MakeVolumeThrottlingManagerServiceId(),
                sender,
                updateRequest.release()),
            0,
            true);

        auto response =
            runtime
                ->GrabEdgeEvent<TEvVolumeThrottlingManager::
                                    TEvUpdateVolumeThrottlingConfigResponse>();
        UNIT_ASSERT_C(!HasError(response->Error), "Error: " << response->Error);
    }

    Y_UNIT_TEST(ShouldRejectOlderConfig)
    {
        auto runtime = PrepareTestActorRuntime();

        // Send valid config first
        {
            auto updateRequest =
                std::make_unique<TEvVolumeThrottlingManager::
                                     TEvUpdateVolumeThrottlingConfigRequest>();
            updateRequest->ThrottlingConfig.SetVersion(10);

            auto sender = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(
                MakeVolumeThrottlingManagerServiceId(),
                sender,
                updateRequest.release()));
            auto response = runtime->GrabEdgeEvent<
                TEvVolumeThrottlingManager::
                    TEvUpdateVolumeThrottlingConfigResponse>();
            UNIT_ASSERT_C(
                !HasError(response->Error),
                "Error: " << response->Error);
        }

        // Send older version
        {
            auto updateRequest =
                std::make_unique<TEvVolumeThrottlingManager::
                                     TEvUpdateVolumeThrottlingConfigRequest>();
            updateRequest->ThrottlingConfig.SetVersion(5);

            auto sender = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(
                MakeVolumeThrottlingManagerServiceId(),
                sender,
                updateRequest.release()));

            auto response = runtime->GrabEdgeEvent<
                TEvVolumeThrottlingManager::
                    TEvUpdateVolumeThrottlingConfigResponse>();
            UNIT_ASSERT(HasError(response->Error));
            UNIT_ASSERT_VALUES_EQUAL(E_FAIL, response->Error.GetCode());
            UNIT_ASSERT_STRING_CONTAINS(
                response->Error.GetMessage(),
                "version older then (or equal to) the known one");
        }
    }

    Y_UNIT_TEST(ShouldNotifyLocalVolumes)
    {
        auto runtime = PrepareTestActorRuntime();

        int notifyCount = 0;
        bool listRequestObserved = false;
        runtime->SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvServicePrivate::EvListMountedVolumesRequest: {
                        listRequestObserved = true;

                        auto response = std::make_unique<
                            TEvServicePrivate::TEvListMountedVolumesResponse>();
                        response->MountedVolumes = {
                            {.DiskId = "vol0",
                             .VolumeActor = TActorId(
                                 runtime.GetNodeId(0),
                                 localVolumeActor)},   // Local
                            {.DiskId = "vol1",
                             .VolumeActor = TActorId(
                                 runtime.GetNodeId(1),
                                 remoteVolumeActor)},   // Remote
                        };

                        runtime.Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release()));

                        return true;
                    }
                    case TEvVolumeThrottlingManager::
                        EvVolumeThrottlingConfigNotification: {
                        UNIT_ASSERT_EQUAL(
                            event->Recipient.NodeId(),
                            runtime.GetNodeId(0));
                        ++notifyCount;
                        return true;
                    }
                    default:
                        return false;
                }
            });

        // Setup config
        {
            auto updateRequest =
                std::make_unique<TEvVolumeThrottlingManager::
                                     TEvUpdateVolumeThrottlingConfigRequest>();
            updateRequest->ThrottlingConfig.SetVersion(1);

            auto sender = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(
                MakeVolumeThrottlingManagerServiceId(),
                sender,
                updateRequest.release()));
            auto response = runtime->GrabEdgeEvent<
                TEvVolumeThrottlingManager::
                    TEvUpdateVolumeThrottlingConfigResponse>();
            UNIT_ASSERT_C(
                !HasError(response->Error),
                "Error: " << response->Error);
        }

        runtime->DispatchEvents({}, TDuration::Seconds(defaultCycleSeconds));

        UNIT_ASSERT_C(
            listRequestObserved,
            "No TEvServicePrivate::EvListMountedVolumesRequest observed");

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            notifyCount);   // Only local volume notified
    }

    Y_UNIT_TEST(ShouldRejectInvalidCoefficients)
    {
        auto runtime = PrepareTestActorRuntime();
        auto sender = runtime->AllocateEdgeActor();

        ui32 version = 1;
        auto testInvalidConfig = [&](auto&&... configSetup)
        {
            auto updateRequest =
                std::make_unique<TEvVolumeThrottlingManager::
                                     TEvUpdateVolumeThrottlingConfigRequest>();
            updateRequest->ThrottlingConfig.SetVersion(version++);
            (configSetup(*updateRequest->ThrottlingConfig.AddRules()), ...);

            runtime->Send(
                new IEventHandle(
                    MakeVolumeThrottlingManagerServiceId(),
                    sender,
                    updateRequest.release()),
                0,
                true);

            auto response = runtime->GrabEdgeEvent<
                TEvVolumeThrottlingManager::
                    TEvUpdateVolumeThrottlingConfigResponse>();
            UNIT_ASSERT(HasError(response.Get()->Error));
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                response.Get()->Error.GetCode());
        };

        // Test negative coefficient
        testInvalidConfig(
            [](NProto::TVolumeThrottlingRule& rule)
            {
                rule.MutableDisks()->AddDiskIds("vol0");
                rule.MutableCoefficients()->SetMaxReadBandwidth(-0.5);
            });

        // Test coefficient > 1.0
        testInvalidConfig(
            [](NProto::TVolumeThrottlingRule& rule)
            {
                rule.MutableDisks()->AddDiskIds("vol0");
                rule.MutableCoefficients()->SetMaxReadBandwidth(1.5);
            });

        // Test multiple invalid coefficients in one rule
        testInvalidConfig(
            [](NProto::TVolumeThrottlingRule& rule)
            {
                rule.MutableDisks()->AddDiskIds("vol0");
                rule.MutableCoefficients()->SetMaxReadBandwidth(0.5);
                rule.MutableCoefficients()->SetMaxWriteBandwidth(
                    2.0);   // Invalid
            });

        // Test valid rule with invalid rule in same config
        testInvalidConfig(
            [](NProto::TVolumeThrottlingRule& rule)
            {
                // Valid rule
                rule.MutableDisks()->AddDiskIds("vol1");
                rule.MutableCoefficients()->SetMaxReadBandwidth(0.5);
            },
            [](NProto::TVolumeThrottlingRule& rule)
            {
                // Invalid rule
                rule.MutableDisks()->AddDiskIds("vol2");
                rule.MutableCoefficients()->SetMaxReadBandwidth(-0.1);
            });
    }

    // Verify that VolumeThrottling rejects NaN and infinity but accepts a valid
    // CLI update with the same version.
    Y_UNIT_TEST(ShouldRejectNonFiniteCoefficients)
    {
        auto runtime = PrepareTestActorRuntime();
        UNIT_ASSERT(HasError(SetManualConfig(
            *runtime, 1, std::numeric_limits<double>::quiet_NaN())));
        UNIT_ASSERT(HasError(SetManualConfig(
            *runtime, 1, std::numeric_limits<double>::infinity())));
        UNIT_ASSERT(!HasError(SetManualConfig(*runtime, 1, 0.5)));
    }

    // Verify that VolumeThrottling startup rules reach Volume without waiting
    // for a ConfigsManager response.
    Y_UNIT_TEST(ShouldApplyStartupConfiguration)
    {
        // Start the manager with service rules and route Volume notifications.
        NProto::TVolumeThrottlingServiceConfig config;
        config.AddRules()->MutableCoefficients()->SetMaxReadBandwidth(0.5);
        auto runtime = PrepareTestActorRuntime(std::move(config));
        const auto recipient = runtime->AllocateEdgeActor();
        RouteNotifications(*runtime, recipient);

        // Request another delivery and check the startup rules and delivery version.
        runtime->Send(new IEventHandle(
            MakeVolumeThrottlingManagerServiceId(), recipient,
            new TEvents::TEvWakeup()));
        const auto received = WaitForConfig(*runtime, recipient);
        UNIT_ASSERT_VALUES_EQUAL(1, received.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(
            0.5, received.GetRules(0).GetCoefficients().GetMaxReadBandwidth());
    }

    // Verify that VolumeThrottling CLI rules override service rules and
    // clearing CLI rules applies the latest service update.
    Y_UNIT_TEST(ShouldRestoreLatestBaseAfterManualOverride)
    {
        // Deliver the initial service configuration snapshot.
        auto runtime = PrepareTestActorRuntime();
        const auto recipient = runtime->AllocateEdgeActor();
        RouteNotifications(*runtime, recipient);
        auto provider = std::make_shared<
            NTestDynconfig::TTestBlockstoreConfigProvider>();
        auto* base = provider->Config.MutableVolumeThrottling();
        base->AddRules()->MutableCoefficients()->SetMaxReadBandwidth(0.5);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            *runtime, MakeVolumeThrottlingManagerServiceId(), provider);
        auto received = WaitForConfig(*runtime, recipient);
        UNIT_ASSERT_VALUES_EQUAL(
            0.5, received.GetRules(0).GetCoefficients().GetMaxReadBandwidth());

        // Apply CLI rules, update service rules, and check that CLI delivery stays
        // unchanged.
        UNIT_ASSERT(!HasError(SetManualConfig(*runtime, 1, 0.25)));
        received = WaitForConfig(*runtime, recipient);
        const auto manualDeliveryVersion = received.GetVersion();
        base->MutableRules(0)->MutableCoefficients()->SetMaxReadBandwidth(0.125);
        provider->Publish();
        runtime->Send(new IEventHandle(
            MakeVolumeThrottlingManagerServiceId(), recipient,
            new TEvConfigsManager::TEvSetConfigSubscriptionResponse(
                provider->Get())));
        runtime->Send(new IEventHandle(
            MakeVolumeThrottlingManagerServiceId(), recipient,
            new TEvents::TEvWakeup()));
        received = WaitForConfig(*runtime, recipient);
        UNIT_ASSERT_VALUES_EQUAL(manualDeliveryVersion, received.GetVersion());
        UNIT_ASSERT_VALUES_EQUAL(
            0.25, received.GetRules(0).GetCoefficients().GetMaxReadBandwidth());

        // Clear CLI rules and check that the latest service update is delivered.
        UNIT_ASSERT(!HasError(SetManualConfig(*runtime, 2)));
        received = WaitForConfig(*runtime, recipient);
        UNIT_ASSERT(received.GetVersion() > manualDeliveryVersion);
        UNIT_ASSERT_VALUES_EQUAL(
            0.125, received.GetRules(0).GetCoefficients().GetMaxReadBandwidth());

        // Remove service rules and check that a repeated CLI version is rejected.
        provider->Config.ClearVolumeThrottling();
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(
            *runtime, MakeVolumeThrottlingManagerServiceId(), provider);
        received = WaitForConfig(*runtime, recipient);
        UNIT_ASSERT_VALUES_EQUAL(0, received.RulesSize());
        UNIT_ASSERT(HasError(SetManualConfig(*runtime, 2, 0.5)));
    }

    // Verify that VolumeThrottling CLI updates share one periodic wakeup.
    Y_UNIT_TEST(ShouldKeepOneNotificationTimer)
    {
        auto runtime = PrepareTestActorRuntime();
        ui32 timers = 0;
        runtime->SetScheduledEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event,
                TDuration, TInstant&)
            {
                if (event->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                    ++timers;
                }
                return false;
            });

        // Apply two CLI updates before the pending wakeup can run.
        UNIT_ASSERT(!HasError(SetManualConfig(*runtime, 1, 0.5)));
        UNIT_ASSERT(!HasError(SetManualConfig(*runtime, 2, 0.25)));
        UNIT_ASSERT_VALUES_EQUAL(1, timers);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
