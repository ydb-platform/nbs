#include "volume_throttling_manager.h"

#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/api/volume_throttling_manager.h>
#include <cloud/blockstore/libs/storage/service/service_events_private.h>
#include <cloud/blockstore/libs/storage/testlib/test_runtime.h>
#include <cloud/blockstore/libs/storage/testlib/test_tablet.h>

#include <contrib/ydb/core/base/tablet.h>
#include <contrib/ydb/core/tablet/tablet_setup.h>

#include <library/cpp/testing/unittest/registar.h>

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

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime()
{
    auto runtime = std::make_unique<TTestBasicRuntime>(nodeCount);

    auto throttlingManager =
        CreateThrottlingManager(TDuration::Seconds(defaultCycleSeconds));

    runtime->AddLocalService(
        MakeThrottlingManagerServiceId(),
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TThrottlingManagerTest)
{
    Y_UNIT_TEST(ShouldAcceptNewerConfig)
    {
        auto runtime = PrepareTestActorRuntime();

        // Send update config request with version 1
        auto updateRequest =
            std::make_unique<TEvThrottlingManager::TEvUpdateConfigRequest>();
        updateRequest->ThrottlingConfig.SetVersion(1);

        auto sender = runtime->AllocateEdgeActor();
        runtime->Send(
            new IEventHandle(
                MakeThrottlingManagerServiceId(),
                sender,
                updateRequest.release()),
            0,
            true);

        auto response = runtime->GrabEdgeEvent<
            TEvThrottlingManager::TEvUpdateConfigResponse>();
        UNIT_ASSERT_C(!HasError(response->Error), "Error: " << response->Error);
    }

    Y_UNIT_TEST(ShouldRejectOlderConfig)
    {
        auto runtime = PrepareTestActorRuntime();

        // Send valid config first
        {
            auto updateRequest = std::make_unique<
                TEvThrottlingManager::TEvUpdateConfigRequest>();
            updateRequest->ThrottlingConfig.SetVersion(10);

            auto sender = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(
                MakeThrottlingManagerServiceId(),
                sender,
                updateRequest.release()));
            auto response = runtime->GrabEdgeEvent<
                TEvThrottlingManager::TEvUpdateConfigResponse>();
            UNIT_ASSERT_C(
                !HasError(response->Error),
                "Error: " << response->Error);
        }

        // Send older version
        {
            auto updateRequest = std::make_unique<
                TEvThrottlingManager::TEvUpdateConfigRequest>();
            updateRequest->ThrottlingConfig.SetVersion(5);

            auto sender = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(
                MakeThrottlingManagerServiceId(),
                sender,
                updateRequest.release()));

            auto response = runtime->GrabEdgeEvent<
                TEvThrottlingManager::TEvUpdateConfigResponse>();
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
                    case TEvThrottlingManager::EvNotifyVolume: {
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
            auto updateRequest = std::make_unique<
                TEvThrottlingManager::TEvUpdateConfigRequest>();
            updateRequest->ThrottlingConfig.SetVersion(1);

            auto sender = runtime->AllocateEdgeActor();
            runtime->Send(new IEventHandle(
                MakeThrottlingManagerServiceId(),
                sender,
                updateRequest.release()));
            auto response = runtime->GrabEdgeEvent<
                TEvThrottlingManager::TEvUpdateConfigResponse>();
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
            auto updateRequest = std::make_unique<
                TEvThrottlingManager::TEvUpdateConfigRequest>();
            updateRequest->ThrottlingConfig.SetVersion(version++);
            (configSetup(*updateRequest->ThrottlingConfig.AddRules()), ...);

            runtime->Send(
                new IEventHandle(
                    MakeThrottlingManagerServiceId(),
                    sender,
                    updateRequest.release()),
                0,
                true);

            auto response = runtime->GrabEdgeEvent<
                TEvThrottlingManager::TEvUpdateConfigResponse>();
            UNIT_ASSERT(HasError(response.Get()->Error));
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT,
                response.Get()->Error.GetCode());
        };

        // Test negative coefficient
        testInvalidConfig(
            [](NProto::TThrottlingRule& rule)
            {
                rule.MutableDisks()->AddDiskIds("vol0");
                rule.MutableCoefficients()->SetMaxReadBandwidth(-0.5);
            });

        // Test coefficient > 1.0
        testInvalidConfig(
            [](NProto::TThrottlingRule& rule)
            {
                rule.MutableDisks()->AddDiskIds("vol0");
                rule.MutableCoefficients()->SetMaxReadBandwidth(1.5);
            });

        // Test multiple invalid coefficients in one rule
        testInvalidConfig(
            [](NProto::TThrottlingRule& rule)
            {
                rule.MutableDisks()->AddDiskIds("vol0");
                rule.MutableCoefficients()->SetMaxReadBandwidth(0.5);
                rule.MutableCoefficients()->SetMaxWriteBandwidth(
                    2.0);   // Invalid
            });

        // Test valid rule with invalid rule in same config
        testInvalidConfig(
            [](NProto::TThrottlingRule& rule)
            {
                // Valid rule
                rule.MutableDisks()->AddDiskIds("vol1");
                rule.MutableCoefficients()->SetMaxReadBandwidth(0.5);
            },
            [](NProto::TThrottlingRule& rule)
            {
                // Invalid rule
                rule.MutableDisks()->AddDiskIds("vol2");
                rule.MutableCoefficients()->SetMaxReadBandwidth(-0.1);
            });
    }
}

}   // namespace NCloud::NBlockStore::NStorage
