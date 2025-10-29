#include "migration_timeout_calculator.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TMyTestEnv final
{
private:
    TActorId Sender;
    TTestBasicRuntime Runtime;
    ui32 RegistrationCount = 0;
    ui32 BandwidthLimit = 0;

public:
    TMyTestEnv()
    {
        NKikimr::SetupTabletServices(Runtime);

        Sender = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeStorageStatsServiceId(), Sender);

        auto filter =
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvStatsServicePrivate::EvRegisterTrafficSourceRequest: {
                    ++RegistrationCount;
                    auto response =
                        std::make_unique<TEvStatsServicePrivate::
                                             TEvRegisterTrafficSourceResponse>(
                            BandwidthLimit);

                    auto* handle = new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0,
                        event->Cookie);

                    runtime.Send(handle, 0);

                    return true;
                }

                default:
                    return false;
            }
        };
        Runtime.SetEventFilter(filter);
    }

    TActorId Register(IActorPtr actor)
    {
        auto actorId = Runtime.Register(actor.release());
        Runtime.EnableScheduleForActor(actorId);

        return actorId;
    }

    void SetBandwidthLimit(ui32 bandwidthLimit)
    {
        BandwidthLimit = bandwidthLimit;
    }

    ui32 GetRegistrationCount() const
    {
        return RegistrationCount;
    }

    void Send(const TActorId& recipient, IEventBasePtr event)
    {
        Runtime.Send(new IEventHandle(recipient, Sender, event.release()));
    }

    void DispatchEvents(TDuration timeout)
    {
        Runtime.DispatchEvents(TDispatchOptions(), timeout);
    }
};

class TTestActor: public TActor<TTestActor>
{
public:
    explicit TTestActor(TNonreplicatedPartitionConfigPtr config)
        : TActor(&TThis::Main)
        , MigrationTimeoutCalculator(16, 4, std::move(config))
    {}

    void Main(TAutoPtr<IEventHandle>& ev)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvBootstrap, HandleBootstrap);
            HFunc(TEvents::TEvPing, HandlePing);
            HFunc(
                TEvStatsServicePrivate::TEvRegisterTrafficSourceResponse,
                MigrationTimeoutCalculator.HandleUpdateBandwidthLimit);
        }
    }

    void HandleBootstrap(
        const TEvents::TEvBootstrap::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        MigrationTimeoutCalculator.RegisterTrafficSource(ctx);
    }

    void HandlePing(const TEvents::TEvPing::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ev);

        MigrationTimeoutCalculator.RegisterTrafficSource(ctx);
    }

    TMigrationTimeoutCalculator MigrationTimeoutCalculator;
};

////////////////////////////////////////////////////////////////////////////////

TDevices MakeDevices()
{
    TDevices result;
    {
        auto* device = result.Add();
        device->SetAgentId("Agent#1");
        device->SetBlocksCount(1024);
        device->SetDeviceUUID("1_1");
    }
    {
        auto* device = result.Add();
        device->SetAgentId("Agent#1");
        device->SetBlocksCount(1024);
        device->SetDeviceUUID("1_2");
    }
    {
        auto* device = result.Add();
        device->SetAgentId("Agent#2");
        device->SetBlocksCount(1024);
        device->SetDeviceUUID("2_1");
    }
    {
        auto* device = result.Add();
        device->SetAgentId("Agent#1");
        device->SetBlocksCount(1024);
        device->SetDeviceUUID("1_3");
    }
    return result;
}

TNonreplicatedPartitionConfigPtr MakePartitionConfig(
    TDevices devices,
    bool useSimpleMigrationBandwidthLimiter)
{
    TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfigInitParams
        params{
            std::move(devices),
            TNonreplicatedPartitionConfig::TVolumeInfo{
                .CreationTs = Now(),
                // only SSD/HDD distinction matters
                .MediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                .EncryptionMode = NProto::NO_ENCRYPTION},
            "vol0",
            DefaultBlockSize,
            NActors::TActorId()};
    params.UseSimpleMigrationBandwidthLimiter =
        useSimpleMigrationBandwidthLimiter;

    return std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));
}

}   // namespace

Y_UNIT_TEST_SUITE(TMigrationCalculatorTest)
{
    Y_UNIT_TEST(ShouldCalculateMigrationTimeout)
    {
        TMigrationTimeoutCalculator timeoutCalculator(
            16,
            4,
            MakePartitionConfig(MakeDevices(), false));

        // Devices #1, #2, #4 belong to Agent#1, device #3 belongs to Agent#2.
        // Therefore, we expect a timeout of 3 times less for 1,2,4 devices than
        // for the 3rd device.

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 0, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 1, 1024)));

        //
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 2, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 3, 1024)));
    }

    Y_UNIT_TEST(ShouldCalculateMigrationTimeoutWithSimpleLimiter)
    {
        TMigrationTimeoutCalculator timeoutCalculator(
            16,
            100500,
            MakePartitionConfig(MakeDevices(), true));

        // When UseSimpleMigrationBandwidthLimiter enabled we expect the same
        // timeout for all devices. This timeout does not depend on the expected
        // number of devices on the agent (ExpectedDiskAgentSize).

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 4,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 0, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 4,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 1, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 4,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 2, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 4,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 3, 1024)));
    }

    Y_UNIT_TEST(ShouldCalculateMigrationTimeoutWithRecommendedBandwidth)
    {
        TMigrationTimeoutCalculator timeoutCalculator(
            16,
            100500,
            MakePartitionConfig(MakeDevices(), true));

        // Old-fashion timeout calculation
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 4,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 0, 1024)));

        // Calculate timeout with recommended bandwidth
        timeoutCalculator.SetRecommendedBandwidth(40_MB);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 10,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 0, 1024)));

        // Reset recommendation and do old-fashion timeout calculation
        timeoutCalculator.SetRecommendedBandwidth(0);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 4,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 0, 1024)));
    }

    Y_UNIT_TEST(ShouldRegisterTrafficSourceWithSimpleLimiter)
    {
        TMyTestEnv testEnv;

        auto* testActorPtr =
            new TTestActor(MakePartitionConfig(MakeDevices(), true));
        auto& timeoutCalculator = testActorPtr->MigrationTimeoutCalculator;

        auto actorId = testEnv.Register(IActorPtr(testActorPtr));

        testEnv.SetBandwidthLimit(16);
        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>());
        UNIT_ASSERT_VALUES_EQUAL(1, testEnv.GetRegistrationCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 4,
            timeoutCalculator.CalculateTimeout(TBlockRange64::MakeOneBlock(0)));

        testEnv.SetBandwidthLimit(8);
        testEnv.Send(actorId, std::make_unique<TEvents::TEvPing>());
        UNIT_ASSERT_VALUES_EQUAL(2, testEnv.GetRegistrationCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 2,
            timeoutCalculator.CalculateTimeout(TBlockRange64::MakeOneBlock(0)));

        testEnv.SetBandwidthLimit(2);
        testEnv.Send(actorId, std::make_unique<TEvents::TEvPing>());
        UNIT_ASSERT_VALUES_EQUAL(3, testEnv.GetRegistrationCount());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) * 2,
            timeoutCalculator.CalculateTimeout(TBlockRange64::MakeOneBlock(0)));
    }

    Y_UNIT_TEST(ShouldRegisterTrafficSourceAndUseLimit)
    {
        TMyTestEnv testEnv;

        auto* testActorPtr =
            new TTestActor(MakePartitionConfig(MakeDevices(), false));
        auto& timeoutCalculator = testActorPtr->MigrationTimeoutCalculator;

        auto actorId = testEnv.Register(IActorPtr(testActorPtr));

        testEnv.SetBandwidthLimit(16);   // No limit
        testEnv.Send(actorId, std::make_unique<TEvents::TEvBootstrap>());
        UNIT_ASSERT_VALUES_EQUAL(1, testEnv.GetRegistrationCount());

        // Devices #1, #2, #4 belong to Agent#1, device #3 belongs to Agent#2.
        // Therefore, we expect a timeout of 3 times less for 1,2,4 devices than
        // for the 3rd device.
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 0, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 1, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 2, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 3, 1024)));

        // setting the limit to two times less
        testEnv.SetBandwidthLimit(8);
        testEnv.Send(actorId, std::make_unique<TEvents::TEvPing>());
        UNIT_ASSERT_VALUES_EQUAL(2, testEnv.GetRegistrationCount());

        // Devices #1, #2, #4 belong to Agent#1, device #3 belongs to Agent#2.
        // Therefore, we expect a timeout of 3 times less for 1,2,4 devices than
        // for the 3rd device.
        UNIT_ASSERT_VALUES_EQUAL(
            2 * TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 0, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            2 * TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 1, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1),   // it has not changed because the minimum
                                     // request rate is one per second
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 2, 1024)));

        UNIT_ASSERT_VALUES_EQUAL(
            2 * TDuration::Seconds(1) / 3,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 3, 1024)));
    }

    Y_UNIT_TEST(ShouldWorkWithEmptyPartitionConfig)
    {
        TMigrationTimeoutCalculator timeoutCalculator(
            16,
            100500,
            nullptr);

        // Calculate timeout with inital bandwidth
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 4,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(0, 1024)));

        // Calculate timeout with recommended bandwidth
        timeoutCalculator.SetRecommendedBandwidth(40_MB);
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(1) / 10,
            timeoutCalculator.CalculateTimeout(
                TBlockRange64::WithLength(1024 * 0, 1024)));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
