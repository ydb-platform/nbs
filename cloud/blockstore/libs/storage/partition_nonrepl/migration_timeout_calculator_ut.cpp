#include "migration_timeout_calculator.h"

#include <cloud/blockstore/config/storage.pb.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

namespace {

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
    return std::make_shared<TNonreplicatedPartitionConfig>(
        devices,
        NProto::VOLUME_IO_OK,
        "vol0",
        4_KB,
        TNonreplicatedPartitionConfig::TVolumeInfo{
            Now(),
            // only SSD/HDD distinction matters
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED},
        NActors::TActorId(),
        false,                 // muteIOErrors
        false,                 // markBlocksUsed
        THashSet<TString>(),   // freshDeviceIds
        TDuration::Zero(),     // maxTimedOutDeviceStateDuration
        false,                 // maxTimedOutDeviceStateDurationOverridden
        useSimpleMigrationBandwidthLimiter);
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

        // Devices #1, #2, #4 belong to Agent#1, device #3 belong to Agent#2.
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
}

}   // namespace NCloud::NBlockStore::NStorage
