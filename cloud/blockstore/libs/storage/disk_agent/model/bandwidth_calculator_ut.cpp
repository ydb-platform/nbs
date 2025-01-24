#include "bandwidth_calculator.h"

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBandwidthCalculatorTest)
{
    Y_UNIT_TEST(ShouldUseDefaultBandwidth)
    {
        NProto::TDiskAgentConfig proto;
        auto* throttling = proto.MutableThrottlingConfig();
        throttling->SetDefaultNetworkMbitThroughput(384);   // 48 MiB
        throttling->SetDirectCopyBandwidthFraction(0.5);    // 24 MiB
        throttling->SetMaxDeviceBandwidthMiB(20);           // 20 MiB

        TDiskAgentConfig config(std::move(proto), "rack", 0);
        TBandwidthCalculator bandwidthCalculator(config);

        TInstant now = TInstant::Now();
        // One client is limited to the MaxDeviceBandwidth
        ui64 bandwidth = bandwidthCalculator.RegisterRequest("device_1", now);
        UNIT_ASSERT_VALUES_EQUAL(20_MB, bandwidth);

        // Two clients receive half of the bandwidth each
        bandwidth = bandwidthCalculator.RegisterRequest("device_2", now);
        UNIT_ASSERT_VALUES_EQUAL(12_MB, bandwidth);
        bandwidth = bandwidthCalculator.RegisterRequest("device_1", now);
        UNIT_ASSERT_VALUES_EQUAL(12_MB, bandwidth);

        // Three clients receive a third of the bandwidth each
        bandwidth = bandwidthCalculator.RegisterRequest("device_3", now);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, bandwidth);
        bandwidth = bandwidthCalculator.RegisterRequest("device_2", now);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, bandwidth);
        bandwidth = bandwidthCalculator.RegisterRequest("device_1", now);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, bandwidth);

        // After a second, we assume that the clients have left
        now = now + TDuration::MilliSeconds(1001);
        bandwidth = bandwidthCalculator.RegisterRequest("device_1", now);
        UNIT_ASSERT_VALUES_EQUAL(20_MB, bandwidth);
    }

    Y_UNIT_TEST(ShouldUseInfraNetworkBandwidth)
    {
        NProto::TDiskAgentConfig proto;
        auto* throttling = proto.MutableThrottlingConfig();
        throttling->SetDefaultNetworkMbitThroughput(100);   // 12.5 MiB
        throttling->SetDirectCopyBandwidthFraction(0.6);

        TDiskAgentConfig config(
            std::move(proto),
            "rack",
            800   // 100 MiB
        );
        TBandwidthCalculator bandwidthCalculator(config);

        // One client got all the bandwidth
        UNIT_ASSERT_VALUES_EQUAL(
            60_MB,
            bandwidthCalculator.RegisterRequest("device_1", TInstant::Now()));
        UNIT_ASSERT_VALUES_EQUAL(
            60_MB,
            bandwidthCalculator.RegisterRequest("device_1", TInstant::Now()));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
