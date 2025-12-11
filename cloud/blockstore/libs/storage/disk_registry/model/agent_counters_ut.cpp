#include "agent_counters.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

auto CreateDevice(TString id, TString name)
{
    NProto::TDeviceConfig config;
    config.SetDeviceUUID(std::move(id));
    config.SetDeviceName(std::move(name));

    return config;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TAgentCountersTest)
{
    Y_UNIT_TEST(ShouldUpdateCounters)
    {
        TAgentCounters agentCounters;

        auto monitoring = CreateMonitoringServiceStub();
        auto diskRegistryGroup =
            monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "disk_registry");

        NProto::TAgentConfig agentConfig;
        agentConfig.SetAgentId("foo");
        *agentConfig.AddDevices() = CreateDevice("uuid-1", "dev-a");
        *agentConfig.AddDevices() = CreateDevice("uuid-2", "dev-a");
        *agentConfig.AddDevices() = CreateDevice("uuid-3", "dev-b");
        *agentConfig.AddDevices() = CreateDevice("uuid-4", "dev-b");

        agentCounters.Register(agentConfig, diskRegistryGroup);

        auto agentGroup =
            diskRegistryGroup->FindSubgroup("agent", agentConfig.GetAgentId());

        UNIT_ASSERT(agentGroup);

        auto daGroup = agentGroup->GetSubgroup("device", "foo:dev-a");
        auto dbGroup = agentGroup->GetSubgroup("device", "foo:dev-b");

        auto daReadCount = daGroup->GetCounter("ReadCount");
        auto daReadBytes = daGroup->GetCounter("ReadBytes");
        auto daWriteCount = daGroup->GetCounter("WriteCount");
        auto daWriteBytes = daGroup->GetCounter("WriteBytes");
        auto daZeroCount = daGroup->GetCounter("ZeroCount");
        auto daZeroBytes = daGroup->GetCounter("ZeroBytes");

        auto dbReadCount = dbGroup->GetCounter("ReadCount");
        auto dbReadBytes = dbGroup->GetCounter("ReadBytes");
        auto dbWriteCount = dbGroup->GetCounter("WriteCount");
        auto dbWriteBytes = dbGroup->GetCounter("WriteBytes");
        auto dbZeroCount = dbGroup->GetCounter("ZeroCount");
        auto dbZeroBytes = dbGroup->GetCounter("ZeroBytes");

        auto mtbf = agentGroup->GetCounter("MeanTimeBetweenFailures");

        UNIT_ASSERT_VALUES_EQUAL(0, daReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, dbReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbZeroBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, mtbf->Val());

        NProto::TMeanTimeBetweenFailures tempMtbf;
        tempMtbf.SetWorkTime(100);
        tempMtbf.SetBrokenCount(2);
        agentCounters.Update(
            "foo",
            []
            {
                NProto::TAgentStats stats;

                {
                    auto* d = stats.AddDeviceStats();
                    d->SetDeviceUUID("uuid-1");
                    d->SetDeviceName("dev-a");

                    d->SetNumReadOps(1);
                    d->SetBytesRead(1000);

                    d->SetNumWriteOps(10);
                    d->SetBytesWritten(1000);

                    d->SetNumZeroOps(100);
                    d->SetBytesZeroed(1000);
                }

                {
                    auto* d = stats.AddDeviceStats();
                    d->SetDeviceUUID("uuid-2");
                    d->SetDeviceName("dev-a");

                    d->SetNumReadOps(2);
                    d->SetBytesRead(2000);

                    d->SetNumWriteOps(20);
                    d->SetBytesWritten(2000);
                }

                {
                    auto* d = stats.AddDeviceStats();
                    d->SetDeviceUUID("uuid-3");
                    d->SetDeviceName("dev-b");

                    d->SetNumReadOps(4);
                    d->SetBytesRead(4000);

                    d->SetNumWriteOps(15);
                    d->SetBytesWritten(1500);
                }

                return stats;
            }(),
            tempMtbf);

        UNIT_ASSERT_VALUES_EQUAL(0, daReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, daZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, dbReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbZeroBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(50, mtbf->Val());

        agentCounters.Publish(TInstant::Hours(1));

        UNIT_ASSERT_VALUES_EQUAL(3, daReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(3000, daReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(30, daWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(3000, daWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(100, daZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1000, daZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(4, dbReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(4000, dbReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(15, dbWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1500, dbWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, dbZeroBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(50, mtbf->Val());

        agentCounters.Update(
            "foo",
            []
            {
                NProto::TAgentStats stats;

                auto* d = stats.AddDeviceStats();
                d->SetDeviceUUID("uuid-1");
                d->SetDeviceName("dev-a");

                d->SetNumReadOps(1);
                d->SetBytesRead(1000);

                d->SetNumZeroOps(10);
                d->SetBytesZeroed(100);

                return stats;
            }(),
            {});

        agentCounters.Update(
            "foo",
            []
            {
                NProto::TAgentStats stats;

                auto* d = stats.AddDeviceStats();
                d->SetDeviceUUID("uuid-4");
                d->SetDeviceName("dev-b");

                d->SetNumWriteOps(20);
                d->SetBytesWritten(2000);

                d->SetNumZeroOps(200);
                d->SetBytesZeroed(2000);

                return stats;
            }(),
            {});

        agentCounters.Publish(TInstant::Hours(2));

        UNIT_ASSERT_VALUES_EQUAL(4, daReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(4000, daReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(30, daWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(3000, daWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(110, daZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1100, daZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(4, dbReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(4000, dbReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(35, dbWriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(3500, dbWriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(200, dbZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2000, dbZeroBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, mtbf->Val());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
