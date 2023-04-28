#include "agent_counters.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

auto CreateDevice(TString id)
{
    NProto::TDeviceConfig config;
    config.SetDeviceUUID(std::move(id));

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
        auto diskRegistryGroup = monitoring->GetCounters()
            ->GetSubgroup("counters", "blockstore")
            ->GetSubgroup("component", "disk_registry");

        NProto::TAgentConfig agentConfig;
        agentConfig.SetAgentId("foo");
        *agentConfig.AddDevices() = CreateDevice("uuid-1");
        *agentConfig.AddDevices() = CreateDevice("uuid-2");

        agentCounters.Register(agentConfig, diskRegistryGroup);

        auto agentGroup = diskRegistryGroup->FindSubgroup(
            "agent",
            agentConfig.GetAgentId());

        UNIT_ASSERT(agentGroup);

        auto d1Group = agentGroup->GetSubgroup("device", "uuid-1");
        auto d2Group = agentGroup->GetSubgroup("device", "uuid-2");

        auto d1ReadCount = d1Group->GetCounter("ReadCount");
        auto d1ReadBytes = d1Group->GetCounter("ReadBytes");
        auto d1WriteCount = d1Group->GetCounter("WriteCount");
        auto d1WriteBytes = d1Group->GetCounter("WriteBytes");
        auto d1ZeroCount = d1Group->GetCounter("ZeroCount");
        auto d1ZeroBytes = d1Group->GetCounter("ZeroBytes");

        auto d2ReadCount = d2Group->GetCounter("ReadCount");
        auto d2ReadBytes = d2Group->GetCounter("ReadBytes");
        auto d2WriteCount = d2Group->GetCounter("WriteCount");
        auto d2WriteBytes = d2Group->GetCounter("WriteBytes");
        auto d2ZeroCount = d2Group->GetCounter("ZeroCount");
        auto d2ZeroBytes = d2Group->GetCounter("ZeroBytes");

        auto mtbf = agentGroup->GetCounter("MeanTimeBetweenFailures");

        UNIT_ASSERT_VALUES_EQUAL(0, d1ReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1ReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1WriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1WriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1ZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1ZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, d2ReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2ReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2WriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2WriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2ZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2ZeroBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, mtbf->Val());

        NProto::TMeanTimeBetweenFailures tempMtbf;
        tempMtbf.SetWorkTime(100);
        tempMtbf.SetBrokenCount(2);
        agentCounters.Update([] {
            NProto::TAgentStats stats;

            {
                auto* d = stats.AddDeviceStats();
                d->SetDeviceUUID("uuid-1");

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

                d->SetNumReadOps(2);
                d->SetBytesRead(2000);

                d->SetNumWriteOps(20);
                d->SetBytesWritten(2000);
            }

            return stats;
        }(), tempMtbf);

        UNIT_ASSERT_VALUES_EQUAL(0, d1ReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1ReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1WriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1WriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1ZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d1ZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(0, d2ReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2ReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2WriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2WriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2ZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2ZeroBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(50, mtbf->Val());

        agentCounters.Publish(TInstant::Hours(1));

        UNIT_ASSERT_VALUES_EQUAL(1, d1ReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1000, d1ReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10, d1WriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1000, d1WriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(100, d1ZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1000, d1ZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(2, d2ReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2000, d2ReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(20, d2WriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2000, d2WriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2ZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, d2ZeroBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(50, mtbf->Val());

        agentCounters.Update([] {
            NProto::TAgentStats stats;

            auto* d = stats.AddDeviceStats();
            d->SetDeviceUUID("uuid-1");

            d->SetNumReadOps(1);
            d->SetBytesRead(1000);

            d->SetNumZeroOps(10);
            d->SetBytesZeroed(100);

            return stats;
        }(), {});

        agentCounters.Update([] {
            NProto::TAgentStats stats;

            auto* d = stats.AddDeviceStats();
            d->SetDeviceUUID("uuid-2");

            d->SetNumWriteOps(20);
            d->SetBytesWritten(2000);

            d->SetNumZeroOps(200);
            d->SetBytesZeroed(2000);

            return stats;
        }(), {});

        agentCounters.Publish(TInstant::Hours(2));

        UNIT_ASSERT_VALUES_EQUAL(2, d1ReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2000, d1ReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(10, d1WriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1000, d1WriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(110, d1ZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(1100, d1ZeroBytes->Val());

        UNIT_ASSERT_VALUES_EQUAL(2, d2ReadCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2000, d2ReadBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(40, d2WriteCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(4000, d2WriteBytes->Val());
        UNIT_ASSERT_VALUES_EQUAL(200, d2ZeroCount->Val());
        UNIT_ASSERT_VALUES_EQUAL(2000, d2ZeroBytes->Val());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
