#include "config.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TCellsIcV2ConfigTest)
{
    Y_UNIT_TEST(ShouldApplyDefaultsWithoutSettingProtoFields)
    {
        const TCellsConfig cells;
        const auto& config = cells.GetIcV2Config();

        UNIT_ASSERT(!config.GetEnabled());
        UNIT_ASSERT_VALUES_EQUAL(config.GetListenAddress(), "::");
        UNIT_ASSERT_VALUES_EQUAL(config.GetPort(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetThreads(), 1);
        UNIT_ASSERT_VALUES_EQUAL(config.GetHandshakeTimeoutMs(), 5000);
        UNIT_ASSERT(!config.GetChecksumEvents());
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxConnections(), 128);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxPendingRequests(), 1024);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxOutputQueueBytes(), 64_MB);
        UNIT_ASSERT_VALUES_EQUAL(config.GetCompletionThreads(), 2);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxOutgoingConnections(), 128);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxDataRequestBytes(), 4_MB);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxDataInflightBytes(), 64_MB);
        UNIT_ASSERT_VALUES_EQUAL(config.GetDataWorkers(), 1);
        UNIT_ASSERT(!config.GetEnableXdc());
        UNIT_ASSERT(!cells.GetCellsConfig().HasIcV2());
        UNIT_ASSERT(!config.GetIcV2Config().HasPort());
        UNIT_ASSERT(!config.GetIcV2Config().HasThreads());
    }

    Y_UNIT_TEST(ShouldPreserveExplicitZeroValues)
    {
        NProto::TCellsConfig proto;
        auto& icV2 = *proto.MutableIcV2();
        icV2.SetListenAddress("");
        icV2.SetPort(0);
        icV2.SetThreads(0);
        icV2.SetHandshakeTimeoutMs(0);
        icV2.SetMaxConnections(0);
        icV2.SetMaxPendingRequests(0);
        icV2.SetMaxOutputQueueBytes(0);
        icV2.SetCompletionThreads(0);
        icV2.SetMaxOutgoingConnections(0);
        icV2.SetMaxDataRequestBytes(0);
        icV2.SetMaxDataInflightBytes(0);
        icV2.SetDataWorkers(0);

        const TCellsConfig cells(std::move(proto));
        const auto& config = cells.GetIcV2Config();
        UNIT_ASSERT_VALUES_EQUAL(config.GetListenAddress(), "");
        UNIT_ASSERT_VALUES_EQUAL(config.GetPort(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetThreads(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetHandshakeTimeoutMs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxConnections(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxPendingRequests(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxOutputQueueBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetCompletionThreads(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxOutgoingConnections(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxDataRequestBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxDataInflightBytes(), 0);
        UNIT_ASSERT_VALUES_EQUAL(config.GetDataWorkers(), 0);
        UNIT_ASSERT(config.GetIcV2Config().HasPort());
    }

    Y_UNIT_TEST(ShouldUseOverridesAndDefaultUnsetFields)
    {
        NProto::TCellsConfig proto;
        auto& icV2 = *proto.MutableIcV2();
        icV2.SetEnabled(true);
        icV2.SetListenAddress("127.0.0.1");
        icV2.SetPort(1234);
        icV2.SetThreads(3);
        icV2.SetChecksumEvents(true);
        icV2.SetEnableXdc(true);

        const TCellsConfig cells(std::move(proto));
        const auto& config = cells.GetIcV2Config();
        UNIT_ASSERT(config.GetEnabled());
        UNIT_ASSERT_VALUES_EQUAL(config.GetListenAddress(), "127.0.0.1");
        UNIT_ASSERT_VALUES_EQUAL(config.GetPort(), 1234);
        UNIT_ASSERT_VALUES_EQUAL(config.GetThreads(), 3);
        UNIT_ASSERT(config.GetChecksumEvents());
        UNIT_ASSERT(config.GetEnableXdc());
        UNIT_ASSERT_VALUES_EQUAL(config.GetHandshakeTimeoutMs(), 5000);
        UNIT_ASSERT_VALUES_EQUAL(config.GetMaxDataRequestBytes(), 4_MB);
    }
}

}   // namespace NCloud::NBlockStore::NCells
