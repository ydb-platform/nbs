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
        UNIT_ASSERT_VALUES_EQUAL("::", config.GetListenAddress());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetPort());
        UNIT_ASSERT_VALUES_EQUAL(1, config.GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(5000, config.GetHandshakeTimeoutMs());
        UNIT_ASSERT(!config.GetChecksumEvents());
        UNIT_ASSERT_VALUES_EQUAL(128, config.GetMaxConnections());
        UNIT_ASSERT_VALUES_EQUAL(1024, config.GetMaxPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL(64_MB, config.GetMaxOutputQueueBytes());
        UNIT_ASSERT_VALUES_EQUAL(2, config.GetCompletionThreads());
        UNIT_ASSERT_VALUES_EQUAL(128, config.GetMaxOutgoingConnections());
        UNIT_ASSERT_VALUES_EQUAL(4_MB, config.GetMaxDataRequestBytes());
        UNIT_ASSERT_VALUES_EQUAL(64_MB, config.GetMaxDataInflightBytes());
        UNIT_ASSERT_VALUES_EQUAL(1, config.GetDataWorkers());
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
        UNIT_ASSERT_VALUES_EQUAL("", config.GetListenAddress());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetPort());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetHandshakeTimeoutMs());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxConnections());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxPendingRequests());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxOutputQueueBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetCompletionThreads());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxOutgoingConnections());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxDataRequestBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxDataInflightBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetDataWorkers());
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
        UNIT_ASSERT_VALUES_EQUAL("127.0.0.1", config.GetListenAddress());
        UNIT_ASSERT_VALUES_EQUAL(1234, config.GetPort());
        UNIT_ASSERT_VALUES_EQUAL(3, config.GetThreads());
        UNIT_ASSERT(config.GetChecksumEvents());
        UNIT_ASSERT(config.GetEnableXdc());
        UNIT_ASSERT_VALUES_EQUAL(5000, config.GetHandshakeTimeoutMs());
        UNIT_ASSERT_VALUES_EQUAL(4_MB, config.GetMaxDataRequestBytes());
    }
}

}   // namespace NCloud::NBlockStore::NCells
