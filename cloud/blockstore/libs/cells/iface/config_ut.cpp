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
        UNIT_ASSERT(!config.GetEnableXdc());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(5),
            config.GetHandshakeTimeout());
        UNIT_ASSERT(!config.GetChecksumEvents());
        UNIT_ASSERT_VALUES_EQUAL(128, config.GetMaxIncomingConnections());
        UNIT_ASSERT_VALUES_EQUAL(128, config.GetMaxOutgoingConnections());
        UNIT_ASSERT_VALUES_EQUAL(1024, config.GetMaxOutgoingInflightRequests());
        UNIT_ASSERT_VALUES_EQUAL(64_MB, config.GetMaxOutgoingInflightBytes());
        UNIT_ASSERT_VALUES_EQUAL(1024, config.GetMaxIncomingInflightRequests());
        UNIT_ASSERT_VALUES_EQUAL(1_GB, config.GetMaxIncomingInflightBytes());
        UNIT_ASSERT_VALUES_EQUAL(1_GB, config.GetMaxTotalOutputQueueBytes());
        UNIT_ASSERT_VALUES_EQUAL(1, config.GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(2, config.GetCompletionThreads());
        UNIT_ASSERT_VALUES_EQUAL(1, config.GetDataWorkers());
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
        icV2.SetHandshakeTimeout(0);
        icV2.SetMaxIncomingConnections(0);
        icV2.SetMaxOutgoingConnections(0);
        icV2.SetMaxOutgoingInflightRequests(0);
        icV2.SetMaxOutgoingInflightBytes(0);
        icV2.SetMaxIncomingInflightRequests(0);
        icV2.SetMaxIncomingInflightBytes(0);
        icV2.SetMaxTotalOutputQueueBytes(0);
        icV2.SetThreads(0);
        icV2.SetCompletionThreads(0);
        icV2.SetDataWorkers(0);

        const TCellsConfig cells(std::move(proto));
        const auto& config = cells.GetIcV2Config();
        UNIT_ASSERT_VALUES_EQUAL("", config.GetListenAddress());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetPort());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Zero(),
            config.GetHandshakeTimeout());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxIncomingConnections());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxOutgoingConnections());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxOutgoingInflightRequests());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxOutgoingInflightBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxIncomingInflightRequests());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxIncomingInflightBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetMaxTotalOutputQueueBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetThreads());
        UNIT_ASSERT_VALUES_EQUAL(0, config.GetCompletionThreads());
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
        icV2.SetEnableXdc(true);
        icV2.SetHandshakeTimeout(1500);
        icV2.SetChecksumEvents(true);
        icV2.SetMaxOutgoingInflightRequests(128);
        icV2.SetMaxOutgoingInflightBytes(32_MB);
        icV2.SetMaxIncomingInflightRequests(2048);
        icV2.SetMaxIncomingInflightBytes(512_MB);
        icV2.SetThreads(3);

        const TCellsConfig cells(std::move(proto));
        const auto& config = cells.GetIcV2Config();
        UNIT_ASSERT(config.GetEnabled());
        UNIT_ASSERT_VALUES_EQUAL("127.0.0.1", config.GetListenAddress());
        UNIT_ASSERT_VALUES_EQUAL(1234, config.GetPort());
        UNIT_ASSERT(config.GetEnableXdc());
        UNIT_ASSERT(config.GetChecksumEvents());
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::MilliSeconds(1500),
            config.GetHandshakeTimeout());
        UNIT_ASSERT_VALUES_EQUAL(128, config.GetMaxOutgoingInflightRequests());
        UNIT_ASSERT_VALUES_EQUAL(32_MB, config.GetMaxOutgoingInflightBytes());
        UNIT_ASSERT_VALUES_EQUAL(2048, config.GetMaxIncomingInflightRequests());
        UNIT_ASSERT_VALUES_EQUAL(512_MB, config.GetMaxIncomingInflightBytes());
        UNIT_ASSERT_VALUES_EQUAL(3, config.GetThreads());
    }
}

}   // namespace NCloud::NBlockStore::NCells
