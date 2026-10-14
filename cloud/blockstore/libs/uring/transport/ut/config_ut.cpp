#include <cloud/blockstore/libs/uring/transport/config.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NUring {

Y_UNIT_TEST_SUITE(TUringConfigTest)
{
    Y_UNIT_TEST(ShouldHaveDefaults)
    {
        TClientConfig c;
        UNIT_ASSERT_VALUES_EQUAL(2u, c.IoThreads);
        UNIT_ASSERT_VALUES_EQUAL(4096u, c.QueueSize);
        UNIT_ASSERT_VALUES_EQUAL(1024u, c.MaxInflightRequests);
        UNIT_ASSERT_VALUES_EQUAL(64ull << 20, c.MaxInflightBytes);
        UNIT_ASSERT_VALUES_EQUAL(64u << 10, c.MaxProtoSize);
        UNIT_ASSERT_VALUES_EQUAL(8u << 20, c.MaxDataSize);
        UNIT_ASSERT_VALUES_EQUAL(1u << 20, c.SendWindowBytes);
        UNIT_ASSERT(c.OobMode == EOobMode::Auto);
        UNIT_ASSERT_VALUES_EQUAL(128u << 10, c.OobThreshold);
        UNIT_ASSERT(c.SendZeroCopyMode == ESendZeroCopyMode::Auto);
        UNIT_ASSERT_VALUES_EQUAL(64u << 10, c.SendZeroCopyThreshold);
        UNIT_ASSERT(c.DirectSgIoEnabled);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(30), c.MaxResponseDelay);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(30), c.MaxReconnectDelay);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(30), c.TcpUserTimeout);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(5), c.KeepAliveIdle);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(5), c.KeepAliveInterval);
        UNIT_ASSERT_VALUES_EQUAL(3u, c.KeepAliveCount);

        TServerConfig s;
        UNIT_ASSERT_VALUES_EQUAL(128u, s.Backlog);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::Seconds(10), s.DataTokenTtl);
    }

    Y_UNIT_TEST(ShouldConvertFromProto)
    {
        NProto::TUringClient proto;
        proto.SetIoThreads(4);
        proto.SetOobMode(NProto::URING_OOB_REQUIRED);
        proto.SetSendZeroCopyMode(NProto::URING_SEND_ZC_OFF);
        proto.SetDirectSgIoEnabled(false);
        proto.SetMaxResponseDelay(1500);

        auto c = CreateClientConfig(proto);
        UNIT_ASSERT_VALUES_EQUAL(4u, c.IoThreads);
        UNIT_ASSERT_VALUES_EQUAL(4096u, c.QueueSize);   // untouched default
        UNIT_ASSERT(c.OobMode == EOobMode::Required);
        UNIT_ASSERT(c.SendZeroCopyMode == ESendZeroCopyMode::Off);
        UNIT_ASSERT(!c.DirectSgIoEnabled);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(1500), c.MaxResponseDelay);

        NProto::TUringServer sp;
        sp.SetBacklog(7);
        sp.SetDataTokenTtl(250);
        auto s = CreateServerConfig(sp);
        UNIT_ASSERT_VALUES_EQUAL(7u, s.Backlog);
        UNIT_ASSERT_VALUES_EQUAL(TDuration::MilliSeconds(250), s.DataTokenTtl);
        UNIT_ASSERT(s.OobMode == EOobMode::Auto);
    }
}

}   // namespace NCloud::NBlockStore::NUring
