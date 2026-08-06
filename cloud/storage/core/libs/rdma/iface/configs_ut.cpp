#include "config.h"

#include <cloud/storage/core/libs/common/helpers.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <util/generic/singleton.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NCloud::NStorage::NRdma {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TClientConfigTraits
{
    using TProto = NProto::TRdmaClient;
    using TConfig = TClientConfig;

    static constexpr ui32 DefaultQueueSize = 10;

    static TConfig Make(const TProto& proto)
    {
        return CreateClientConfig(proto);
    }
};

struct TServerConfigTraits
{
    using TProto = NProto::TRdmaServer;
    using TConfig = TServerConfig;

    static constexpr ui32 DefaultQueueSize = 10;

    static TConfig Make(const TProto& proto)
    {
        return CreateServerConfig(proto);
    }
};

template <typename TTraits>
class TQueueSizeCompatibilityTest: public ::testing::Test
{
};

using TQueueSizeCompatibilityTestTypes =
    ::testing::Types<TClientConfigTraits, TServerConfigTraits>;

TYPED_TEST_SUITE(TQueueSizeCompatibilityTest, TQueueSizeCompatibilityTestTypes);

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST(TQueueSizeCompatibilityTest, ShouldUseQueueSizeWhenSendAndRecvNotSet)
{
    using TTraits = TypeParam;
    using TProto = typename TTraits::TProto;

    TProto proto;
    proto.SetQueueSize(128);

    const auto config = TTraits::Make(proto);

    EXPECT_EQ(128u, config.QueueSize);
    EXPECT_EQ(128u, config.SendQueueSize);
    EXPECT_EQ(128u, config.RecvQueueSize);
}

TYPED_TEST(TQueueSizeCompatibilityTest, ShouldNotOverrideExplicitSendQueueSize)
{
    using TTraits = TypeParam;
    using TProto = typename TTraits::TProto;

    TProto proto;
    proto.SetQueueSize(128);
    proto.SetSendQueueSize(64);

    const auto config = TTraits::Make(proto);

    EXPECT_EQ(128u, config.QueueSize);
    EXPECT_EQ(64u, config.SendQueueSize);
    EXPECT_EQ(128u, config.RecvQueueSize);
}

TYPED_TEST(TQueueSizeCompatibilityTest, ShouldNotOverrideExplicitRecvQueueSize)
{
    using TTraits = TypeParam;
    using TProto = typename TTraits::TProto;

    TProto proto;
    proto.SetQueueSize(128);
    proto.SetRecvQueueSize(32);

    const auto config = TTraits::Make(proto);

    EXPECT_EQ(128u, config.QueueSize);
    EXPECT_EQ(128u, config.SendQueueSize);
    EXPECT_EQ(32u, config.RecvQueueSize);
}

TYPED_TEST(
    TQueueSizeCompatibilityTest,
    ShouldNotOverrideExplicitSendAndRecvQueueSize)
{
    using TTraits = TypeParam;
    using TProto = typename TTraits::TProto;

    TProto proto;
    proto.SetQueueSize(128);
    proto.SetSendQueueSize(64);
    proto.SetRecvQueueSize(32);

    const auto config = TTraits::Make(proto);

    EXPECT_EQ(128u, config.QueueSize);
    EXPECT_EQ(64u, config.SendQueueSize);
    EXPECT_EQ(32u, config.RecvQueueSize);
}

TYPED_TEST(
    TQueueSizeCompatibilityTest,
    ShouldDeriveSendAndRecvQueueSizeFromDefaultQueueSize)
{
    using TTraits = TypeParam;
    using TProto = typename TTraits::TProto;

    TProto proto;
    const auto config = TTraits::Make(proto);

    EXPECT_EQ(TTraits::DefaultQueueSize, config.QueueSize);
    EXPECT_EQ(TTraits::DefaultQueueSize, config.SendQueueSize);
    EXPECT_EQ(TTraits::DefaultQueueSize, config.RecvQueueSize);
}

}   // namespace NCloud::NStorage::NRdma
