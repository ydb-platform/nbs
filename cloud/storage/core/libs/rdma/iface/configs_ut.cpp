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

    static TConfig Default;

    static TConfig Make(const TProto& proto)
    {
        return CreateClientConfig(proto);
    }
};

struct TServerConfigTraits
{
    using TProto = NProto::TRdmaServer;
    using TConfig = TServerConfig;

    static TConfig Default;

    static TConfig Make(const TProto& proto)
    {
        return CreateServerConfig(proto);
    }
};

TClientConfig TClientConfigTraits::Default;
TServerConfig TServerConfigTraits::Default;

template <typename TTraits>
class TQueueSizeCompatibilityTest: public ::testing::Test
{
};

template <typename TTraits>
class TOptionalFieldsTest: public ::testing::Test
{
};

using TQueueSizeCompatibilityTestTypes =
    ::testing::Types<TClientConfigTraits, TServerConfigTraits>;

using TOptionalFieldsTestTypes =
    ::testing::Types<TClientConfigTraits, TServerConfigTraits>;

TYPED_TEST_SUITE(TQueueSizeCompatibilityTest, TQueueSizeCompatibilityTestTypes);
TYPED_TEST_SUITE(TOptionalFieldsTest, TOptionalFieldsTestTypes);

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

    EXPECT_EQ(TTraits::Default.QueueSize, config.QueueSize);
    EXPECT_EQ(TTraits::Default.QueueSize, config.SendQueueSize);
    EXPECT_EQ(TTraits::Default.QueueSize, config.RecvQueueSize);
}

TYPED_TEST(TOptionalFieldsTest, ShouldUseDefaultIfOptionalIsNotSet)
{
    using TTraits = TypeParam;
    using TProto = typename TTraits::TProto;

    TProto proto;
    const auto config = TTraits::Make(proto);

    EXPECT_EQ(TTraits::Default.QpTimeout, config.QpTimeout);
    EXPECT_EQ(TTraits::Default.QpRetryCount, config.QpRetryCount);
    EXPECT_EQ(TTraits::Default.QpMinRnrTimer, config.QpMinRnrTimer);
    EXPECT_EQ(TTraits::Default.QpRnrRetryCount, config.QpRnrRetryCount);
}

TYPED_TEST(TOptionalFieldsTest, ShouldSetZeroValue)
{
    using TTraits = TypeParam;
    using TProto = typename TTraits::TProto;

    TProto proto;
    proto.SetQpRetryCount(0);
    proto.SetQpRnrRetryCount(0);
    proto.SetQpTimeout(0);
    proto.SetQpMinRnrTimer(0);
    const auto config = TTraits::Make(proto);

    EXPECT_EQ(0, config.QpTimeout);
    EXPECT_EQ(0, config.QpRetryCount);
    EXPECT_EQ(0, config.QpMinRnrTimer);
    EXPECT_EQ(0, config.QpRnrRetryCount);
}

}   // namespace NCloud::NStorage::NRdma
