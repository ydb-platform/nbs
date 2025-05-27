#include "node.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sleeper_test.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

using ::testing::_;
using ::testing::Return;

namespace NCloud::NStorage {

namespace {

ui32 DefaultNodeId = 123U;
NActors::TScopeId DefaultScopeId{4UL, 5UL};

struct TMockRegistrant: public INodeRegistrant
{
    MOCK_METHOD(
        TResultOrError<TRegistrationResult>,
        TryRegister,
        (const TString&));
    MOCK_METHOD(
        TResultOrError<NKikimrConfig::TAppConfig>,
        TryConfigure,
        (const TString&, ui32));
};

////////////////////////////////////////////////////////////////////////////////

TRegisterDynamicNodeOptions CreateRegisterOptions(bool loadConfigs)
{
    return {
        .NodeBrokerAddress = "NodeBrokerAddressTest",
        .LoadCmsConfigs = loadConfigs,
        .Settings =
            {
                .MaxAttempts = 3,
                .ErrorTimeout = TDuration::Seconds(1),
                .LoadConfigsFromCmsRetryMinDelay = TDuration::Seconds(1),
                .LoadConfigsFromCmsRetryMaxDelay = TDuration::Seconds(8),
                .LoadConfigsFromCmsTotalTimeout = TDuration::Seconds(14),
            },
    };
}

decltype(auto) CreateTestEnvironment()
{
    auto timer = std::make_shared<TTestTimer>();
    auto sleeper = std::make_shared<TTestSleeper>(timer);
    auto log = TLog{};
    auto appConfig = std::make_shared<NKikimrConfig::TAppConfig>();
    return std::tuple{timer, sleeper, log, appConfig};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRegisterDynamicNodeTest)
{
    Y_UNIT_TEST(ShouldSucceedOnFirstAttempt)
    {
        auto registrant = std::make_unique<TMockRegistrant>();
        auto& registrantRef = *registrant;

        EXPECT_CALL(registrantRef, TryRegister(_))
            .WillOnce(
                Return(TRegistrationResult{DefaultNodeId, DefaultScopeId}));

        EXPECT_CALL(registrantRef, TryConfigure(_, DefaultNodeId))
            .WillOnce(Return(NKikimrConfig::TAppConfig{}));

        TRegisterDynamicNodeOptions options = CreateRegisterOptions(true);

        auto [timer, sleeper, log, appConfig] = CreateTestEnvironment();

        auto [nodeId, scopeId, maybeConfig] = RegisterDynamicNode(
            appConfig,
            options,
            std::move(registrant),
            log,
            timer,
            sleeper);

        ASSERT_EQ(nodeId, DefaultNodeId);
        ASSERT_EQ(scopeId, DefaultScopeId);
        ASSERT_TRUE(maybeConfig);
    }

    Y_UNIT_TEST(ShouldRetryRegistrationAndSucceedConfiguration)
    {
        auto registrant = std::make_unique<TMockRegistrant>();
        auto& registrantRef = *registrant;

        EXPECT_CALL(registrantRef, TryRegister(_))
            .WillOnce(Return(MakeError(E_FAIL, "Registration failed")))
            .WillOnce(Return(MakeError(E_FAIL, "Registration failed")))
            .WillOnce(
                Return(TRegistrationResult{DefaultNodeId, DefaultScopeId}));

        EXPECT_CALL(registrantRef, TryConfigure(_, DefaultNodeId))
            .WillOnce(Return(NKikimrConfig::TAppConfig{}));

        TRegisterDynamicNodeOptions options = CreateRegisterOptions(true);

        auto [timer, sleeper, log, appConfig] = CreateTestEnvironment();

        auto [nodeId, scopeId, maybeConfig] = RegisterDynamicNode(
            appConfig,
            options,
            std::move(registrant),
            log,
            timer,
            sleeper);

        ASSERT_EQ(sleeper->SleepDurations.size(), 2UL);
        EXPECT_EQ(sleeper->SleepDurations[0], TDuration::Seconds(1));
        EXPECT_EQ(sleeper->SleepDurations[1], TDuration::Seconds(1));

        ASSERT_EQ(nodeId, DefaultNodeId);
        ASSERT_EQ(scopeId, (NActors::TScopeId{4UL, 5UL}));
        ASSERT_TRUE(maybeConfig);
    }

    Y_UNIT_TEST(ShouldRetryRegistrationUntilMaxAttempts)
    {
        auto registrant = std::make_unique<TMockRegistrant>();
        auto& registrantRef = *registrant;
        EXPECT_CALL(registrantRef, TryRegister(_))
            .Times(3)
            .WillRepeatedly(Return(MakeError(E_FAIL, "Registration failed")));

        TRegisterDynamicNodeOptions options = CreateRegisterOptions(false);

        auto [timer, sleeper, log, appConfig] = CreateTestEnvironment();

        EXPECT_THROW(
            RegisterDynamicNode(
                appConfig,
                options,
                std::move(registrant),
                log,
                timer,
                sleeper),
            TServiceError);

        ASSERT_EQ(sleeper->SleepDurations.size(), 2UL);
        EXPECT_EQ(sleeper->SleepDurations[0], TDuration::Seconds(1));
        EXPECT_EQ(sleeper->SleepDurations[1], TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ShouldRetryConfigurationWithExponentialBackoff)
    {
        auto registrant = std::make_unique<TMockRegistrant>();
        auto& registrantRef = *registrant;
        EXPECT_CALL(registrantRef, TryRegister(_))
            .WillOnce(
                Return(TRegistrationResult{DefaultNodeId, DefaultScopeId}));

        EXPECT_CALL(*registrant, TryConfigure(_, DefaultNodeId))
            .WillRepeatedly(Return(MakeError(E_FAIL, "Configure failed")));

        TRegisterDynamicNodeOptions options = CreateRegisterOptions(true);

        auto [timer, sleeper, log, appConfig] = CreateTestEnvironment();

        EXPECT_THROW(
            RegisterDynamicNode(
                appConfig,
                options,
                std::move(registrant),
                log,
                timer,
                sleeper),
            TServiceError);

        ASSERT_EQ(sleeper->SleepDurations.size(), 4UL);
        EXPECT_EQ(sleeper->SleepDurations[0], TDuration::Seconds(1));
        EXPECT_EQ(sleeper->SleepDurations[1], TDuration::Seconds(2));
        EXPECT_EQ(sleeper->SleepDurations[2], TDuration::Seconds(4));
        EXPECT_EQ(sleeper->SleepDurations[3], TDuration::Seconds(8));
    }
}

}   // namespace NCloud::NStorage
