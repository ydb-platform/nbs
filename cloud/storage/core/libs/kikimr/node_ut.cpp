#include "node.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <chrono>

using ::testing::_;
using ::testing::Return;

namespace NCloud::NStorage {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

using TRegistrationResult = INodeRegistrant::TRegistrationResult;

////////////////////////////////////////////////////////////////////////////////

struct TMockRegistrant: public INodeRegistrant
{
    MOCK_METHOD(
        TResultOrError<TRegistrationResult>,
        RegisterNode,
        (const TString&));
    MOCK_METHOD(
        TResultOrError<NKikimrConfig::TAppConfig>,
        GetConfigs,
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
                .ErrorTimeout = 1s,
                .LoadConfigsFromCmsRetryMinDelay = 1s,
                .LoadConfigsFromCmsRetryMaxDelay = 8s,
                .LoadConfigsFromCmsTotalTimeout = 14s,
            },
    };
}

////////////////////////////////////////////////////////////////////////////////

struct TFixture: public NUnitTest::TBaseFixture
{
    const ui32 DefaultNodeId = 123U;
    const NActors::TScopeId DefaultScopeId{4UL, 5UL};

    std::shared_ptr<TTestTimer> Timer;
    TLog Log;
    NKikimrConfig::TAppConfigPtr AppConfig;

    void SetUp(NUnitTest::TTestContext& /*testContext*/) override
    {
        Timer = std::make_shared<TTestTimer>();
        AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRegisterDynamicNodeTest)
{
    Y_UNIT_TEST_F(ShouldSucceedOnFirstAttempt, TFixture)
    {
        auto registrant = std::make_unique<TMockRegistrant>();
        auto& registrantRef = *registrant;

        EXPECT_CALL(registrantRef, RegisterNode(_))
            .WillOnce(
                Return(TRegistrationResult{DefaultNodeId, DefaultScopeId}));

        EXPECT_CALL(registrantRef, GetConfigs(_, DefaultNodeId))
            .WillOnce(Return(NKikimrConfig::TAppConfig{}));

        TRegisterDynamicNodeOptions options = CreateRegisterOptions(true);

        auto [nodeId, scopeId, maybeConfig] = RegisterDynamicNode(
            AppConfig,
            options,
            std::move(registrant),
            Log,
            Timer);

        ASSERT_EQ(nodeId, DefaultNodeId);
        ASSERT_EQ(scopeId, DefaultScopeId);
        ASSERT_TRUE(maybeConfig);
    }

    Y_UNIT_TEST_F(ShouldRetryRegistrationAndSucceedConfiguration, TFixture)
    {
        auto registrant = std::make_unique<TMockRegistrant>();
        auto& registrantRef = *registrant;

        EXPECT_CALL(registrantRef, RegisterNode(_))
            .WillOnce(Return(MakeError(E_FAIL, "Registration failed")))
            .WillOnce(Return(MakeError(E_FAIL, "Registration failed")))
            .WillOnce(
                Return(TRegistrationResult{DefaultNodeId, DefaultScopeId}));

        EXPECT_CALL(registrantRef, GetConfigs(_, DefaultNodeId))
            .WillOnce(Return(NKikimrConfig::TAppConfig{}));

        TRegisterDynamicNodeOptions options = CreateRegisterOptions(true);

        auto [nodeId, scopeId, maybeConfig] = RegisterDynamicNode(
            AppConfig,
            options,
            std::move(registrant),
            Log,
            Timer);

        const auto& sleepDurations = Timer->GetSleepDurations();

        ASSERT_EQ(sleepDurations.size(), 2UL);
        EXPECT_EQ(sleepDurations[0], 1s);
        EXPECT_EQ(sleepDurations[1], 1s);

        ASSERT_EQ(nodeId, DefaultNodeId);
        ASSERT_EQ(scopeId, DefaultScopeId);
        ASSERT_TRUE(maybeConfig);
    }

    Y_UNIT_TEST_F(ShouldRetryRegistrationUntilMaxAttempts, TFixture)
    {
        auto registrant = std::make_unique<TMockRegistrant>();
        auto& registrantRef = *registrant;
        EXPECT_CALL(registrantRef, RegisterNode(_))
            .Times(3)
            .WillRepeatedly(Return(MakeError(E_FAIL, "Registration failed")));

        TRegisterDynamicNodeOptions options = CreateRegisterOptions(false);

        EXPECT_THROW(
            RegisterDynamicNode(
                AppConfig,
                options,
                std::move(registrant),
                Log,
                Timer),
            TServiceError);

        const auto& sleepDurations = Timer->GetSleepDurations();

        ASSERT_EQ(sleepDurations.size(), 2UL);
        EXPECT_EQ(sleepDurations[0], 1s);
        EXPECT_EQ(sleepDurations[1], 1s);
    }

    Y_UNIT_TEST_F(ShouldRetryConfigurationWithExponentialBackoff, TFixture)
    {
        auto registrant = std::make_unique<TMockRegistrant>();
        auto& registrantRef = *registrant;
        EXPECT_CALL(registrantRef, RegisterNode(_))
            .WillOnce(
                Return(TRegistrationResult{DefaultNodeId, DefaultScopeId}));

        EXPECT_CALL(registrantRef, GetConfigs(_, DefaultNodeId))
            .WillRepeatedly(Return(MakeError(E_FAIL, "Configure failed")));

        TRegisterDynamicNodeOptions options = CreateRegisterOptions(true);

        EXPECT_THROW(
            RegisterDynamicNode(
                AppConfig,
                options,
                std::move(registrant),
                Log,
                Timer),
            TServiceError);

        const auto& sleepDurations = Timer->GetSleepDurations();

        ASSERT_EQ(sleepDurations.size(), 4UL);
        EXPECT_EQ(sleepDurations[0], 1s);
        EXPECT_EQ(sleepDurations[1], 2s);
        EXPECT_EQ(sleepDurations[2], 4s);
        EXPECT_EQ(sleepDurations[3], 8s);
    }
}

}   // namespace NCloud::NStorage
