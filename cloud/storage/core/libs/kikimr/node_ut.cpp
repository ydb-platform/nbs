#include "node.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/time_control_test.h>
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

////////////////////////////////////////////////////////////////////////////////

struct TTestEnvironment
{
    std::shared_ptr<TTestTimeControl> TimeControl;
    TLog Logger;
    NKikimrConfig::TAppConfigPtr AppConfig;
};

TTestEnvironment CreateTestEnvironment()
{
    auto timeControl = std::make_shared<TTestTimeControl>();
    auto log = TLog{};
    auto appConfig = std::make_shared<NKikimrConfig::TAppConfig>();
    return {.TimeControl = timeControl, .Logger = log, .AppConfig = appConfig};
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

        auto [timeControl, log, appConfig] = CreateTestEnvironment();

        auto [nodeId, scopeId, maybeConfig] = RegisterDynamicNode(
            appConfig,
            options,
            std::move(registrant),
            log,
            timeControl);

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

        auto [timeControl, log, appConfig] = CreateTestEnvironment();

        auto [nodeId, scopeId, maybeConfig] = RegisterDynamicNode(
            appConfig,
            options,
            std::move(registrant),
            log,
            timeControl);

        ASSERT_EQ(timeControl->SleepDurations.size(), 2UL);
        EXPECT_EQ(timeControl->SleepDurations[0], TDuration::Seconds(1));
        EXPECT_EQ(timeControl->SleepDurations[1], TDuration::Seconds(1));

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

        auto [timeControl, log, appConfig] = CreateTestEnvironment();

        EXPECT_THROW(
            RegisterDynamicNode(
                appConfig,
                options,
                std::move(registrant),
                log,
                timeControl),
            TServiceError);

        ASSERT_EQ(timeControl->SleepDurations.size(), 2UL);
        EXPECT_EQ(timeControl->SleepDurations[0], TDuration::Seconds(1));
        EXPECT_EQ(timeControl->SleepDurations[1], TDuration::Seconds(1));
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

        auto [timeControl, log, appConfig] = CreateTestEnvironment();

        EXPECT_THROW(
            RegisterDynamicNode(
                appConfig,
                options,
                std::move(registrant),
                log,
                timeControl),
            TServiceError);

        ASSERT_EQ(timeControl->SleepDurations.size(), 4UL);
        EXPECT_EQ(timeControl->SleepDurations[0], TDuration::Seconds(1));
        EXPECT_EQ(timeControl->SleepDurations[1], TDuration::Seconds(2));
        EXPECT_EQ(timeControl->SleepDurations[2], TDuration::Seconds(4));
        EXPECT_EQ(timeControl->SleepDurations[3], TDuration::Seconds(8));
    }
}

}   // namespace NCloud::NStorage
