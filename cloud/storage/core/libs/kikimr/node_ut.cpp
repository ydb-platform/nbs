#include "node.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/bootstrap_monitoring/bootstrap_monitoring.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/testing/common/network.h>

#include <library/cpp/testing/gmock_in_unittest/gmock.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/stream/str.h>

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
    Y_UNIT_TEST_F(ShouldServeMetricsDuringRegistrationAndConfigRetries, TFixture)
    {
        const auto port = NTesting::GetFreePort();
        NKikimrConfig::TMonitoringConfig config;
        config.SetMonitoringAddress("127.0.0.1");
        config.SetMonitoringPort(port);
        auto monitoring = CreateBootstrapMonitoring(config, "blockstore");
        monitoring->Start();

        auto scrape = [&] {
            TKeepAliveHttpClient client("127.0.0.1", port);
            TStringStream response;
            UNIT_ASSERT_VALUES_EQUAL(200, client.DoGet(
                "/counters/counters=blockstore/json", &response));
            UNIT_ASSERT(response.Str().Contains("Initializing"));
        };

        auto registrant = std::make_unique<TMockRegistrant>();
        ui32 registrations = 0;
        ui32 configurations = 0;
        EXPECT_CALL(*registrant, RegisterNode(_)).Times(2).WillRepeatedly(
            [&] (const TString&) -> TResultOrError<TRegistrationResult> {
                scrape();
                if (++registrations == 1) {
                    return MakeError(E_FAIL, "Node broker unavailable");
                }
                return TRegistrationResult{DefaultNodeId, DefaultScopeId};
            });
        EXPECT_CALL(*registrant, GetConfigs(_, DefaultNodeId))
            .Times(2).WillRepeatedly(
                [&] (const TString&, ui32) -> TResultOrError<NKikimrConfig::TAppConfig> {
                    scrape();
                    if (++configurations == 1) {
                        return MakeError(E_FAIL, "CMS unavailable");
                    }
                    return NKikimrConfig::TAppConfig{};
                });

        RegisterDynamicNode(
            AppConfig,
            CreateRegisterOptions(true),
            std::move(registrant),
            Log,
            Timer);
        monitoring->Stop();
    }

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

        ASSERT_EQ(DefaultNodeId, nodeId);
        ASSERT_EQ(DefaultScopeId, scopeId);
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

        ASSERT_EQ(2UL, sleepDurations.size());
        EXPECT_EQ(1s, sleepDurations[0]);
        EXPECT_EQ(1s, sleepDurations[1]);

        ASSERT_EQ(DefaultNodeId, nodeId);
        ASSERT_EQ(DefaultScopeId, scopeId);
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

        ASSERT_EQ(2UL, sleepDurations.size());
        EXPECT_EQ(1s, sleepDurations[0]);
        EXPECT_EQ(1s, sleepDurations[1]);
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

        ASSERT_EQ(4UL, sleepDurations.size());
        EXPECT_EQ(1s, sleepDurations[0]);
        EXPECT_EQ(2s, sleepDurations[1]);
        EXPECT_EQ(4s, sleepDurations[2]);
        EXPECT_EQ(8s, sleepDurations[3]);
    }
}

}   // namespace NCloud::NStorage
