#include "bootstrap_monitoring.h"

#include <contrib/ydb/core/protos/config.pb.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/stream/str.h>

namespace NCloud::NStorage {

namespace {

NKikimrConfig::TMonitoringConfig MakeConfig(ui16 port)
{
    NKikimrConfig::TMonitoringConfig config;
    config.SetMonitoringPort(port);
    config.SetMonitoringAddress("127.0.0.1");
    return config;
}

TString Scrape(ui16 port, const TString& path, bool secure = false)
{
    TKeepAliveHttpClient client(
        secure ? "https://127.0.0.1" : "127.0.0.1",
        port, TDuration::Seconds(5), TDuration::Seconds(5));
    if (secure) {
        client.DisableVerificationForHttps();
    }
    TStringStream response;
    UNIT_ASSERT_VALUES_EQUAL(200, client.DoGet(path, &response));
    return response.Str();
}

void CheckInitializing(ui16 port, const TString& service)
{
    const auto response =
        Scrape(port, "/counters/counters=" + service + "/json");
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(response, &json));
    const auto& sensors = json["sensors"].GetArray();
    UNIT_ASSERT_VALUES_EQUAL(1, sensors.size());
    UNIT_ASSERT_VALUES_EQUAL(
        "Initializing", sensors[0]["labels"]["sensor"].GetString());
    UNIT_ASSERT_VALUES_EQUAL(1, sensors[0]["value"].GetIntegerRobust());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TBootstrapMonitoringTest)
{
    Y_UNIT_TEST(ShouldServeMetricsBeforeMainActorSystemExists)
    {
        const auto port = NTesting::GetFreePort();
        auto monitoring =
            CreateBootstrapMonitoring(MakeConfig(port), "filestore");
        monitoring->Start();
        CheckInitializing(port, "filestore");
        UNIT_ASSERT_VALUES_EQUAL(
            404, TKeepAliveHttpClient("127.0.0.1", port).DoGet("/actors"));
        monitoring->Stop();
    }

    Y_UNIT_TEST(ShouldReleasePortAndPreserveCountersForMainMonitoring)
    {
        const auto port = NTesting::GetFreePort();
        auto monitoring =
            CreateBootstrapMonitoring(MakeConfig(port), "blockstore");
        auto counters = monitoring->GetCounters();
        monitoring->Start();
        CheckInitializing(port, "blockstore");
        UNIT_ASSERT(!Scrape(port, "/counters/spack").empty());
        UNIT_ASSERT(
            Scrape(port, "/counters/prometheus").Contains("Initializing"));

        monitoring->Stop();
        monitoring->Stop();
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "bootstrap")
                ->GetCounter("Initializing", false)
                ->GetAtomic());

        // A listener without SO_REUSEPORT must be able to acquire the port.
        auto mainMonitoring = CreateMonitoringService(port, "127.0.0.1");
        mainMonitoring->RegisterMonPage(new NMonitoring::TDynamicCountersPage(
            "startup", "Startup counters", counters));
        mainMonitoring->Start();
        const auto response = Scrape(port, "/startup/counters=blockstore/json");
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(response, &json));
        UNIT_ASSERT_VALUES_EQUAL(
            0, json["sensors"][0]["value"].GetIntegerRobust());
        mainMonitoring->Stop();
    }

    Y_UNIT_TEST(ShouldServeMetricsOverHttps)
    {
        const auto port = NTesting::GetFreePort();
        auto config = MakeConfig(port);
        config.SetMonitoringCertificateFile(JoinFsPaths(
            ArcadiaSourceRoot(),
            "cloud/storage/core/libs/grpc/ut/certs/server1.crt"));
        config.SetMonitoringPrivateKeyFile(JoinFsPaths(
            ArcadiaSourceRoot(),
            "cloud/storage/core/libs/grpc/ut/certs/server1.key"));
        auto monitoring = CreateBootstrapMonitoring(config, "blockstore");
        monitoring->Start();
        UNIT_ASSERT(
            Scrape(port, "/counters/json", true).Contains("Initializing"));
        monitoring->Stop();
    }

    Y_UNIT_TEST(ShouldAllowMonitoringToBeDisabled)
    {
        auto monitoring =
            CreateBootstrapMonitoring(MakeConfig(0), "blockstore");
        monitoring->Start();
        monitoring->Stop();
    }
}

}   // namespace NCloud::NStorage
