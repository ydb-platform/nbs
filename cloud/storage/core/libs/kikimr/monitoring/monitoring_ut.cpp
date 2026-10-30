#include "monitoring.h"

#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <contrib/ydb/core/protos/config.pb.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/logger/backend.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/monlib/dynamic_counters/page.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/path.h>
#include <util/folder/tempdir.h>
#include <util/network/socket.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>

#include <mutex>

namespace NCloud::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TCapturedLogBackend final: public TLogBackend
{
private:
    mutable std::mutex Mutex;
    TString Messages;

public:
    void WriteData(const TLogRecord& record) override
    {
        std::lock_guard lock(Mutex);
        Messages.append(record.Data, record.Len);
    }

    void ReopenLog() override
    {}

    TString Read() const
    {
        std::lock_guard lock(Mutex);
        return Messages;
    }

    size_t Count(TStringBuf message) const
    {
        const auto messages = Read();
        size_t count = 0;
        for (auto pos = messages.find(message); pos != TString::npos;
             pos = messages.find(message, pos + message.size()))
        {
            ++count;
        }
        return count;
    }
};

void WaitForMessage(const TCapturedLogBackend& backend, TStringBuf message)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    while (!backend.Read().Contains(message)) {
        UNIT_ASSERT_C(
            TInstant::Now() < deadline,
            "Expected monitoring log message: " << message);
        Sleep(TDuration::MilliSeconds(10));
    }
}

////////////////////////////////////////////////////////////////////////////////

NKikimrConfig::TMonitoringConfig MakeConfig(ui16 port)
{
    NKikimrConfig::TMonitoringConfig config;
    config.SetMonitoringPort(port);
    config.SetMonitoringAddress("127.0.0.1");
    config.SetMonitoringThreads(1);
    return config;
}

TString Get(ui16 port, const TString& path, bool secure = false)
{
    TKeepAliveHttpClient client(
        secure ? "https://127.0.0.1" : "127.0.0.1",
        port, TDuration::Seconds(5), TDuration::Seconds(5));
    if (secure) {
        // Verify the configured TLS transport without relying on the fixture's
        // test-only trust chain or its hostname matching the loopback address.
        client.DisableVerificationForHttps();
    }
    TStringStream response;
    UNIT_ASSERT_VALUES_EQUAL(200, client.DoGet(path, &response));
    return response.Str();
}

TString WaitForCounters(ui16 port, bool secure = false)
{
    const auto deadline = TInstant::Now() + TDuration::Seconds(10);
    for (;;) {
        try {
            TKeepAliveHttpClient client(
                secure ? "https://127.0.0.1" : "127.0.0.1",
                port, TDuration::Seconds(1), TDuration::Seconds(1));
            if (secure) {
                client.DisableVerificationForHttps();
            }
            TStringStream response;
            if (client.DoGet("/counters/json", &response) == 200) {
                return response.Str();
            }
        } catch (const yexception&) {
            // A failed initial bind is expected until the listener can retry.
        }
        UNIT_ASSERT_C(TInstant::Now() < deadline, "Monitoring did not recover");
        Sleep(TDuration::MilliSeconds(10));
    }
}

NJson::TJsonValue Scrape(
    ui16 port,
    const TString& path =
        "/counters/counters=filestore/component=bootstrap/json")
{
    NJson::TJsonValue json;
    UNIT_ASSERT(NJson::ReadJsonTree(Get(port, path), &json));
    return json;
}

i64 GetSensorValue(const NJson::TJsonValue& json, const TString& name)
{
    for (const auto& sensor: json["sensors"].GetArray()) {
        if (sensor["labels"]["sensor"].GetString() == name) {
            return sensor["value"].GetIntegerRobust();
        }
    }
    UNIT_FAIL("Missing sensor " << name);
    return 0;
}

TSocketHolder BindExclusiveListener(ui16 port)
{
    TSocketHolder listener(socket(AF_INET, SOCK_STREAM, 0));
    UNIT_ASSERT(!listener.Closed());
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port);
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    UNIT_ASSERT_VALUES_EQUAL(
        0,
        bind(listener, reinterpret_cast<sockaddr*>(&address), sizeof(address)));
    UNIT_ASSERT_VALUES_EQUAL(0, listen(listener, 1));
    return listener;
}

TString TlsFixture(const TString& name)
{
    return JoinFsPaths(
        ArcadiaSourceRoot(), "cloud/storage/core/libs/grpc/ut/certs/" + name);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TKikimrMonitoringTest)
{
    Y_UNIT_TEST(ShouldServeOnlyCountersBeforeMainActorSystemExists)
    {
        const auto port = NTesting::GetFreePort();
        auto monitoring = CreateKikimrMonitoring(MakeConfig(port), "filestore");
        monitoring->Start();
        UNIT_ASSERT(WaitForCounters(port).Contains("Initializing"));
        UNIT_ASSERT_VALUES_EQUAL(
            1, GetSensorValue(Scrape(port), "Initializing"));
        UNIT_ASSERT(!Get(port, "/counters/spack").empty());
        UNIT_ASSERT(Get(port, "/counters/prometheus").Contains("Initializing"));

        // Authentication and actor pages belong to the normal TMon. The early
        // server must not expose them while their services are unavailable.
        TKeepAliveHttpClient client(
            "127.0.0.1", port, TDuration::Seconds(5), TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(404, client.DoGet("/actors"));
        UNIT_ASSERT_VALUES_EQUAL(404, client.DoGet("/trace"));
        UNIT_ASSERT_VALUES_EQUAL(405, client.DoPost("/counters/json", ""));
    }

    Y_UNIT_TEST(ShouldLogAndRetryWhenMonitoringPortIsOccupied)
    {
        const auto port = NTesting::GetFreePort();
        auto backend = MakeHolder<TCapturedLogBackend>();
        const auto* messages = backend.Get();
        TLog log(std::move(backend));
        auto monitoring =
            CreateKikimrMonitoring(MakeConfig(port), "filestore", log);

        {
            const auto listener = BindExclusiveListener(port);
            // Monitoring is best effort: a bind failure must not prevent node
            // registration or crash-loop a service with a working data path.
            monitoring->Start();
            monitoring->Start();
            WaitForMessage(*messages, "Failed to bind");
            WaitForMessage(*messages, "Failed to init - retrying...");
            UNIT_ASSERT(messages->Read().Contains(ToString(port)));
            // Start's readiness wait spans several native retries. Alternating
            // specific and generic warnings must not defeat log throttling.
            UNIT_ASSERT_VALUES_EQUAL(1, messages->Count("Failed to bind"));
            UNIT_ASSERT_VALUES_EQUAL(
                1, messages->Count("Failed to init - retrying..."));
        }
        // Registration can remain blocked while the HTTP acceptor retries and
        // acquires a port that became available after the initial attempt.
        UNIT_ASSERT(WaitForCounters(port).Contains("Initializing"));
        monitoring->Stop();
        monitoring->Stop();
    }

    Y_UNIT_TEST(ShouldIgnoreUnknownCounterGroupsForHttpAndHttps)
    {
        for (const bool secure: {false, true}) {
            const auto port = NTesting::GetFreePort();
            auto config = MakeConfig(port);
            if (secure) {
                config.SetMonitoringCertificateFile(TlsFixture("server1.crt"));
                config.SetMonitoringPrivateKeyFile(TlsFixture("server1.key"));
            }
            auto monitoring = CreateKikimrMonitoring(config, "filestore");
            monitoring->Start();
            UNIT_ASSERT(WaitForCounters(port, secure).Contains("Initializing"));

            TKeepAliveHttpClient client(
                secure ? "https://127.0.0.1" : "127.0.0.1",
                port, TDuration::Seconds(5), TDuration::Seconds(5));
            if (secure) {
                client.DisableVerificationForHttps();
            }
            // Normal monitoring also responds without content until this
            // service component creates its group after initialization.
            TStringStream response;
            UNIT_ASSERT_VALUES_EQUAL(
                204,
                client.DoGet(
                    "/counters/counters=filestore/component=service/json",
                    &response));
            UNIT_ASSERT(response.Str().empty());
        }
    }

    Y_UNIT_TEST(ShouldReleasePortAndKeepCountersForNormalMonitoring)
    {
        const auto port = NTesting::GetFreePort();
        auto monitoring = CreateKikimrMonitoring(MakeConfig(port), "filestore");
        auto counters = monitoring->GetCounters();
        auto initialization = counters->GetSubgroup("counters", "filestore")
                                  ->GetSubgroup("component", "bootstrap");
        auto counter = initialization->GetCounter("SavedCounter", true);
        *counter = 42;

        monitoring->Start();
        monitoring->Start();
        const auto before = Scrape(port);
        UNIT_ASSERT_VALUES_EQUAL(1, GetSensorValue(before, "Initializing"));
        UNIT_ASSERT_VALUES_EQUAL(42, GetSensorValue(before, "SavedCounter"));

        monitoring->Stop();
        monitoring->Stop();

        // Production stops the early server immediately before KikimrStart().
        // This listener cannot share an occupied port, so acquiring it also
        // proves that Stop() completed before normal monitoring starts.
        auto normalMonitoring = CreateMonitoringService(port, "127.0.0.1");
        normalMonitoring->RegisterMonPage(new NMonitoring::TDynamicCountersPage(
            "startup", "Startup counters", counters));
        normalMonitoring->Start();
        monitoring->CompleteInitialization();
        monitoring->CompleteInitialization();
        ++*counter;
        const auto after = Scrape(
            port, "/startup/counters=filestore/component=bootstrap/json");
        UNIT_ASSERT_VALUES_EQUAL(0, GetSensorValue(after, "Initializing"));
        UNIT_ASSERT_VALUES_EQUAL(43, GetSensorValue(after, "SavedCounter"));
        UNIT_ASSERT(counters == monitoring->GetCounters());
        normalMonitoring->Stop();
    }

    Y_UNIT_TEST(ShouldAllowMonitoringToBeDisabled)
    {
        auto monitoring = CreateKikimrMonitoring(MakeConfig(0), "filestore");
        auto initializing = monitoring->GetCounters()
                                ->GetSubgroup("counters", "filestore")
                                ->GetSubgroup("component", "bootstrap")
                                ->GetCounter("Initializing", false);

        monitoring->Start();
        monitoring->Start();
        UNIT_ASSERT_VALUES_EQUAL(1, initializing->GetAtomic());
        monitoring->CompleteInitialization();
        UNIT_ASSERT_VALUES_EQUAL(0, initializing->GetAtomic());
        monitoring->Stop();
        monitoring->Stop();
    }

    Y_UNIT_TEST(ShouldIgnoreTransportConfigurationWhenMonitoringIsDisabled)
    {
        auto config = MakeConfig(0);
        config.SetInactivityTimeout("not a duration");
        config.SetMonitoringCertificate("not a certificate");
        auto backend = MakeHolder<TCapturedLogBackend>();
        const auto* messages = backend.Get();
        TLog log(std::move(backend));
        auto monitoring = CreateKikimrMonitoring(config, "filestore", log);

        // Port zero disables the transport completely. Configuration intended
        // for a listener must not be parsed or produce startup warnings here.
        monitoring->Start();
        monitoring->Stop();
        UNIT_ASSERT(messages->Read().empty());
    }

    Y_UNIT_TEST(
        ShouldLogInvalidTransportConfigurationWithoutFailingInitialization)
    {
        NKikimrConfig::TMonitoringConfig configs[] = {
            MakeConfig(NTesting::GetFreePort()),
            MakeConfig(NTesting::GetFreePort()),
            MakeConfig(NTesting::GetFreePort()),
        };
        configs[0].SetInactivityTimeout("not a duration");
        configs[1].SetMonitoringAddress("::invalid");
        configs[2].SetMonitoringAddress("::::");

        for (const auto& config: configs) {
            auto backend = MakeHolder<TCapturedLogBackend>();
            const auto* messages = backend.Get();
            TLog log(std::move(backend));
            auto monitoring = CreateKikimrMonitoring(config, "filestore", log);
            auto initializing = monitoring->GetCounters()
                                    ->GetSubgroup("counters", "filestore")
                                    ->GetSubgroup("component", "bootstrap")
                                    ->GetCounter("Initializing", false);

            // Invalid settings must be caught before starting the actor
            // runtime. Losing the listener must not abort or complete
            // initialization.
            UNIT_ASSERT_NO_EXCEPTION(monitoring->Start());
            const auto listener =
                BindExclusiveListener(config.GetMonitoringPort());
            UNIT_ASSERT_NO_EXCEPTION(monitoring->Stop());
            UNIT_ASSERT_C(
                messages->Read().Contains(
                    "Cannot start initialization monitoring"),
                config.ShortDebugString());
            UNIT_ASSERT_VALUES_EQUAL(1, initializing->GetAtomic());
        }
    }

    Y_UNIT_TEST(ShouldServeHttpsAndReleasePortForHttpMonitoring)
    {
        const auto port = NTesting::GetFreePort();
        auto config = MakeConfig(port);
        config.SetMonitoringCertificateFile(TlsFixture("server1.crt"));
        config.SetMonitoringPrivateKeyFile(TlsFixture("server1.key"));
        auto monitoring = CreateKikimrMonitoring(config, "filestore");
        const auto counters = monitoring->GetCounters();
        monitoring->Start();
        UNIT_ASSERT(WaitForCounters(port, true).Contains("Initializing"));
        monitoring->Stop();

        // The final CMS configuration is consumed by normal monitoring after
        // the early transport stops. It may select a different transport on the
        // same port; the early HTTPS settings must not be retained or frozen.
        auto normalMonitoring = CreateMonitoringService(port, "127.0.0.1");
        normalMonitoring->RegisterMonPage(new NMonitoring::TDynamicCountersPage(
            "startup", "Startup counters", counters));
        normalMonitoring->Start();
        monitoring->CompleteInitialization();
        const auto after = Scrape(
            port, "/startup/counters=filestore/component=bootstrap/json");
        UNIT_ASSERT_VALUES_EQUAL(0, GetSensorValue(after, "Initializing"));
        normalMonitoring->Stop();
    }

    Y_UNIT_TEST(ShouldServeHttpsWithInlineCertificateAndPrivateKey)
    {
        const auto port = NTesting::GetFreePort();
        auto config = MakeConfig(port);
        config.SetMonitoringCertificate(
            TFileInput(TlsFixture("server1.crt")).ReadAll() +
            TFileInput(TlsFixture("server1.key")).ReadAll());
        auto monitoring = CreateKikimrMonitoring(config, "filestore");
        monitoring->Start();
        UNIT_ASSERT(WaitForCounters(port, true).Contains("Initializing"));
        monitoring->Stop();
    }

    Y_UNIT_TEST(ShouldLogTlsErrorsAndRecoverWhenCertificatesAppear)
    {
        const auto port = NTesting::GetFreePort();
        TTempDir dir;
        const auto certificate = dir.Path() / "monitoring.crt";
        const auto privateKey = dir.Path() / "monitoring.key";
        auto config = MakeConfig(port);
        config.SetMonitoringCertificateFile(certificate.GetPath());
        config.SetMonitoringPrivateKeyFile(privateKey.GetPath());
        auto backend = MakeHolder<TCapturedLogBackend>();
        const auto* messages = backend.Get();
        TLog log(std::move(backend));
        auto monitoring = CreateKikimrMonitoring(config, "filestore", log);

        {
            const auto listener = BindExclusiveListener(port);
            // The readiness deadline must leave the acceptor retrying. Missing
            // certificates can appear after registration has already blocked.
            monitoring->Start();
            WaitForMessage(
                *messages, "Failed to construct server security context");
            WaitForMessage(*messages, "Failed to init - retrying...");
            UNIT_ASSERT(messages->Read().Contains(certificate.GetPath()));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                messages->Count("Failed to construct server security context"));
            UNIT_ASSERT_VALUES_EQUAL(
                1, messages->Count("Failed to init - retrying..."));
            TOFStream(certificate.GetPath())
                .Write(TFileInput(TlsFixture("server1.crt")).ReadAll());
            TOFStream(privateKey.GetPath())
                .Write(TFileInput(TlsFixture("server1.key")).ReadAll());

            // A different failure must be visible immediately, even while
            // repeated certificate and generic retry warnings are suppressed.
            WaitForMessage(*messages, "Failed to bind");
            UNIT_ASSERT_VALUES_EQUAL(1, messages->Count("Failed to bind"));
            UNIT_ASSERT_VALUES_EQUAL(
                1, messages->Count("Failed to init - retrying..."));
        }
        UNIT_ASSERT(WaitForCounters(port, true).Contains("Initializing"));
        monitoring->Stop();
    }

    Y_UNIT_TEST(ShouldReleaseListenerIfInitializationIsInterrupted)
    {
        const auto port = NTesting::GetFreePort();
        bool started = false;
        UNIT_ASSERT_EXCEPTION(
            [&]
            {
                auto monitoring =
                    CreateKikimrMonitoring(MakeConfig(port), "filestore");
                monitoring->Start();
                started = true;
                ythrow yexception() << "Initialization interrupted";
            }(), yexception);
        UNIT_ASSERT(started);

        // No main actor system was started. Unwinding bootstrap must still
        // close the listener synchronously so another process can bind it.
        const auto listener = BindExclusiveListener(port);
    }
}

}   // namespace NCloud::NStorage
