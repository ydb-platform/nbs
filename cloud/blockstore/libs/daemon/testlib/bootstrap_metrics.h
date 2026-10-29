#pragma once

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/public/api/grpc/ydb_discovery_v1.grpc.pb.h>
#include <contrib/ydb/public/api/protos/ydb_discovery.pb.h>

#include <contrib/libs/grpc/include/grpcpp/security/server_credentials.h>
#include <contrib/libs/grpc/include/grpcpp/server_builder.h>

#include <library/cpp/http/simple/http_client.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/common/network.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/folder/tempdir.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/stream/str.h>
#include <util/string/cast.h>

#include <chrono>
#include <future>
#include <memory>
#include <mutex>

namespace NCloud::NBlockStore::NServer::NTests {

////////////////////////////////////////////////////////////////////////////////

// Exercise the real bootstrap up to its blocking registration call. The fake
// broker scrapes before replying, then deliberately rejects registration so the
// test does not need a running storage cluster or unrelated backend services.
class TBootstrapMetricsEnv
{
private:
    static constexpr const char* RegistrationRejected =
        "registration rejected after checking initialization metrics";

    class TBroker final: public Ydb::Discovery::V1::DiscoveryService::Service
    {
    private:
        const ui16 MonitoringPort;
        std::promise<TString> Metrics;
        std::once_flag Observed;

    public:
        explicit TBroker(ui16 monitoringPort)
            : MonitoringPort(monitoringPort)
        {}

        std::future<TString> GetMetrics()
        {
            return Metrics.get_future();
        }

        grpc::Status NodeRegistration(
            grpc::ServerContext*,
            const Ydb::Discovery::NodeRegistrationRequest*,
            Ydb::Discovery::NodeRegistrationResponse* response) override
        {
            std::call_once(
                Observed,
                [&]
                {
                    try {
                        TKeepAliveHttpClient client(
                            "127.0.0.1",
                            MonitoringPort,
                            TDuration::Seconds(5), TDuration::Seconds(5));
                        TStringStream body;
                        const auto status = client.DoGet(
                            "/counters/counters=blockstore/"
                            "component=bootstrap/json", &body);
                        Y_ENSURE(
                            status == 200,
                            "Initialization metrics request returned HTTP "
                                << status << ": " << body.Str());
                        Metrics.set_value(body.Str());
                    } catch (...) {
                        // Assertions abort on gRPC worker threads. Preserve
                        // ordinary request failures for the test thread and
                        // still release Init().
                        Metrics.set_exception(std::current_exception());
                    }
                });

            auto* operation = response->mutable_operation();
            operation->set_ready(true);
            operation->set_status(Ydb::StatusIds::BAD_REQUEST);
            operation->add_issues()->set_message(RegistrationRejected);
            return grpc::Status::OK;
        }
    };

    TPortManager Ports;
    const ui16 MonitoringPort = Ports.GetPort();
    const ui16 BrokerPort = Ports.GetPort();
    const ui16 InterconnectPort = Ports.GetPort();
    TTempDir Dir;
    TBroker Broker{MonitoringPort};
    std::unique_ptr<grpc::Server> Server;

public:
    TBootstrapMetricsEnv()
    {
        const auto certificate = JoinFsPaths(
            ArcadiaSourceRoot(), "cloud/blockstore/tests/certs/server.crt");
        const auto privateKey = JoinFsPaths(
            ArcadiaSourceRoot(), "cloud/blockstore/tests/certs/server.key");
        grpc::SslServerCredentialsOptions ssl;
        grpc::SslServerCredentialsOptions::PemKeyCertPair keyCert;
        keyCert.private_key = TFileInput(privateKey).ReadAll();
        keyCert.cert_chain = TFileInput(certificate).ReadAll();
        ssl.pem_key_cert_pairs.push_back(std::move(keyCert));
        grpc::ServerBuilder builder;
        builder.RegisterService(&Broker);
        builder.AddListeningPort(
            "[::]:" + ToString(BrokerPort), grpc::SslServerCredentials(ssl));
        Server = builder.BuildAndStart();
        UNIT_ASSERT(Server);

        // One explicit failed registration keeps this test bounded and avoids
        // changing the production retry policy to reach the ordering assertion.
        TStringStream storage;
        storage << "NodeRegistrationMaxAttempts: 1\n"
                   "NodeRegistrationTimeout: 10000\n"
                   "NodeRegistrationUseSsl: true\n"
                   "NodeRegistrationRootCertsFile: \""
                << certificate << "\"\n";
        TOFStream((Dir.Path() / "storage.txt").GetPath()).Write(storage.Str());
    }

    ~TBootstrapMetricsEnv()
    {
        if (Server) {
            Server->Shutdown();
        }
    }

    template <typename TBootstrap>
    void CheckRegistration(
        TBootstrap& bootstrap, const TVector<TString>& extraArgs = {})
    {
        auto metrics = Broker.GetMetrics();
        TVector<TString> args = {
            "./program",
            "--ic-port",
            ToString(InterconnectPort),
            "--domain",
            "local",
            "--node-broker",
            "localhost",
            "--node-broker-port",
            ToString(BrokerPort),
            "--mon-address",
            "127.0.0.1",
            "--mon-port",
            ToString(MonitoringPort),
            "--storage-file",
            (Dir.Path() / "storage.txt").GetPath(),
            "--load-configs-from-cms",
        };
        args.insert(args.end(), extraArgs.begin(), extraArgs.end());
        TVector<char*> argv;
        for (auto& arg: args) {
            argv.push_back(arg.begin());
        }
        bootstrap.ParseOptions(argv.size(), argv.data());

        // Removing or moving the production early-monitor startup below
        // registration makes the broker's scrape fail, even though Init() still
        // reaches our deliberate rejection and unwinds to the test thread.
        UNIT_ASSERT_EXCEPTION_CONTAINS(
            bootstrap.Init(), TServiceError, RegistrationRejected);
        // The broker stores the scrape result before sending its rejection.
        UNIT_ASSERT_C(
            metrics.wait_for(std::chrono::seconds(0)) ==
                std::future_status::ready,
            "Node registration did not produce initialization metrics");
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(metrics.get(), &json));
        bool found = false;
        for (const auto& sensor: json["sensors"].GetArray()) {
            if (sensor["labels"]["sensor"].GetString() == "Initializing") {
                UNIT_ASSERT_VALUES_EQUAL(1, sensor["value"].GetIntegerRobust());
                found = true;
            }
        }
        UNIT_ASSERT_C(
            found, "Missing initialization gauge during registration");
    }
};

}   // namespace NCloud::NBlockStore::NServer::NTests
