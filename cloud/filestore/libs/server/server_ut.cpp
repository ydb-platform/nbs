#include "server.h"

#include "config.h"

#include <cloud/filestore/libs/client/client.h>
#include <cloud/filestore/libs/client/config.h>
#include <cloud/filestore/libs/diagnostics/config.h>
#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/diagnostics/profile_log.h>
#include <cloud/filestore/libs/diagnostics/request_stats.h>
#include <cloud/filestore/libs/service/context.h>
#include <cloud/filestore/libs/service/endpoint_test.h>
#include <cloud/filestore/libs/service/filestore_test.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/init.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/datetime/base.h>
#include <util/generic/guid.h>

namespace NCloud::NFileStore::NServer {

using namespace NThreading;

using namespace NCloud::NFileStore::NClient;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

template <typename F>
bool WaitFor(F&& event)
{
    TInstant deadline = WaitTimeout.ToDeadLine();
    while (TInstant::Now() < deadline) {
        if (event()) {
            return true;
        }
        Sleep(TDuration::MilliSeconds(100));
    }
    return false;
}


TString GetTestFilePath(const TString& fileName)
{
    return JoinFsPaths(
        ArcadiaSourceRoot(),
        "cloud/filestore/tests",
        fileName);
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TAtomicStorage
{
private:
    TMaybe<T> Value;
    TMutex Lock;

public:
    void Set(T value)
    {
        with_lock (Lock) {
            Value = std::move(value);
        }
    }

    T Get() const
    {
        with_lock (Lock) {
            return Value.GetRef();
        }
    }

    bool Empty() const
    {
        with_lock (Lock) {
            return Value.Empty();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TLogBackendControlPlaneCollector
    : public TLogBackend
{
private:
    TVector<TString> RequestCandidates;
    TVector<TString> ResponseCandidates;

public:
    void WriteData(const TLogRecord& rec) override
    {
        if (rec.Priority == ELogPriority::TLOG_INFO) {
            if (strstr(rec.Data, "execute request")) {
                RequestCandidates.push_back(rec.Data);
            } else if (strstr(rec.Data, "send response")) {
                ResponseCandidates.push_back(rec.Data);
            }
        }
    }

    void ReopenLog() override
    {
    }

    bool CheckExistCommand(TStringBuf command)
    {
        return CheckExistCommand(command, RequestCandidates)
            && CheckExistCommand(command, ResponseCandidates);
    }

private:
    bool CheckExistCommand(TStringBuf command, const TVector<TString>& candidates)
    {
        for (const auto& row : candidates) {
            if (row.Contains(command)) {
                return true;
            }
        }
        return false;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestServerBuilder final
{
private:
    NProto::TServerConfig ServerConfig;

public:
    TTestServerBuilder& SetSecureEndpoint(
        const TString& rootCertsFileName,
        TVector<std::pair<TString, TString>> certs)
    {
        ServerConfig.SetRootCertsFile(GetTestFilePath(rootCertsFileName));

        for (const auto& cert: certs) {
            auto* c = ServerConfig.MutableCerts()->Add();
            c->SetCertFile(GetTestFilePath(cert.first));
            c->SetCertPrivateKeyFile(GetTestFilePath(cert.second));
        }
        return *this;
    }

    TTestServerBuilder& AddCert(
        const TString& certFileName,
        const TString& certPrivateKeyFileName)
    {
        auto* c = ServerConfig.MutableCerts()->Add();
        c->SetCertFile(GetTestFilePath(certFileName));
        c->SetCertPrivateKeyFile(GetTestFilePath(certPrivateKeyFileName));
        return *this;
    }

    NProto::TServerConfig BuildServerConfig() const
    {
        return ServerConfig;
    }

    void SetUnixSocketPath(const TString& path)
    {
        ServerConfig.SetUnixSocketPath(path);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTestClientBuilder final
{
private:
    NProto::TClientConfig ClientConfig;

public:
    TTestClientBuilder& SetSecureEndpoint(
        const TString& rootCertsFileName,
        const TString& authToken)
    {
        ClientConfig.SetRootCertsFile(GetTestFilePath(rootCertsFileName));
        ClientConfig.SetAuthToken(authToken);
        return *this;
    }

    TTestClientBuilder& SetCertificate(
        const TString& certsFileName,
        const TString& certPrivateKeyFileName)
    {
        ClientConfig.SetCertFile(GetTestFilePath(certsFileName));
        ClientConfig.SetCertPrivateKeyFile(GetTestFilePath(certPrivateKeyFileName));
        return *this;
    }

    NProto::TClientConfig BuildClientConfig()
    {
        return ClientConfig;
    }

    void SetUnixSocketPath(const TString& path)
    {
        ClientConfig.SetUnixSocketPath(path);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TServerSetup
{
    using TService = TFileStoreTest;
    using TClientIntf = IFileStoreServicePtr;

    static TClientIntf CreateClient(
        TClientConfigPtr config,
        ILoggingServicePtr logging)
    {
        return CreateFileStoreClient(std::move(config), std::move(logging));
    }

    static IServerPtr CreateTestServer(
        TServerConfigPtr config,
        ILoggingServicePtr logging,
        IRequestStatsPtr requestStats,
        IFileStoreServicePtr service)
    {
        return CreateServer(
            std::move(config),
            std::move(logging),
            std::move(requestStats),
            CreateProfileLogStub(),
            std::move(service));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TVHostSetup
{
    using TService = TEndpointManagerTest;
    using TClientIntf = IEndpointManagerPtr;

    static TClientIntf CreateClient(
        TClientConfigPtr config,
        ILoggingServicePtr logging)
    {
        return CreateEndpointManagerClient(
            std::move(config),
            std::move(logging));
    }

    static IServerPtr CreateTestServer(
        TServerConfigPtr config,
        ILoggingServicePtr logging,
        IRequestStatsPtr requestStats,
        IEndpointManagerPtr service)
    {
        return CreateServer(
            std::move(config),
            std::move(logging),
            std::move(requestStats),
            std::move(service));
    }
};

////////////////////////////////////////////////////////////////////////////////

template <typename TSetup>
struct TBootstrap
{
    NMonitoring::TDynamicCountersPtr Counters;

    IServerPtr Server;
    TVector<typename TSetup::TClientIntf> Clients;
    ILoggingServicePtr Logging;
    std::shared_ptr<typename TSetup::TService> Service;
    TServerConfigPtr ServerConfig;
    bool Stopped = false;

public:
    TBootstrap(
            const NProto::TServerConfig& serverConfig = {},
            std::shared_ptr<TLogBackendControlPlaneCollector> logBackend = nullptr)
        : Counters{MakeIntrusive<NMonitoring::TDynamicCounters>()}
        , Logging{logBackend ? CreateLoggingService(logBackend) : CreateLoggingService("console")}
        , Service{std::make_shared<typename TSetup::TService>()}
    {
        auto registry = CreateRequestStatsRegistry(
            "server_ut",
            std::make_shared<TDiagnosticsConfig>(),
            Counters,
            CreateWallClockTimer(),
            NCloud::NStorage::NUserStats::CreateUserCounterSupplierStub());

        ServerConfig = CreateConfig(serverConfig);

        auto grpcLog = Logging->CreateLog("GRPC");
        GrpcLoggerInit(grpcLog, true /* enableTracing */);

        Server = TSetup::CreateTestServer(
            ServerConfig,
            Logging,
            registry->GetRequestStats(),
            Service);

        CreateClient();
    }

    ~TBootstrap()
    {
        Stop();
    }

    void Start()
    {
        Server->Start();
        for (auto& client: Clients) {
            client->Start();
        }
    }

    void Stop()
    {
        if (!Stopped) {
            Server->Stop();
            for (auto& client: Clients) {
                client->Stop();
            }
            Stopped = true;
        }
    }

    typename TSetup::TClientIntf CreateClient(NProto::TClientConfig config = {})
    {
        auto clientConfig = config;
        if (!clientConfig.GetSecurePort() && clientConfig.GetRootCertsFile()) {
            clientConfig.SetSecurePort(ServerConfig->GetSecurePort());
        }
        if (!clientConfig.GetPort()) {
            clientConfig.SetPort(ServerConfig->GetPort());
        }
        Clients.push_back(
            TSetup::CreateClient(
                std::make_shared<TClientConfig>(clientConfig),
                Logging));
        return Clients.back();
    }

private:
    TPortManager PortManager;

    TServerConfigPtr CreateConfig(const NProto::TServerConfig& config)
    {
        auto serverConfig = config;
        serverConfig.SetPort(PortManager.GetPort(9021));
        if (serverConfig.GetRootCertsFile()) {
            serverConfig.SetSecurePort(PortManager.GetPort(9022));
        }
        return  std::make_shared<TServerConfig>(std::move(serverConfig));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServerTest)
{
    Y_UNIT_TEST(ShouldHandleRequests)
    {
        TBootstrap<TServerSetup> bootstrap;

        bootstrap.Service->PingHandler = [] (auto, auto) {
            return MakeFuture<NProto::TPingResponse>();
        };

        bootstrap.CreateClient();
        bootstrap.Start();

        auto context = MakeIntrusive<TCallContext>("fs");
        auto request = std::make_shared<NProto::TPingRequest>();

        auto future = bootstrap.Clients[0]->Ping(
            std::move(context),
            std::move(request));

        const auto& response = future.GetValue(WaitTimeout);
        UNIT_ASSERT_C(!HasError(response), FormatError(response.GetError()));
    }

    Y_UNIT_TEST(CheckLoggingPriority)
    {
        auto logBackend = std::make_shared<TLogBackendControlPlaneCollector>();
        TBootstrap<TServerSetup> bootstrap({}, logBackend);

        bootstrap.Service->PingHandler = [] (auto, auto) {
            return MakeFuture<NProto::TPingResponse>();
        };

        bootstrap.Service->CreateFileStoreHandler = [] (auto, auto) {
            return MakeFuture<NProto::TCreateFileStoreResponse>();
        };

        bootstrap.CreateClient();
        bootstrap.Start();

        auto context = MakeIntrusive<TCallContext>("fs");
        auto requestCreateFileStore = std::make_shared<NProto::TCreateFileStoreRequest>();
        auto requestPing = std::make_shared<NProto::TPingRequest>();

        auto futureCreateFileStore = bootstrap.Clients[0]->CreateFileStore(
            context,
            std::move(requestCreateFileStore));

        auto futurePing = bootstrap.Clients[0]->Ping(
            context,
            std::move(requestPing));

        const auto& responseCreateFileStore = futureCreateFileStore.GetValue(WaitTimeout);
        UNIT_ASSERT_C(
            !HasError(responseCreateFileStore),
            FormatError(responseCreateFileStore.GetError()));

        const auto& responsePing = futurePing.GetValue(WaitTimeout);
        UNIT_ASSERT_C(!HasError(responsePing), FormatError(responsePing.GetError()));

        UNIT_ASSERT(logBackend->CheckExistCommand("CreateFileStore"));
        UNIT_ASSERT(!logBackend->CheckExistCommand("Ping"));
    }

    Y_UNIT_TEST(ShouldGetSessionEventsStream)
    {
        TAtomicStorage<IResponseHandlerPtr<NProto::TGetSessionEventsResponse>> storage;

        TBootstrap<TServerSetup> bootstrap;
        bootstrap.Service->GetSessionEventsStreamHandler =
            [&] (auto callContext, auto request, auto responseHandler) {
                Y_UNUSED(callContext, request);
                storage.Set(responseHandler);
            };

        bootstrap.CreateClient();
        bootstrap.Start();

        auto context = MakeIntrusive<TCallContext>("fs");
        auto request = std::make_shared<NProto::TGetSessionEventsRequest>();
        auto responseHandler = std::make_shared<TResponseHandler>();

        bootstrap.Clients[0]->GetSessionEventsStream(
            std::move(context),
            std::move(request),
            responseHandler);
        UNIT_ASSERT(WaitFor([&] { return !storage.Empty(); }));

        auto serverHandler = storage.Get();

        NProto::TGetSessionEventsResponse respose;
        auto* event = respose.AddEvents();
        event->SetSeqNo(1);

        serverHandler->HandleResponse(respose);
        UNIT_ASSERT(WaitFor([=] { return responseHandler->GotResponse(); }));

        serverHandler->HandleCompletion({});
        UNIT_ASSERT(WaitFor([=] { return responseHandler->GotCompletion(); }));
    }

    Y_UNIT_TEST(ShouldHitErrorMetricOnFailure)
    {
        TBootstrap<TServerSetup> bootstrap;
        bootstrap.Service->CreateNodeHandler = [] (auto, auto) {
            return MakeFuture<NProto::TCreateNodeResponse>(TErrorResponse(E_IO, ""));
        };

        bootstrap.CreateClient();
        bootstrap.Start();

        auto context = MakeIntrusive<TCallContext>("fs");
        auto request = std::make_shared<NProto::TCreateNodeRequest>();
        auto future = bootstrap.Clients[0]->CreateNode(
            std::move(context),
            std::move(request));

        auto response = future.GetValueSync();
        UNIT_ASSERT(HasError(response));
        UNIT_ASSERT_EQUAL(E_IO, response.GetError().GetCode());
        bootstrap.Stop();

        auto counters = bootstrap.Counters
            ->FindSubgroup("component", "server_ut")
            ->FindSubgroup("request", "CreateNode");
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Errors")->GetAtomic());
        UNIT_ASSERT_EQUAL(1, counters->GetCounter("Errors/Fatal")->GetAtomic());
    }

    Y_UNIT_TEST(ShouldHandleAuthRequests)
    {
        TTestServerBuilder serverConfigBuilder;
        serverConfigBuilder.SetSecureEndpoint(
            "certs/server.crt",
            {{"certs/server.crt", "certs/server.key"}});

        TBootstrap<TServerSetup> bootstrap(serverConfigBuilder.BuildServerConfig());
        bootstrap.Service->PingHandler =
            [&] (auto callContext, auto request) {
                Y_UNUSED(callContext);
                UNIT_ASSERT_VALUES_EQUAL(
                    "test",
                    request->GetHeaders().GetInternal().GetAuthToken()
                );
                return MakeFuture<NProto::TPingResponse>();
            };

        TTestClientBuilder clientConfigBuilder;
        clientConfigBuilder.SetSecureEndpoint(
            "certs/server.crt",
            "test");

        auto client = bootstrap.CreateClient(clientConfigBuilder.BuildClientConfig());
        bootstrap.Start();

        auto future = client->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }


    Y_UNIT_TEST(ShouldHandleAuthRequestsWithMultipleCertificates)
    {
        TTestServerBuilder serverConfigBuilder;
        serverConfigBuilder.SetSecureEndpoint(
            "certs/server.crt",
            {})
        .AddCert(
            "certs/server_fallback.crt",
            "certs/server.key")
        .AddCert(
            "certs/server.crt",
            "certs/server.key");

        TBootstrap<TServerSetup> bootstrap(serverConfigBuilder.BuildServerConfig());
        bootstrap.Service->PingHandler =
            [&] (auto callContext, auto request) {
                Y_UNUSED(callContext);
                UNIT_ASSERT_VALUES_EQUAL(
                    "test",
                    request->GetHeaders().GetInternal().GetAuthToken()
                );
                return MakeFuture<NProto::TPingResponse>();
            };

        TTestClientBuilder clientConfigBuilder;
        clientConfigBuilder.SetSecureEndpoint(
            "certs/server.crt",
            "test");

        auto client = bootstrap.CreateClient(clientConfigBuilder.BuildClientConfig());
        bootstrap.Start();

        auto future = client->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }


    Y_UNIT_TEST(ShouldHandleSecureAndInsecureClientsSimultaneously)
    {
        TTestServerBuilder serverConfigBuilder;
        serverConfigBuilder.SetSecureEndpoint(
            "certs/server.crt",
            {{"certs/server.crt", "certs/server.key"}});

        TBootstrap<TServerSetup> bootstrap(serverConfigBuilder.BuildServerConfig());
        bootstrap.Service->PingHandler =
            [&] (auto callContext, auto request) {
                Y_UNUSED(callContext);
                Y_UNUSED(request);
                return MakeFuture<NProto::TPingResponse>();
            };

        bootstrap.CreateClient();

        TTestClientBuilder clientConfigBuilder;
        clientConfigBuilder.SetSecureEndpoint(
            "certs/server.crt",
            "test");

        bootstrap.CreateClient(clientConfigBuilder.BuildClientConfig());

        bootstrap.Start();

        {
            auto insecureFuture = bootstrap.Clients[0]->Ping(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TPingRequest>()
            );

            const auto& response = insecureFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }

        {
            auto secureFuture = bootstrap.Clients[1]->Ping(
                MakeIntrusive<TCallContext>(),
                std::make_shared<NProto::TPingRequest>()
            );

            const auto& response = secureFuture.GetValue(TDuration::Seconds(5));
            UNIT_ASSERT_C(!HasError(response), response.GetError());
        }
    }

    Y_UNIT_TEST(ShouldFailRequestWithNonEmptyInternalHeaders)
    {
        TBootstrap<TServerSetup> bootstrap;
        bootstrap.Service->PingHandler =
            [&] (auto callContext, auto request) {
                Y_UNUSED(callContext);
                UNIT_ASSERT_VALUES_EQUAL(
                    "",
                    request->GetHeaders().GetInternal().GetAuthToken()
                );
                return MakeFuture<NProto::TPingResponse>();
            };

        bootstrap.Start();

        auto request = std::make_shared<NProto::TPingRequest>();
        request->MutableHeaders()->MutableInternal()->SetAuthToken("test");
        auto future = bootstrap.Clients[0]->Ping(
            MakeIntrusive<TCallContext>(),
            std::move(request)
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldIdentifyFdControlChannelSource)
    {
        TFsPath unixSocket(CreateGuidAsString() + ".sock");

        TTestServerBuilder serverConfigBuilder;
        serverConfigBuilder.SetUnixSocketPath(unixSocket.GetPath());

        TBootstrap<TVHostSetup> bootstrap(
            serverConfigBuilder.BuildServerConfig());
        bootstrap.Service->PingHandler =
            [&] (auto request) {
                UNIT_ASSERT_VALUES_EQUAL(
                    int(NProto::SOURCE_FD_CONTROL_CHANNEL),
                    int(request->GetHeaders().GetInternal().GetRequestSource())
                );
                return MakeFuture<NProto::TPingResponse>();
            };

        bootstrap.Start();

        TTestClientBuilder clientConfigBuilder;
        clientConfigBuilder.SetUnixSocketPath(unixSocket.GetPath());
        auto client = bootstrap.CreateClient(clientConfigBuilder.BuildClientConfig());
        client->Start();

        auto request = std::make_shared<NProto::TPingRequest>();
        auto future = client->Ping(
            MakeIntrusive<TCallContext>(),
            std::move(request)
        );

        const auto& response = future.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }

    Y_UNIT_TEST(ShouldReportCriticalEventIfFailedToStartUnixSocketEndpoint)
    {
        TFsPath unixSocket("./invalid/path/test_socket");

        TTestServerBuilder serverConfigBuilder;
        serverConfigBuilder.SetUnixSocketPath(unixSocket.GetPath());

        TBootstrap<TVHostSetup> bootstrap(
            serverConfigBuilder.BuildServerConfig());
        auto errorCounter =
            bootstrap.Counters->
            GetSubgroup("component", "server_ut")->
            GetCounter("AppCriticalEvents/EndpointStartingError", true);

        UNIT_ASSERT_VALUES_EQUAL(0, static_cast<int>(*errorCounter));
        bootstrap.Start();
        UNIT_ASSERT_VALUES_EQUAL(1, static_cast<int>(*errorCounter));
    }

    Y_UNIT_TEST(ShouldStartUnixSocketEndpointForFilestoreServer)
    {
        TFsPath unixSocket(CreateGuidAsString() + ".sock");

        TTestServerBuilder serverConfigBuilder;
        serverConfigBuilder.SetUnixSocketPath(unixSocket.GetPath());

        TBootstrap<TServerSetup> bootstrap(
            serverConfigBuilder.BuildServerConfig());

        bootstrap.Service->PingHandler = [] (auto, auto) {
            return MakeFuture<NProto::TPingResponse>();
        };

        TTestClientBuilder clientConfigBuilder;
        clientConfigBuilder.SetUnixSocketPath(unixSocket.GetPath());
        auto client =
            bootstrap.CreateClient(clientConfigBuilder.BuildClientConfig());

        bootstrap.Start();

        auto insecureFuture = bootstrap.Clients[0]->Ping(
            MakeIntrusive<TCallContext>(),
            std::make_shared<NProto::TPingRequest>()
        );

        const auto& response = insecureFuture.GetValue(TDuration::Seconds(5));
        UNIT_ASSERT_C(!HasError(response), response.GetError());
    }
}

}   // namespace NCloud::NFileStore::NServer
