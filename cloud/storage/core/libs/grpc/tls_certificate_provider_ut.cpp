#include "tls_certificate_provider.h"

#include "init.h"
#include "tls_certificate_watcher.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <grpcpp/channel.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/support/channel_arguments.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <chrono>
#include <latch>
#include <memory>
#include <thread>

namespace NCloud {

using namespace NTlsUtils;

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadCertResource(TStringBuf relativePath)
{
    return NResource::Find(
        TStringBuilder() << "grpc/ut/certs/" << relativePath);
}

void WriteTextFile(const TString& path, const TString& content)
{
    TFileOutput out(path);
    out.Write(content.data(), content.size());
}

TCertificateFiles CreateCertificatePair(
    const TString& dirPath,
    const TString& prefix,
    const TString& privateKeyContent,
    const TString& certChainContent)
{
    const TString privateKeyPath = TStringBuilder()
        << dirPath << "/" << prefix << ".key";
    const TString certChainPath = TStringBuilder()
        << dirPath << "/" << prefix << ".crt";

    WriteTextFile(privateKeyPath, privateKeyContent);
    WriteTextFile(certChainPath, certChainContent);

    return {
        .PrivateKeyPath = privateKeyPath,
        .CertChainPath = certChainPath,
    };
}

////////////////////////////////////////////////////////////////////////////////

// Records subscriptions and lets a test fire the callbacks.
class TFakeCertificateWatcher final
    : public ICertificateWatcher
{
public:
    struct TIdentitySubscription
    {
        TCertificateFiles Files;
        TIdentityCallback OnChange;
    };

    struct TRootCaSubscription
    {
        TString Path;
        TRootCaCallback OnChange;
    };

    TVector<TIdentitySubscription> Identities;
    TVector<TRootCaSubscription> RootCas;

    TIdentityContent WatchIdentity(
        const TCertificateFiles& files,
        TIdentityCallback onChange) override
    {
        Identities.push_back({files, std::move(onChange)});
        return ReadIdentity(files).ExtractResult();
    }

    TString WatchRootCa(const TString& path, TRootCaCallback onChange) override
    {
        RootCas.push_back({path, std::move(onChange)});
        return TryReadFile(path).ExtractResult();
    }

    NThreading::TFuture<NProto::TError> UpdateCertificates() override
    {
        return NThreading::MakeFuture(NProto::TError());
    }

    void Start() override
    {}

    void Stop() override
    {}
};

////////////////////////////////////////////////////////////////////////////////

struct TProviderContext
{
    TTempDir TempDir;

    TString RootPath;
    TCertificateFiles ServerPair;
    TCertificateFiles ClientPair;

    std::shared_ptr<TFakeCertificateWatcher> Watcher;

    TProviderContext()
        : RootPath(TStringBuilder() << TempDir.Name() << "/ca.crt")
        , ServerPair(CreateCertificatePair(
              TempDir.Name(),
              "server",
              ReadCertResource("server1.key"),
              ReadCertResource("server1.crt")))
        , ClientPair(CreateCertificatePair(
              TempDir.Name(),
              "client",
              ReadCertResource("server2.key"),
              ReadCertResource("server2.crt")))
        , Watcher(std::make_shared<TFakeCertificateWatcher>())
    {
        WriteTextFile(RootPath, ReadCertResource("ca.crt"));
    }
};

// Whether a client trusting |rootPath| and presenting |identity| completes
// the TLS handshake with the server on |port|.
bool Connects(
    int port,
    const TString& rootPath,
    const TCertificateFiles& identity,
    const TString& serverName)
{
    auto credentials = CreateStaticCertificateProvider(rootPath, {identity})
        ->CreateSecureClientCredentials();

    grpc::ChannelArguments args;
    args.SetSslTargetNameOverride(serverName);
    auto channel = grpc::CreateCustomChannel(
        TStringBuilder() << "localhost:" << port,
        credentials,
        args);

    const auto deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(10);
    while (true) {
        const auto state = channel->GetState(true);
        if (state == GRPC_CHANNEL_READY) {
            return true;
        }
        if (state == GRPC_CHANNEL_TRANSIENT_FAILURE) {
            return false;
        }
        if (!channel->WaitForStateChange(state, deadline)) {
            return false;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

// A real TLS server with a stable port across restarts.
class TTestTlsServer
{
private:
    std::unique_ptr<grpc::Server> Server;
    std::unique_ptr<grpc::ServerCompletionQueue> Queue;
    int Port = 0;

public:
    ~TTestTlsServer()
    {
        Stop();
    }

    void Start(const std::shared_ptr<grpc::ServerCredentials>& credentials)
    {
        grpc::ServerBuilder builder;
        builder.AddListeningPort(
            TStringBuilder() << "localhost:" << Port,
            credentials,
            &Port);
        Queue = builder.AddCompletionQueue();
        Server = builder.BuildAndStart();
        UNIT_ASSERT(Server);
    }

    void Stop()
    {
        if (!Server) {
            return;
        }
        Server->Shutdown();
        Queue->Shutdown();
        void* tag = nullptr;
        bool ok = false;
        while (Queue->Next(&tag, &ok)) {
        }
        Server.reset();
        Queue.reset();
    }

    int GetPort() const
    {
        return Port;
    }
};

std::shared_ptr<grpc::Channel> CreateTestChannel(
    int port,
    const std::shared_ptr<grpc::ChannelCredentials>& credentials,
    const TString& serverName = "server1.grpc.ut")
{
    grpc::ChannelArguments args;
    args.SetSslTargetNameOverride(serverName);
    args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, 100);
    args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, 100);
    args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, 100);
    return grpc::CreateCustomChannel(
        TStringBuilder() << "localhost:" << port,
        credentials,
        args);
}

void WaitForDisconnect(const std::shared_ptr<grpc::Channel>& channel)
{
    const auto deadline =
        std::chrono::system_clock::now() + std::chrono::seconds(5);
    auto state = channel->GetState(false);
    while (state == GRPC_CHANNEL_READY) {
        UNIT_ASSERT(channel->WaitForStateChange(state, deadline));
        state = channel->GetState(false);
    }
    UNIT_ASSERT(!channel->WaitForConnected(
        std::chrono::system_clock::now() + std::chrono::milliseconds(500)));
}

////////////////////////////////////////////////////////////////////////////////

struct TGrpcShutdownFixture: public NUnitTest::TBaseFixture
{
    void TearDown(NUnitTest::TTestContext& /* context */) override
    {
        // A failed TLS handshake leaves a subchannel retry timer on the
        // default EventEngine. The subchannel owns SSL_CTX and releases the
        // last gRPC reference asynchronously, so wait for it here. Otherwise
        // it can be destroyed concurrently with OPENSSL_cleanup at exit.
        UNIT_ASSERT(WaitForGrpcShutdown(TDuration::Seconds(10)));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTlsCertificateProviderTest)
{
    Y_UNIT_TEST(ShouldSkipEmptyPairsInStaticProvider)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        TVector<TCertificateFiles> certs{
            {},
            pair,
            {},
        };

        auto provider = CreateStaticCertificateProvider(
            rootPath,
            std::move(certs));
        UNIT_ASSERT(provider);
        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
    }

    Y_UNIT_TEST(ShouldRejectStaticProviderIncompletePair)
    {
        UNIT_ASSERT_EXCEPTION(
            CreateStaticCertificateProvider(
                {},
                {TCertificateFiles{.PrivateKeyPath = "/tmp/k"}}),
            yexception);

        UNIT_ASSERT_EXCEPTION(
            CreateStaticCertificateProvider(
                {},
                {TCertificateFiles{.CertChainPath = "/tmp/c"}}),
            yexception);
    }

    Y_UNIT_TEST(ShouldFailStaticProviderWithInvalidFiles)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            "broken certificate");
        WriteTextFile(rootPath, "broken root");

        UNIT_ASSERT_EXCEPTION(
            CreateStaticCertificateProvider(
                rootPath,
                TVector<TCertificateFiles>{pair}),
            yexception);
    }

    Y_UNIT_TEST(ShouldSubscribeToEveryNonEmptyPairAndRootCa)
    {
        TProviderContext context;

        auto provider = CreateCertificateProvider(
            context.Watcher,
            context.RootPath,
            TVector<TCertificateFiles>{
                {},
                context.ServerPair,
                {},
                context.ClientPair,
            });
        UNIT_ASSERT(provider);

        UNIT_ASSERT_VALUES_EQUAL(2, context.Watcher->Identities.size());
        UNIT_ASSERT(context.Watcher->Identities[0].Files == context.ServerPair);
        UNIT_ASSERT(context.Watcher->Identities[1].Files == context.ClientPair);
        UNIT_ASSERT_VALUES_EQUAL(1, context.Watcher->RootCas.size());
        UNIT_ASSERT_VALUES_EQUAL(
            context.RootPath,
            context.Watcher->RootCas[0].Path);

        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
    }

    Y_UNIT_TEST(ShouldCreateCredentialsForRootOnlyProvider)
    {
        TProviderContext context;

        auto provider = CreateCertificateProvider(
            context.Watcher,
            context.RootPath,
            TVector<TCertificateFiles>{{}});

        UNIT_ASSERT(context.Watcher->Identities.empty());
        UNIT_ASSERT_VALUES_EQUAL(1, context.Watcher->RootCas.size());
        // Server credentials need an identity; gRPC rejects them later, at
        // server start, so only the client side is checked here.
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
    }

    Y_UNIT_TEST(ShouldNotWatchRootCaWhenPathIsEmpty)
    {
        TProviderContext context;

        auto provider = CreateCertificateProvider(
            context.Watcher,
            {},
            TVector<TCertificateFiles>{context.ServerPair});

        UNIT_ASSERT_VALUES_EQUAL(1, context.Watcher->Identities.size());
        UNIT_ASSERT(context.Watcher->RootCas.empty());
        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
    }

    Y_UNIT_TEST(ShouldAcceptUpdatesFromWatcher)
    {
        TProviderContext context;

        auto provider = CreateCertificateProvider(
            context.Watcher,
            context.RootPath,
            TVector<TCertificateFiles>{context.ServerPair});
        provider->Start();
        Y_DEFER {
            provider->Stop();
        };

        context.Watcher->Identities[0].OnChange(TIdentityContent{
            .PrivateKey = ReadCertResource("server3.key"),
            .CertChain = ReadCertResource("server3.crt"),
        });
        context.Watcher->RootCas[0].OnChange(ReadCertResource("server2.crt"));

        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
    }

    Y_UNIT_TEST(ShouldPropagateWatcherLoadFailure)
    {
        TProviderContext context;
        auto scheduler = CreateScheduler();
        auto watcher = CreateCertificateWatcher(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            scheduler,
            CreateTaskQueueStub(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            TDuration::Zero());

        WriteTextFile(context.ServerPair.CertChainPath, "broken certificate");
        UNIT_ASSERT_EXCEPTION(
            CreateCertificateProvider(
                watcher,
                context.RootPath,
                TVector<TCertificateFiles>{context.ServerPair}),
            yexception);
    }

    Y_UNIT_TEST_F(ShouldPublishWatcherUpdatesToGrpc, TGrpcShutdownFixture)
    {
        TGrpcInitializer grpcInitializer;
        TProviderContext context;

        // server3 is self-signed: trusted by nobody but itself.
        const TString rotatedRootPath = TStringBuilder()
            << context.TempDir.Name() << "/rotated-ca.crt";
        WriteTextFile(rotatedRootPath, ReadCertResource("server3.crt"));
        const auto rotatedPair = CreateCertificatePair(
            context.TempDir.Name(),
            "rotated",
            ReadCertResource("server3.key"),
            ReadCertResource("server3.crt"));

        auto provider = CreateCertificateProvider(
            context.Watcher,
            context.RootPath,
            TVector<TCertificateFiles>{context.ServerPair});

        int port = 0;
        grpc::ServerBuilder builder;
        builder.AddListeningPort(
            "localhost:0",
            provider->CreateSecureServerCredentials(),
            &port);
        // A server with no services still needs a queue to start.
        auto queue = builder.AddCompletionQueue();
        auto server = builder.BuildAndStart();
        UNIT_ASSERT(server);
        Y_DEFER {
            server->Shutdown();
            queue->Shutdown();
            void* tag = nullptr;
            bool ok = false;
            while (queue->Next(&tag, &ok)) {
            }
        };

        UNIT_ASSERT(Connects(
            port, context.RootPath, context.ClientPair, "server1.grpc.ut"));
        UNIT_ASSERT(!Connects(
            port, rotatedRootPath, context.ClientPair, "server3"));

        // The server identity is rotated to server3.
        context.Watcher->Identities[0].OnChange(TIdentityContent{
            .PrivateKey = ReadCertResource("server3.key"),
            .CertChain = ReadCertResource("server3.crt"),
        });
        UNIT_ASSERT(Connects(
            port, rotatedRootPath, context.ClientPair, "server3"));
        UNIT_ASSERT(!Connects(
            port, context.RootPath, context.ClientPair, "server1.grpc.ut"));

        // The server now trusts only server3 as the client CA.
        context.Watcher->RootCas[0].OnChange(ReadCertResource("server3.crt"));
        UNIT_ASSERT(!Connects(
            port, rotatedRootPath, context.ClientPair, "server3"));
        UNIT_ASSERT(Connects(port, rotatedRootPath, rotatedPair, "server3"));
    }

    Y_UNIT_TEST_F(
        ShouldReconnectWatchedClientAfterServerRestart,
        TGrpcShutdownFixture)
    {
        TGrpcInitializer grpcInitializer;

        for (const bool withIdentity: {false, true}) {
            TProviderContext context;
            TVector<TCertificateFiles> identities;
            if (withIdentity) {
                identities.push_back(context.ClientPair);
            }
            auto provider = CreateCertificateProvider(
                context.Watcher,
                context.RootPath,
                std::move(identities));
            auto credentials = provider->CreateSecureClientCredentials();
            auto serverCredentials = CreateStaticCertificateProvider(
                context.RootPath,
                {context.ServerPair})->CreateSecureServerCredentials();

            TTestTlsServer server;
            server.Start(serverCredentials);
            auto channel = CreateTestChannel(server.GetPort(), credentials);
            UNIT_ASSERT(channel->WaitForConnected(
                std::chrono::system_clock::now() + std::chrono::seconds(5)));

            server.Stop();
            WaitForDisconnect(channel);
            server.Start(serverCredentials);

            // No file updates or credential replacement: the same watched
            // client must reconnect after gRPC drops its last TLS watcher.
            UNIT_ASSERT_C(
                channel->WaitForConnected(
                    std::chrono::system_clock::now() +
                    std::chrono::seconds(5)),
                "Watched client did not reconnect; identity=" << withIdentity);
        }
    }

    Y_UNIT_TEST_F(
        ShouldKeepOtherChannelConnectedWhenSharedProviderReconnects,
        TGrpcShutdownFixture)
    {
        TGrpcInitializer grpcInitializer;
        TProviderContext context;
        auto provider = CreateCertificateProvider(
            context.Watcher,
            context.RootPath,
            {context.ClientPair});
        auto credentials = provider->CreateSecureClientCredentials();
        auto serverCredentials = CreateStaticCertificateProvider(
            context.RootPath,
            {context.ServerPair})->CreateSecureServerCredentials();

        // Different ports ensure independent TLS connections sharing one
        // provider and one set of channel credentials.
        TTestTlsServer firstServer;
        TTestTlsServer secondServer;
        firstServer.Start(serverCredentials);
        secondServer.Start(serverCredentials);
        auto first = CreateTestChannel(firstServer.GetPort(), credentials);
        auto second = CreateTestChannel(secondServer.GetPort(), credentials);
        for (const auto& channel: {first, second}) {
            UNIT_ASSERT(channel->WaitForConnected(
                std::chrono::system_clock::now() + std::chrono::seconds(5)));
        }

        firstServer.Stop();
        WaitForDisconnect(first);
        UNIT_ASSERT(second->GetState(false) == GRPC_CHANNEL_READY);

        firstServer.Start(serverCredentials);
        UNIT_ASSERT(first->WaitForConnected(
            std::chrono::system_clock::now() + std::chrono::seconds(5)));
        UNIT_ASSERT(second->GetState(false) == GRPC_CHANNEL_READY);
    }

    Y_UNIT_TEST_F(
        ShouldRetainCertificatesAfterProviderAndWatcherDestruction,
        TGrpcShutdownFixture)
    {
        TGrpcInitializer grpcInitializer;

        for (const bool withIdentity: {false, true}) {
            TProviderContext context;
            TVector<TCertificateFiles> identities;
            if (withIdentity) {
                identities.push_back(context.ClientPair);
            }
            auto provider = CreateCertificateProvider(
                context.Watcher,
                context.RootPath,
                std::move(identities));
            auto credentials = provider->CreateSecureClientCredentials();
            auto serverCredentials = CreateStaticCertificateProvider(
                context.RootPath,
                {context.ServerPair})->CreateSecureServerCredentials();

            provider.reset();
            // File callbacks may still run after the outer provider is gone.
            context.Watcher->RootCas[0].OnChange(ReadCertResource("ca.crt"));
            if (withIdentity) {
                context.Watcher->Identities[0].OnChange(TIdentityContent{
                    .PrivateKey = ReadCertResource("server2.key"),
                    .CertChain = ReadCertResource("server2.crt"),
                });
            }
            context.Watcher.reset();

            TTestTlsServer server;
            server.Start(serverCredentials);
            auto channel = CreateTestChannel(server.GetPort(), credentials);
            credentials.reset();
            UNIT_ASSERT(channel->WaitForConnected(
                std::chrono::system_clock::now() + std::chrono::seconds(5)));

            server.Stop();
            WaitForDisconnect(channel);
            server.Start(serverCredentials);
            UNIT_ASSERT_C(
                channel->WaitForConnected(
                    std::chrono::system_clock::now() +
                    std::chrono::seconds(5)),
                "Credentials lost certificates; identity=" << withIdentity);
        }
    }

    Y_UNIT_TEST_F(
        ShouldApplyRotatedCertificatesWhileChannelsReconnect,
        TGrpcShutdownFixture)
    {
        TGrpcInitializer grpcInitializer;
        TProviderContext context;
        const auto rotatedPair = CreateCertificatePair(
            context.TempDir.Name(),
            "rotated",
            ReadCertResource("server3.key"),
            ReadCertResource("server3.crt"));
        const TString rotatedRootPath = TStringBuilder()
            << context.TempDir.Name() << "/rotated-ca.crt";
        WriteTextFile(rotatedRootPath, ReadCertResource("server3.crt"));

        auto provider = CreateCertificateProvider(
            context.Watcher,
            context.RootPath,
            {context.ClientPair});
        auto credentials = provider->CreateSecureClientCredentials();
        auto initialServerCredentials = CreateStaticCertificateProvider(
            context.RootPath,
            {context.ServerPair})->CreateSecureServerCredentials();
        TTestTlsServer firstServer;
        firstServer.Start(initialServerCredentials);
        auto first = CreateTestChannel(firstServer.GetPort(), credentials);
        UNIT_ASSERT(first->WaitForConnected(
            std::chrono::system_clock::now() + std::chrono::seconds(5)));

        firstServer.Stop();
        WaitForDisconnect(first);
        // The restarted server accepts only the rotated client identity.
        firstServer.Start(CreateStaticCertificateProvider(
            rotatedRootPath,
            {context.ServerPair})->CreateSecureServerCredentials());
        TTestTlsServer secondServer;
        secondServer.Start(CreateStaticCertificateProvider(
            rotatedRootPath,
            {rotatedPair})->CreateSecureServerCredentials());
        auto second = CreateTestChannel(
            secondServer.GetPort(),
            credentials,
            "server3");

        bool firstConnected = false;
        bool secondConnected = false;
        {
            std::latch start{3};
            std::thread reconnectFirst([&]
            {
                start.arrive_and_wait();
                firstConnected = first->WaitForConnected(
                    std::chrono::system_clock::now() + std::chrono::seconds(10));
            });
            std::thread connectSecond([&]
            {
                start.arrive_and_wait();
                secondConnected = second->WaitForConnected(
                    std::chrono::system_clock::now() + std::chrono::seconds(10));
            });

            Y_DEFER {
                reconnectFirst.join();
                connectSecond.join();
            };

            start.arrive_and_wait();
            context.Watcher->RootCas[0].OnChange(
                ReadCertResource("ca.crt") + "\n" + ReadCertResource("server3.crt"));
            context.Watcher->Identities[0].OnChange(TIdentityContent{
                .PrivateKey = ReadCertResource("server3.key"),
                .CertChain = ReadCertResource("server3.crt"),
            });
        }

        // Success of the first channel proves the new identity is in use;
        // success of the second proves the newly trusted CA is in use too.
        UNIT_ASSERT_C(firstConnected, "Rotated client identity was not used");
        UNIT_ASSERT_C(secondConnected, "Rotated root certificates were not used");
    }
}

}   // namespace NCloud
