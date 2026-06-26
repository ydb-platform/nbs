#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/generic/deque.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/fs.h>
#include <util/system/mutex.h>

#include <sys/stat.h>

#include <memory>
#include <thread>

namespace NCloud {

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

struct TCertificateProviderTestContext
{
    TTempDir TempDir;

    TString RootPath;
    TCertificateFiles ServerPair;
    TCertificateFiles ClientPair;

    TString RootPem;
    TString ServerPem;
    TString ClientPem;

    ISchedulerPtr Scheduler;
    ICertificateProviderPtr Provider;
    NMonitoring::TDynamicCountersPtr RootCounters;
    NMonitoring::TDynamicCountersPtr ServerGroup;

    TCertificateProviderTestContext()
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
        , RootPem(ReadCertResource("ca.crt"))
        , ServerPem(ReadCertResource("server1.crt"))
        , ClientPem(ReadCertResource("server2.crt"))
        , Scheduler(CreateScheduler())
    {
        Scheduler->Start();
        RestoreFiles();

        RootCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        ServerGroup = RootCounters->GetSubgroup("component", "server");

        Provider = CreateCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            Scheduler,
            ServerGroup,
            RootPath,
            TVector<TCertificateFiles>{ServerPair, ClientPair},
            TDuration::Seconds(1));
        UNIT_ASSERT(Provider);
        Provider->Start();
    }

    ~TCertificateProviderTestContext()
    {
        if (Provider) {
            Provider->Stop();
        }
        Scheduler->Stop();
    }

    void RestoreFiles() const
    {
        WriteTextFile(RootPath, RootPem);
        WriteTextFile(ServerPair.CertChainPath, ServerPem);
        WriteTextFile(ClientPair.CertChainPath, ClientPem);
    }

    ui64 GetExpireTs(const TString& certPath) const
    {
        auto certGroup = ServerGroup
            ->GetSubgroup("subsystem", "certificates")
            ->GetSubgroup("cert", GetBaseName(certPath));
        return certGroup
            ->GetCounter("ExpireTs", false)
            ->Val();
    }
};

////////////////////////////////////////////////////////////////////////////////

// Scheduler that defers callbacks until RunPending() is called, giving the test
// deterministic control over when periodic ticks fire and how many timers are
// outstanding.
class TManualScheduler final
    : public IScheduler
{
private:
    TMutex Lock;
    TDeque<TCallback> Pending;

public:
    void Start() override
    {}

    void Stop() override
    {}

    void Schedule(ITaskQueue*, TInstant, TCallback callback) override
    {
        TGuard guard(Lock);
        Pending.push_back(std::move(callback));
    }

    size_t PendingCount()
    {
        TGuard guard(Lock);
        return Pending.size();
    }

    // Runs every currently pending callback once. Callbacks scheduled during
    // the run stay queued for the next RunPending().
    void RunPending()
    {
        TDeque<TCallback> batch;
        {
            TGuard guard(Lock);
            batch.swap(Pending);
        }
        for (auto& callback: batch) {
            callback();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

// Periodic provider wired to a TManualScheduler; the test starts/stops it and
// fires ticks explicitly.
struct TManualProviderContext
{
    TTempDir TempDir;

    TString RootPath;
    TCertificateFiles ServerPair;
    TCertificateFiles ClientPair;

    std::shared_ptr<TManualScheduler> Scheduler;
    NMonitoring::TDynamicCountersPtr RootCounters;
    NMonitoring::TDynamicCountersPtr ServerGroup;
    ICertificateProviderPtr Provider;

    TManualProviderContext()
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
        , Scheduler(std::make_shared<TManualScheduler>())
        , RootCounters(MakeIntrusive<NMonitoring::TDynamicCounters>())
        , ServerGroup(RootCounters->GetSubgroup("component", "server"))
    {
        WriteTextFile(RootPath, ReadCertResource("ca.crt"));

        Provider = CreatePeriodicCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            Scheduler,
            ServerGroup,
            RootPath,
            TVector<TCertificateFiles>{ServerPair, ClientPair},
            TDuration::Seconds(1));
    }

    void RotateServer(const TString& key, const TString& cert) const
    {
        WriteTextFile(ServerPair.PrivateKeyPath, ReadCertResource(key));
        WriteTextFile(ServerPair.CertChainPath, ReadCertResource(cert));
    }

    ui64 GetExpireTs(const TString& certPath) const
    {
        return ServerGroup
            ->GetSubgroup("subsystem", "certificates")
            ->GetSubgroup("cert", GetBaseName(certPath))
            ->GetCounter("ExpireTs", false)
            ->Val();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTlsCertificateProviderTest)
{
    Y_UNIT_TEST(ShouldUpdateCertificates)
    {
        TCertificateProviderTestContext context;

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT(
            context.GetExpireTs(context.ServerPair.CertChainPath) > 0);
        UNIT_ASSERT(
            context.GetExpireTs(context.ClientPair.CertChainPath) > 0);
    }

    Y_UNIT_TEST(ShouldSkipUpdateIfRootCaIsInvalid)
    {
        TCertificateProviderTestContext context;
        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT(
            context.GetExpireTs(context.ServerPair.CertChainPath) > 0);
        const ui64 before = context.GetExpireTs(
            context.ServerPair.CertChainPath);

        WriteTextFile(context.RootPath, "not a certificate");

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            before,
            context.GetExpireTs(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldSkipUpdateIfAnyIdentityPairBecomesInvalid)
    {
        TCertificateProviderTestContext context;

        context.Provider->UpdateCertificates().GetValueSync();
        UNIT_ASSERT(
            context.GetExpireTs(context.ClientPair.CertChainPath) > 0);
        const ui64 before = context.GetExpireTs(
            context.ClientPair.CertChainPath);

        WriteTextFile(
            context.ClientPair.CertChainPath,
            "broken certificate chain");

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            before,
            context.GetExpireTs(context.ClientPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldPublishCredentialsOnStart)
    {
        TCertificateProviderTestContext context;

        UNIT_ASSERT(context.Provider->CreateSecureServerCredentials());
        UNIT_ASSERT(context.Provider->CreateSecureClientCredentials());
    }

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

    Y_UNIT_TEST(ShouldFallBackToStaticWhenRefreshIntervalIsZero)
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

        auto provider = CreateCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            CreateSchedulerStub(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            rootPath,
            TVector<TCertificateFiles>{pair},
            TDuration::Zero());

        UNIT_ASSERT(provider);
        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
        UNIT_ASSERT(provider->UpdateCertificates().HasValue());
    }

    Y_UNIT_TEST(ShouldFallBackToStubWhenNoCertificatesAndRefreshEnabled)
    {
        auto provider = CreateCertificateProvider(
            CreateLoggingService("console"),
            "TLS_CERTIFICATE_PROVIDER",
            CreateSchedulerStub(),
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            /*rootCertPath=*/{},
            /*certificates=*/{},
            TDuration::Seconds(1));

        UNIT_ASSERT(provider);
        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
        UNIT_ASSERT(provider->UpdateCertificates().HasValue());
    }

    Y_UNIT_TEST(ShouldResolveUpdateRequestedBeforeStart)
    {
        TManualProviderContext context;

        // No Start() yet.
        auto future = context.Provider->UpdateCertificates();
        UNIT_ASSERT(!future.HasValue());
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());

        context.Scheduler->RunPending();

        // The request resolves (as a no-op) instead of hanging forever.
        UNIT_ASSERT(future.HasValue());
    }

    Y_UNIT_TEST(ShouldResolveUpdateRequestedAfterStop)
    {
        TManualProviderContext context;
        context.Provider->Start();
        context.Provider->Stop();

        auto future = context.Provider->UpdateCertificates();
        context.Scheduler->RunPending();

        UNIT_ASSERT(future.HasValue());
    }

    Y_UNIT_TEST(ShouldNotLeakTimersOnOnDemandUpdates)
    {
        TManualProviderContext context;
        context.Provider->Start();

        // Exactly one periodic timer is outstanding after Start().
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());

        for (int i = 0; i < 5; ++i) {
            context.Provider->UpdateCertificates();
            context.Scheduler->RunPending();
            // On-demand triggers must not accumulate self-rescheduling timers:
            // only the single periodic chain survives.
            UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());
        }
    }

    Y_UNIT_TEST(ShouldKeepSinglePeriodicTimer)
    {
        TManualProviderContext context;
        context.Provider->Start();

        for (int i = 0; i < 3; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());
            context.Scheduler->RunPending();
        }
        UNIT_ASSERT_VALUES_EQUAL(1, context.Scheduler->PendingCount());
    }

    Y_UNIT_TEST(ShouldPickUpRotatedCertificateOnTimer)
    {
        TManualProviderContext context;
        context.Provider->Start();

        const ui64 before =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(before > 0);

        // server3 is valid but has a different notAfter than server1.
        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();

        const ui64 after =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(after > 0);
        UNIT_ASSERT_VALUES_UNEQUAL(before, after);
    }

    Y_UNIT_TEST(ShouldBeIdempotentOnStartAndStop)
    {
        TManualProviderContext context;
        context.Provider->Start();
        const size_t afterStart = context.Scheduler->PendingCount();

        // Second Start() must not schedule another periodic chain.
        context.Provider->Start();
        UNIT_ASSERT_VALUES_EQUAL(afterStart, context.Scheduler->PendingCount());

        context.Provider->Stop();
        context.Provider->Stop();

        UNIT_ASSERT(context.Provider->CreateSecureServerCredentials());
    }

    Y_UNIT_TEST(ShouldProvideCredentialsBeforeStart)
    {
        TManualProviderContext context;

        UNIT_ASSERT(context.Provider->CreateSecureServerCredentials());
        UNIT_ASSERT(context.Provider->CreateSecureClientCredentials());
    }

    Y_UNIT_TEST(ShouldRecoverAfterIdentityBecomesValidAgain)
    {
        TManualProviderContext context;
        context.Provider->Start();

        const ui64 initial =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(initial > 0);

        // Break the identity certificate: the periodic refresh skips it and
        // keeps the previously published expiry.
        WriteTextFile(context.ServerPair.CertChainPath, "broken");
        context.Scheduler->RunPending();
        UNIT_ASSERT_VALUES_EQUAL(
            initial,
            context.GetExpireTs(context.ServerPair.CertChainPath));

        // Restore a valid (but different) certificate: it is picked up again.
        context.RotateServer("server3.key", "server3.crt");
        context.Scheduler->RunPending();

        const ui64 recovered =
            context.GetExpireTs(context.ServerPair.CertChainPath);
        UNIT_ASSERT(recovered > 0);
        UNIT_ASSERT_VALUES_UNEQUAL(initial, recovered);
    }
}

}   // namespace NCloud