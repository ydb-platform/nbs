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
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/fs.h>

#include <sys/stat.h>

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
            ServerGroup,
            RootPath,
            TVector<TCertificateFiles>{ServerPair, ClientPair},
            TDuration::Seconds(1),
            Scheduler);
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

    Y_UNIT_TEST(ShouldReturnImmediatelyOnUpdateTrigger)
    {
        TCertificateProviderTestContext context;

        const auto& fifoPath = context.ClientPair.CertChainPath;
        NFs::Remove(fifoPath);

        UNIT_ASSERT_VALUES_EQUAL(0, ::mkfifo(fifoPath.c_str(), 0600));

        auto allowWritePromise = NThreading::NewPromise<void>();
        auto allowWrite = allowWritePromise.GetFuture();
        std::thread writer([fifoPath, pem = context.ClientPem] (
            NThreading::TFuture<void> allowWrite)
        {
            TFileOutput out(fifoPath);
            allowWrite.GetValueSync();
            out.Write(pem.data(), pem.size());
        }, std::move(allowWrite));

        auto future1 = context.Provider->UpdateCertificates();
        const auto stateId1 = future1.StateId();
        UNIT_ASSERT(stateId1.Defined());

        auto future2 = context.Provider->UpdateCertificates();
        const auto stateId2 = future2.StateId();
        UNIT_ASSERT(stateId2.Defined());
        UNIT_ASSERT(*stateId1 == *stateId2);

        UNIT_ASSERT(!future1.IsReady());

        allowWritePromise.SetValue();
        future1.GetValueSync();
        future2.GetValueSync();
        writer.join();
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
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            rootPath,
            TVector<TCertificateFiles>{pair},
            TDuration::Zero(),
            CreateSchedulerStub());

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
            MakeIntrusive<NMonitoring::TDynamicCounters>(),
            /*rootCertPath=*/{},
            /*certificates=*/{},
            TDuration::Seconds(1),
            CreateSchedulerStub());

        UNIT_ASSERT(provider);
        UNIT_ASSERT(provider->CreateSecureServerCredentials());
        UNIT_ASSERT(provider->CreateSecureClientCredentials());
        UNIT_ASSERT(provider->UpdateCertificates().HasValue());
    }
}

}   // namespace NCloud