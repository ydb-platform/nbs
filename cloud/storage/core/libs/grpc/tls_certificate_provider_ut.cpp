#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <library/cpp/logger/log.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/folder/dirut.h>
#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/fs.h>

#include <sys/stat.h>

#include <chrono>
#include <thread>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString TestDataPath(TStringBuf relativePath)
{
    return TStringBuilder()
        << ArcadiaSourceRoot() << "/cloud/storage/core/libs/grpc/ut/certs/"
        << relativePath;
}

TString ReadTextFile(const TString& path)
{
    TFileInput in(path);
    return in.ReadAll();
}

void WriteTextFile(const TString& path, const TString& content)
{
    TFileOutput out(path);
    out.Write(content.data(), content.size());
}

void CopyTextFile(const TString& sourcePath, const TString& destinationPath)
{
    WriteTextFile(destinationPath, ReadTextFile(sourcePath));
}

TCertificateFiles CreateCertificatePair(
    const TString& dirPath,
    const TString& prefix,
    const TString& privateKeySourcePath,
    const TString& certChainSourcePath)
{
    const TString privateKeyPath = TStringBuilder()
        << dirPath << "/" << prefix << ".key";
    const TString certChainPath = TStringBuilder()
        << dirPath << "/" << prefix << ".crt";

    CopyTextFile(privateKeySourcePath, privateKeyPath);
    CopyTextFile(certChainSourcePath, certChainPath);

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

    ICertificateProviderPtr Provider;
    ICertificateRefresherPtr Refresher;
    NMonitoring::TDynamicCountersPtr RootCounters;
    NMonitoring::TDynamicCountersPtr ServerGroup;

    TCertificateProviderTestContext()
        : RootPath(TStringBuilder() << TempDir.Name() << "/ca.crt")
        , ServerPair(CreateCertificatePair(
              TempDir.Name(),
              "server",
              TestDataPath("server1.key"),
              TestDataPath("server1.crt")))
        , ClientPair(CreateCertificatePair(
              TempDir.Name(),
              "client",
              TestDataPath("server2.key"),
              TestDataPath("server2.crt")))
        , RootPem(ReadTextFile(TestDataPath("ca.crt")))
        , ServerPem(ReadTextFile(TestDataPath("server1.crt")))
        , ClientPem(ReadTextFile(TestDataPath("server2.crt")))
    {
        WriteTextFile(RootPath, RootPem);

        RootCounters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        ServerGroup = RootCounters->GetSubgroup("component", "server");

        Refresher = GetCertificateRefresher();
        Refresher->Init(
            CreateLoggingService("console"),
            ServerGroup,
            RootPath,
            TVector<TCertificateFiles>{ServerPair, ClientPair},
            TDuration::Seconds(1));
        Provider = Refresher->GetCertificateProvider();
        UNIT_ASSERT(Provider);
        Provider->Start();
    }

    ~TCertificateProviderTestContext()
    {
        if (Provider) {
            Provider->Stop();
        }
    }

    void RestoreFiles() const
    {
        WriteTextFile(RootPath, RootPem);
        WriteTextFile(ServerPair.CertChainPath, ServerPem);
        WriteTextFile(ClientPair.CertChainPath, ClientPem);
    }

    ui64 GetNotAfterMetric(const TString& certPath) const
    {
        auto certGroup = ServerGroup
            ->GetSubgroup("subsystem", "certificates")
            ->GetSubgroup("cert", GetBaseName(certPath));
        return certGroup
            ->GetCounter("CertNotAfterUnixTimestampSec", false)
            ->Val();
    }
};

TCertificateProviderTestContext& GetTestContext()
{
    static TCertificateProviderTestContext context;
    return context;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTlsCertificateProviderTest)
{
    Y_UNIT_TEST(ShouldUpdateCertificates)
    {
        auto& context = GetTestContext();
        context.RestoreFiles();

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT(
            context.GetNotAfterMetric(context.ServerPair.CertChainPath) > 0);
        UNIT_ASSERT(
            context.GetNotAfterMetric(context.ClientPair.CertChainPath) > 0);
    }

    Y_UNIT_TEST(ShouldSkipUpdateIfRootCaIsInvalid)
    {
        auto& context = GetTestContext();
        context.RestoreFiles();
        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT(
            context.GetNotAfterMetric(context.ServerPair.CertChainPath) > 0);
        const ui64 before = context.GetNotAfterMetric(
            context.ServerPair.CertChainPath);

        WriteTextFile(context.RootPath, "not a certificate");

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            before,
            context.GetNotAfterMetric(context.ServerPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldSkipUpdateIfAnyIdentityPairBecomesInvalid)
    {
        auto& context = GetTestContext();
        context.RestoreFiles();

        context.Provider->UpdateCertificates().GetValueSync();
        UNIT_ASSERT(
            context.GetNotAfterMetric(context.ClientPair.CertChainPath) > 0);
        const ui64 before = context.GetNotAfterMetric(
            context.ClientPair.CertChainPath);

        WriteTextFile(
            context.ClientPair.CertChainPath,
            "broken certificate chain");

        context.Provider->UpdateCertificates().GetValueSync();

        UNIT_ASSERT_VALUES_EQUAL(
            before,
            context.GetNotAfterMetric(context.ClientPair.CertChainPath));
    }

    Y_UNIT_TEST(ShouldFailOnRepeatedInit)
    {
        auto& context = GetTestContext();
        context.RestoreFiles();

        UNIT_ASSERT_EXCEPTION(
            context.Refresher->Init(
                CreateLoggingService("console"),
                context.ServerGroup,
                context.RootPath,
                TVector<TCertificateFiles>{
                    context.ServerPair,
                    context.ClientPair},
                TDuration::Seconds(1)),
            TServiceError);
    }

    Y_UNIT_TEST(ShouldReturnImmediatelyOnUpdateTrigger)
    {
        auto& context = GetTestContext();
        context.RestoreFiles();

        const auto& fifoPath = context.ClientPair.CertChainPath;
        NFs::Remove(fifoPath);

        UNIT_ASSERT_VALUES_EQUAL(0, ::mkfifo(fifoPath.c_str(), 0600));

        std::thread writer([fifoPath, pem = context.ClientPem] {
            TFileOutput out(fifoPath);
            std::this_thread::sleep_for(std::chrono::seconds(1));
            out.Write(pem.data(), pem.size());
        });

        const auto start = TInstant::Now();
        auto future1 = context.Provider->UpdateCertificates();
        const auto stateId1 = future1.StateId();
        UNIT_ASSERT(stateId1.Defined());

        auto future2 = context.Provider->UpdateCertificates();
        const auto stateId2 = future2.StateId();
        UNIT_ASSERT(stateId2.Defined());
        UNIT_ASSERT(*stateId1 == *stateId2);
        const auto elapsed = TInstant::Now() - start;

        UNIT_ASSERT(elapsed < TDuration::MilliSeconds(200));

        future1.GetValueSync();
        future2.GetValueSync();
        writer.join();

        NFs::Remove(fifoPath);
        WriteTextFile(fifoPath, context.ClientPem);
    }
}

}   // namespace NCloud
