#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

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
    const TString privateKeyPath = TStringBuilder() << dirPath << "/" << prefix << ".key";
    const TString certChainPath = TStringBuilder() << dirPath << "/" << prefix << ".crt";

    CopyTextFile(privateKeySourcePath, privateKeyPath);
    CopyTextFile(certChainSourcePath, certChainPath);

    return {
        .PrivateKeyPath = privateKeyPath,
        .CertChainPath = certChainPath,
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTlsCertificateProviderTest)
{
    Y_UNIT_TEST(ShouldUpdateCertificatesWithoutRootCa)
    {
        TTempDir tempDir;

        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "identity",
            TestDataPath("server1.key"),
            TestDataPath("server1.crt"));

        auto provider = CreatePeriodicCertificateProvider(
            "",
            TVector<TCertificateFiles>{pair},
            1);

        const auto error = provider->UpdateCertificates().GetValueSync();
        UNIT_ASSERT(!HasError(error));
    }

    Y_UNIT_TEST(ShouldFailUpdateIfRootCaIsInvalid)
    {
        TTempDir tempDir;

        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "identity",
            TestDataPath("server1.key"),
            TestDataPath("server1.crt"));

        const TString rootPath = TStringBuilder() << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, "not a certificate");

        auto provider = CreatePeriodicCertificateProvider(
            rootPath,
            TVector<TCertificateFiles>{pair},
            1);

        const auto error = provider->UpdateCertificates().GetValueSync();
        UNIT_ASSERT(HasError(error));
    }

    Y_UNIT_TEST(ShouldFailIfAnyIdentityPairBecomesInvalid)
    {
        TTempDir tempDir;

        const TString rootPath = TStringBuilder() << tempDir.Name() << "/ca.crt";
        CopyTextFile(
            TestDataPath("ca.crt"),
            rootPath);

        const auto serverPair = CreateCertificatePair(
            tempDir.Name(),
            "server",
            TestDataPath("server1.key"),
            TestDataPath("server1.crt"));
        const auto clientPair = CreateCertificatePair(
            tempDir.Name(),
            "client",
            TestDataPath("server2.key"),
            TestDataPath("server2.crt"));

        auto provider = CreatePeriodicCertificateProvider(
            rootPath,
            TVector<TCertificateFiles>{
                serverPair,
                clientPair},
            1);

        const auto initialError = provider->UpdateCertificates().GetValueSync();
        UNIT_ASSERT(!HasError(initialError));

        WriteTextFile(clientPair.CertChainPath, "broken certificate chain");

        const auto updateError = provider->UpdateCertificates().GetValueSync();
        UNIT_ASSERT(HasError(updateError));
    }
}

}   // namespace NCloud
