#include "tls_utils.h"

#include <library/cpp/testing/common/env.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NCloud::NTlsUtils {

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

TCertificateFiles CreateCertificatePair(
    const TString& dirPath,
    const TString& prefix,
    const TString& privateKeySourcePath,
    const TString& certChainSourcePath)
{
    const TString privateKeyPath =
        TStringBuilder() << dirPath << "/" << prefix << ".key";
    const TString certChainPath =
        TStringBuilder() << dirPath << "/" << prefix << ".crt";

    WriteTextFile(privateKeyPath, ReadTextFile(privateKeySourcePath));
    WriteTextFile(certChainPath, ReadTextFile(certChainSourcePath));

    return {
        .PrivateKeyPath = privateKeyPath,
        .CertChainPath = certChainPath,
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TTlsUtilsTest)
{
    Y_UNIT_TEST(ShouldValidatePemCertificate)
    {
        const auto pem = ReadTextFile(TestDataPath("server1.crt"));
        const auto result = IsValidPemCertificate(pem);
        UNIT_ASSERT(!HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectInvalidPemCertificate)
    {
        const auto result = IsValidPemCertificate("not a certificate");
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldMatchPrivateKeyAndCertificate)
    {
        const auto key = ReadTextFile(TestDataPath("server1.key"));
        const auto cert = ReadTextFile(TestDataPath("server1.crt"));
        const auto result = PrivateKeyAndCertificateMatch(key, cert);
        UNIT_ASSERT(!HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldDetectMismatchedPrivateKeyAndCertificate)
    {
        const auto key = ReadTextFile(TestDataPath("server1.key"));
        const auto cert = ReadTextFile(TestDataPath("server2.crt"));
        const auto result = PrivateKeyAndCertificateMatch(key, cert);
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldValidateIdentityWithRoot)
    {
        const auto root = ReadTextFile(TestDataPath("ca.crt"));
        const auto cert = ReadTextFile(TestDataPath("server1.crt"));
        const auto result = ValidateIdentityCertificateWithRoot(root, cert);
        UNIT_ASSERT(!HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldExtractCertificateNotAfterTimestamp)
    {
        const auto cert = ReadTextFile(TestDataPath("server1.crt"));
        const auto result = GetCertificateNotAfterTimestampSec(cert);
        UNIT_ASSERT(!HasError(result.GetError()));
        UNIT_ASSERT(result.GetResult() > 0);
    }

    Y_UNIT_TEST(ShouldReadAndValidateRootCertificate)
    {
        const auto result = ReadAndValidateRootCertificate(
            TestDataPath("ca.crt"));
        UNIT_ASSERT(!HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldReadAndValidateIdentityPair)
    {
        TTempDir tempDir;
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "identity",
            TestDataPath("server1.key"),
            TestDataPath("server1.crt"));

        const auto result = ReadAndValidateIdentityPair(pair);
        UNIT_ASSERT(!HasError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(1, result.GetResult().size());
    }

    Y_UNIT_TEST(ShouldRejectIdentityPairWithMismatchedFiles)
    {
        TTempDir tempDir;
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "identity",
            TestDataPath("server1.key"),
            TestDataPath("server2.crt"));

        const auto result = ReadAndValidateIdentityPair(pair);
        UNIT_ASSERT(HasError(result.GetError()));
    }
}

}   // namespace NCloud::NTlsUtils
