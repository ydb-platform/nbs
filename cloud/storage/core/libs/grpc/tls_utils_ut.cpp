#include "tls_utils.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

namespace NCloud::NTlsUtils {

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
    const TString privateKeyPath =
        TStringBuilder() << dirPath << "/" << prefix << ".key";
    const TString certChainPath =
        TStringBuilder() << dirPath << "/" << prefix << ".crt";

    WriteTextFile(privateKeyPath, privateKeyContent);
    WriteTextFile(certChainPath, certChainContent);

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
        const auto pem = ReadCertResource("server1.crt");
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
        const auto key = ReadCertResource("server1.key");
        const auto cert = ReadCertResource("server1.crt");
        const auto result = PrivateKeyAndCertificateMatch(key, cert);
        UNIT_ASSERT(!HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldDetectMismatchedPrivateKeyAndCertificate)
    {
        const auto key = ReadCertResource("server1.key");
        const auto cert = ReadCertResource("server2.crt");
        const auto result = PrivateKeyAndCertificateMatch(key, cert);
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldValidateIdentityWithRoot)
    {
        const auto root = ReadCertResource("ca.crt");
        const auto cert = ReadCertResource("server1.crt");
        const auto result = ValidateIdentityCertificateWithRoot(root, cert);
        UNIT_ASSERT(!HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldExtractCertificateNotAfterTimestamp)
    {
        const auto cert = ReadCertResource("server1.crt");
        const auto result = GetCertificateNotAfterTimestampSec(cert);
        UNIT_ASSERT(!HasError(result.GetError()));
        UNIT_ASSERT(result.GetResult() > 0);
    }

    Y_UNIT_TEST(ShouldReadAndValidateRootCertificate)
    {
        TTempDir tempDir;
        const TString rootPath = TStringBuilder()
            << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));
        const auto result = ReadAndValidateRootCertificate(rootPath);
        UNIT_ASSERT(!HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldReadAndValidateIdentityPair)
    {
        TTempDir tempDir;
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "identity",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

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
            ReadCertResource("server1.key"),
            ReadCertResource("server2.crt"));

        const auto result = ReadAndValidateIdentityPair(pair);
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectEmptyPemCertificate)
    {
        const auto result = IsValidPemCertificate("");
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectPemWithoutCertificate)
    {
        const auto key = ReadCertResource("server1.key");
        const auto result = IsValidPemCertificate(key);
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectIdentityNotSignedByRoot)
    {
        const auto root = ReadCertResource("server2.crt");
        const auto cert = ReadCertResource("server1.crt");
        const auto result = ValidateIdentityCertificateWithRoot(root, cert);
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectIdentityValidationWithEmptyRoot)
    {
        const auto cert = ReadCertResource("server1.crt");
        const auto result = ValidateIdentityCertificateWithRoot("", cert);
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectIdentityValidationWithEmptyIdentity)
    {
        const auto root = ReadCertResource("ca.crt");
        const auto result = ValidateIdentityCertificateWithRoot(root, "");
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldFailExtractingNotAfterFromInvalidCertificate)
    {
        const auto result =
            GetCertificateNotAfterTimestampSec("not a certificate");
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldFailReadingMissingFile)
    {
        const auto result = TryReadFile("/nonexistent/certificate.pem");
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldFailValidatingMissingRootCertificate)
    {
        const auto result =
            ReadAndValidateRootCertificate("/nonexistent/ca.crt");
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectIdentityPairWithMissingFiles)
    {
        const TCertificateFiles files{
            .PrivateKeyPath = "/nonexistent/identity.key",
            .CertChainPath = "/nonexistent/identity.crt",
        };
        const auto result = ReadAndValidateIdentityPair(files);
        UNIT_ASSERT(HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldReportSystemErrorOnVerificationFailure)
    {
        const auto root = ReadCertResource("server2.crt");
        const auto cert = ReadCertResource("server1.crt");
        const auto result = ValidateIdentityCertificateWithRoot(root, cert);
        UNIT_ASSERT(HasError(result.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<ui32>(FACILITY_SYSTEM),
            FACILITY_FROM_CODE(result.GetError().GetCode()));
    }
}

}   // namespace NCloud::NTlsUtils
