#include "tls_utils.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <openssl/bio.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <memory>

namespace NCloud::NTlsUtils {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr long YearSeconds = 365 * 24 * 60 * 60;

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

TString SetCertificateValidity(
    TStringBuf pem,
    long notBeforeOffsetSec,
    long notAfterOffsetSec)
{
    using TBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;
    using TX509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;
    using TAsn1TimePtr =
        std::unique_ptr<ASN1_TIME, decltype(&ASN1_TIME_free)>;

    TBioPtr input(
        BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())),
        BIO_free);
    UNIT_ASSERT(input);

    TX509Ptr certificate(
        PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr),
        X509_free);
    UNIT_ASSERT(certificate);

    TAsn1TimePtr notBefore(
        X509_gmtime_adj(nullptr, notBeforeOffsetSec),
        ASN1_TIME_free);
    TAsn1TimePtr notAfter(
        X509_gmtime_adj(nullptr, notAfterOffsetSec),
        ASN1_TIME_free);
    UNIT_ASSERT(notBefore);
    UNIT_ASSERT(notAfter);
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        X509_set1_notBefore(certificate.get(), notBefore.get()));
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        X509_set1_notAfter(certificate.get(), notAfter.get()));
    UNIT_ASSERT(i2d_re_X509_tbs(certificate.get(), nullptr) > 0);

    TBioPtr output(BIO_new(BIO_s_mem()), BIO_free);
    UNIT_ASSERT(output);
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        PEM_write_bio_X509(output.get(), certificate.get()));

    char* data = nullptr;
    const long size = BIO_get_mem_data(output.get(), &data);
    UNIT_ASSERT(size > 0);
    return TString(data, size);
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

    Y_UNIT_TEST(ShouldValidateIdentityCertificateValidity)
    {
        const auto cert = ReadCertResource("server1.crt");
        const auto result = ValidateIdentityCertificateValidity(cert);
        UNIT_ASSERT(!HasError(result.GetError()));
    }

    Y_UNIT_TEST(ShouldRejectExpiredAndNotYetValidIdentityCertificates)
    {
        const auto cert = ReadCertResource("server1.crt");
        const auto expired = SetCertificateValidity(
            cert,
            -10 * YearSeconds,
            -5 * YearSeconds);
        const auto notYetValid = SetCertificateValidity(
            cert,
            5 * YearSeconds,
            10 * YearSeconds);

        UNIT_ASSERT(HasError(
            ValidateIdentityCertificateValidity(cert + expired).GetError()));
        UNIT_ASSERT(HasError(
            ValidateIdentityCertificateValidity(cert + notYetValid).GetError()));
    }

    Y_UNIT_TEST(ShouldExtractCertificateNotAfterTimestamp)
    {
        const auto cert = ReadCertResource("server1.crt");
        const auto result = GetCertificateNotAfterTimestampSec(cert);
        UNIT_ASSERT(!HasError(result.GetError()));
        UNIT_ASSERT(result.GetResult() > 0);
    }

    Y_UNIT_TEST(ShouldExtractEarliestNotAfterTimestampFromChain)
    {
        const auto cert = ReadCertResource("server1.crt");
        const auto earlier = SetCertificateValidity(
            cert,
            -YearSeconds,
            5 * YearSeconds);
        const auto later = SetCertificateValidity(
            cert,
            -YearSeconds,
            10 * YearSeconds);

        const auto earlierResult =
            GetCertificateNotAfterTimestampSec(earlier);
        UNIT_ASSERT(!HasError(earlierResult.GetError()));

        const auto chainResult =
            GetCertificateNotAfterTimestampSec(later + earlier);
        UNIT_ASSERT(!HasError(chainResult.GetError()));
        UNIT_ASSERT_VALUES_EQUAL(
            earlierResult.GetResult(),
            chainResult.GetResult());
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

    Y_UNIT_TEST(ShouldUpdateAllCertificatesWhenValid)
    {
        TTempDir tempDir;
        const TString rootPath =
            TStringBuilder() << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        TVector<TCertificatePair> certs{TCertificatePair{.Files = files}};
        TRootCaPair root{
            .RootCaPath = rootPath,
            .RootCa = ReadCertResource("ca.crt"),
        };
        TLog log;

        const auto result = UpdateCertificates(certs, root, log);

        UNIT_ASSERT(result.RootCa.Defined());
        UNIT_ASSERT_VALUES_EQUAL(ReadCertResource("ca.crt"), *result.RootCa);
        UNIT_ASSERT_VALUES_EQUAL(1, result.Certificates.size());
        UNIT_ASSERT(result.Certificates[0].Defined());
        UNIT_ASSERT(result.Certificates[0]->NotValidAfter != TInstant::Zero());
    }

    Y_UNIT_TEST(ShouldKeepPreviousRootWhenRootBecomesInvalid)
    {
        TTempDir tempDir;
        const TString rootPath =
            TStringBuilder() << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, "not a certificate");

        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        const TString previousRoot = ReadCertResource("ca.crt");
        TVector<TCertificatePair> certs{TCertificatePair{.Files = files}};
        TRootCaPair root{.RootCaPath = rootPath, .RootCa = previousRoot};
        TLog log;

        const auto result = UpdateCertificates(certs, root, log);

        UNIT_ASSERT(result.RootCa.Defined());
        UNIT_ASSERT_VALUES_EQUAL(previousRoot, *result.RootCa);
    }

    Y_UNIT_TEST(ShouldFallBackToPreviousIdentityWhenFileInvalid)
    {
        TTempDir tempDir;
        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            "broken certificate");

        TVector<TCertificatePair> certs{TCertificatePair{
            .Files = files,
            .PrivateKey = ReadCertResource("server1.key"),
            .CertChain = ReadCertResource("server1.crt"),
        }};
        TLog log;

        const auto result = UpdateCertificates(certs, TRootCaPair{}, log);

        UNIT_ASSERT(result.Certificates[0].Defined());
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.crt"),
            TString(result.Certificates[0]
                        ->CertificatesChain.front()
                        .cert_chain()));
    }

    Y_UNIT_TEST(ShouldFallBackToPreviousIdentityWhenValidityCheckFails)
    {
        TTempDir tempDir;
        const auto validCert = ReadCertResource("server1.crt");
        const auto privateKey = ReadCertResource("server1.key");
        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            privateKey,
            validCert);

        TVector<TCertificatePair> certs{TCertificatePair{
            .Files = files,
            .PrivateKey = privateKey,
            .CertChain = validCert,
        }};
        TLog log;

        for (const auto& invalidCert: {
                 SetCertificateValidity(
                     validCert,
                     -10 * YearSeconds,
                     -5 * YearSeconds),
                 SetCertificateValidity(
                     validCert,
                     5 * YearSeconds,
                     10 * YearSeconds)})
        {
            WriteTextFile(files.CertChainPath, validCert + invalidCert);
            const auto result =
                UpdateCertificates(certs, TRootCaPair{}, log);

            UNIT_ASSERT(result.Certificates[0].Defined());
            UNIT_ASSERT_VALUES_EQUAL(
                validCert,
                TString(result.Certificates[0]
                            ->CertificatesChain.front()
                            .cert_chain()));
        }
    }

    Y_UNIT_TEST(ShouldAcceptExpiredIdentityDuringInitialLoad)
    {
        TTempDir tempDir;
        const auto validCert = ReadCertResource("server1.crt");
        const auto expiredCert = SetCertificateValidity(
            validCert,
            -10 * YearSeconds,
            -5 * YearSeconds);
        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            validCert + expiredCert);

        const auto pairs = LoadCertificatePairs({files});

        UNIT_ASSERT_VALUES_EQUAL(1, pairs.size());
        UNIT_ASSERT_VALUES_EQUAL(validCert + expiredCert, pairs[0].CertChain);
    }

    Y_UNIT_TEST(ShouldLeaveIdentityUndefinedWhenInvalidAndNoPrevious)
    {
        TTempDir tempDir;
        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            "broken certificate");

        TVector<TCertificatePair> certs{TCertificatePair{.Files = files}};
        TLog log;

        const auto result = UpdateCertificates(certs, TRootCaPair{}, log);

        UNIT_ASSERT_VALUES_EQUAL(1, result.Certificates.size());
        UNIT_ASSERT(!result.Certificates[0].Defined());
    }

    Y_UNIT_TEST(ShouldLoadCertificatePairsAndSkipEmpty)
    {
        TTempDir tempDir;
        const auto files = CreateCertificatePair(
            tempDir.Name(),
            "server",
            ReadCertResource("server1.key"),
            ReadCertResource("server1.crt"));

        TVector<TCertificateFiles> input{{}, files, {}};
        const auto pairs = LoadCertificatePairs(std::move(input));

        UNIT_ASSERT_VALUES_EQUAL(1, pairs.size());
        UNIT_ASSERT_VALUES_EQUAL(files.PrivateKeyPath, pairs[0].Files.PrivateKeyPath);
        UNIT_ASSERT_VALUES_EQUAL(files.CertChainPath, pairs[0].Files.CertChainPath);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.key"),
            pairs[0].PrivateKey);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.crt"),
            pairs[0].CertChain);
    }

    Y_UNIT_TEST(ShouldThrowOnIncompletePairs)
    {
        UNIT_ASSERT_EXCEPTION(
            LoadCertificatePairs({TCertificateFiles{.PrivateKeyPath = "/k"}}),
            yexception);
        UNIT_ASSERT_EXCEPTION(
            LoadCertificatePairs({TCertificateFiles{.CertChainPath = "/c"}}),
            yexception);
    }

    Y_UNIT_TEST(ShouldThrowOnUnreadablePairs)
    {
        UNIT_ASSERT_EXCEPTION(
            LoadCertificatePairs({TCertificateFiles{
                .PrivateKeyPath = "/nonexistent/k",
                .CertChainPath = "/nonexistent/c",
            }}),
            yexception);
    }

    Y_UNIT_TEST(ShouldLoadRootCaPair)
    {
        UNIT_ASSERT(!LoadRootCaPair({}).RootCaPath);

        TTempDir tempDir;
        const TString rootPath =
            TStringBuilder() << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, ReadCertResource("ca.crt"));

        const auto pair = LoadRootCaPair(rootPath);
        UNIT_ASSERT_VALUES_EQUAL(rootPath, pair.RootCaPath);
        UNIT_ASSERT_VALUES_EQUAL(ReadCertResource("ca.crt"), pair.RootCa);
    }

    Y_UNIT_TEST(ShouldThrowOnUnreadableRootCaPair)
    {
        UNIT_ASSERT_EXCEPTION(
            LoadRootCaPair("/nonexistent/ca.crt"),
            yexception);
    }
}

}   // namespace NCloud::NTlsUtils
