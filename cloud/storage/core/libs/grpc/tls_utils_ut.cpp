#include "tls_utils.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/builder.h>

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/rsa.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

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

using TEvpPkeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using TX509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;

TEvpPkeyPtr GeneratePrivateKey()
{
    using TCtxPtr =
        std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)>;

    TCtxPtr ctx(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), EVP_PKEY_CTX_free);
    UNIT_ASSERT(ctx);
    UNIT_ASSERT_VALUES_EQUAL(1, EVP_PKEY_keygen_init(ctx.get()));
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        EVP_PKEY_CTX_set_rsa_keygen_bits(ctx.get(), 2048));

    EVP_PKEY* key = nullptr;
    UNIT_ASSERT_VALUES_EQUAL(1, EVP_PKEY_keygen(ctx.get(), &key));
    return TEvpPkeyPtr(key, EVP_PKEY_free);
}

TString PrivateKeyToPem(EVP_PKEY* key)
{
    using TBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;

    TBioPtr output(BIO_new(BIO_s_mem()), BIO_free);
    UNIT_ASSERT(output);
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        PEM_write_bio_PrivateKey(
            output.get(),
            key,
            nullptr,
            nullptr,
            0,
            nullptr,
            nullptr));

    char* data = nullptr;
    const long size = BIO_get_mem_data(output.get(), &data);
    UNIT_ASSERT(size > 0);
    return TString(data, size);
}

void AssertChainCannotBeBuilt(const TResultOrError<void>& result)
{
    UNIT_ASSERT(HasError(result.GetError()));
    UNIT_ASSERT_STRING_CONTAINS(
        result.GetError().GetMessage(),
        "chain cannot be built");
}

TString CertificateToPem(X509* certificate)
{
    using TBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;

    TBioPtr output(BIO_new(BIO_s_mem()), BIO_free);
    UNIT_ASSERT(output);
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        PEM_write_bio_X509(output.get(), certificate));

    char* data = nullptr;
    const long size = BIO_get_mem_data(output.get(), &data);
    UNIT_ASSERT(size > 0);
    return TString(data, size);
}

TX509Ptr ParseCertificate(TStringBuf pem)
{
    using TBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;

    TBioPtr input(
        BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())),
        BIO_free);
    UNIT_ASSERT(input);

    TX509Ptr certificate(
        PEM_read_bio_X509(input.get(), nullptr, nullptr, nullptr),
        X509_free);
    UNIT_ASSERT(certificate);
    return certificate;
}

void AddExtension(X509* certificate, X509* issuer, int nid, const char* value)
{
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, issuer, certificate, nullptr, nullptr, 0);

    X509_EXTENSION* extension = X509V3_EXT_conf_nid(
        nullptr,
        &ctx,
        nid,
        const_cast<char*>(value));
    UNIT_ASSERT(extension);
    UNIT_ASSERT_VALUES_EQUAL(1, X509_add_ext(certificate, extension, -1));
    X509_EXTENSION_free(extension);
}

// CA certificate for |key|, self-signed if |issuer| is null and signed by
// |issuerKey| otherwise. Valid from one day ago for one year.
TString IssueCertificate(
    const TString& commonName,
    EVP_PKEY* key,
    X509* issuer,
    EVP_PKEY* issuerKey)
{
    TX509Ptr certificate(X509_new(), X509_free);
    UNIT_ASSERT(certificate);
    UNIT_ASSERT_VALUES_EQUAL(1, X509_set_version(certificate.get(), 2));
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        ASN1_INTEGER_set(
            X509_get_serialNumber(certificate.get()),
            static_cast<long>(TInstant::Now().MicroSeconds() & 0x7fffffff)));

    X509_NAME* subject = X509_get_subject_name(certificate.get());
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        X509_NAME_add_entry_by_txt(
            subject,
            "CN",
            MBSTRING_ASC,
            reinterpret_cast<const unsigned char*>(commonName.c_str()),
            -1,
            -1,
            0));
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        X509_set_issuer_name(
            certificate.get(),
            issuer ? X509_get_subject_name(issuer) : subject));

    UNIT_ASSERT(X509_gmtime_adj(
        X509_getm_notBefore(certificate.get()),
        -24 * 60 * 60));
    UNIT_ASSERT(X509_gmtime_adj(
        X509_getm_notAfter(certificate.get()),
        YearSeconds));
    UNIT_ASSERT_VALUES_EQUAL(1, X509_set_pubkey(certificate.get(), key));

    AddExtension(
        certificate.get(),
        issuer ? issuer : certificate.get(),
        NID_basic_constraints,
        "critical,CA:TRUE");
    AddExtension(
        certificate.get(),
        issuer ? issuer : certificate.get(),
        NID_key_usage,
        "critical,keyCertSign,digitalSignature");

    UNIT_ASSERT(X509_sign(
        certificate.get(),
        issuerKey ? issuerKey : key,
        EVP_sha256()));

    return CertificateToPem(certificate.get());
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

        const auto key = ReadCertResource("server1.key");

        const auto expiredResult =
            ValidateIdentity({.PrivateKey = key, .CertChain = cert + expired});
        UNIT_ASSERT_STRING_CONTAINS(
            expiredResult.GetError().GetMessage(),
            "#1 has expired");

        const auto notYetValidResult = ValidateIdentity({
            .PrivateKey = key,
            .CertChain = cert + notYetValid,
        });
        UNIT_ASSERT_STRING_CONTAINS(
            notYetValidResult.GetError().GetMessage(),
            "#1 is not valid yet");
    }

    Y_UNIT_TEST(ShouldAcceptIdentityCertificateChainThatCanBeBuilt)
    {
        const auto key = ReadCertResource("server1.key");
        const auto leaf = ReadCertResource("server1.crt");
        const auto ca = ReadCertResource("ca.crt");

        UNIT_ASSERT(!HasError(
            ValidateIdentity({.PrivateKey = key, .CertChain = leaf})
                .GetError()));
        UNIT_ASSERT(!HasError(
            ValidateIdentity({.PrivateKey = key, .CertChain = leaf + ca})
                .GetError()));
        UNIT_ASSERT(!HasError(
            ValidateIdentity({
                .PrivateKey = ReadCertResource("server3.key"),
                .CertChain = ReadCertResource("server3.crt"),
            }).GetError()));
    }

    Y_UNIT_TEST(ShouldRejectIdentityCertificateChainThatCannotBeBuilt)
    {
        const auto key = ReadCertResource("server1.key");
        const auto leaf = ReadCertResource("server1.crt");
        const auto ca = ReadCertResource("ca.crt");
        const auto unrelated = ReadCertResource("server3.crt");

        // Leaf is not issued by the next certificate.
        AssertChainCannotBeBuilt(ValidateIdentity({
            .PrivateKey = key,
            .CertChain = leaf + unrelated,
        }));
        // Intermediate certificate is not issued by the next certificate.
        // ca.crt has no trailing newline.
        AssertChainCannotBeBuilt(ValidateIdentity({
            .PrivateKey = key,
            .CertChain = leaf + ca + "\n" + unrelated,
        }));
    }

    Y_UNIT_TEST(ShouldRejectIdentityCertificateChainWithRenamedIssuer)
    {
        const auto issuerKey = GeneratePrivateKey();
        const auto leafKey = GeneratePrivateKey();
        const auto leafKeyPem = PrivateKeyToPem(leafKey.get());

        const auto issuerPem = IssueCertificate(
            "intermediate",
            issuerKey.get(),
            nullptr,
            nullptr);
        const auto issuer = ParseCertificate(issuerPem);
        const auto leafPem = IssueCertificate(
            "leaf",
            leafKey.get(),
            issuer.get(),
            issuerKey.get());
        UNIT_ASSERT(!HasError(
            ValidateIdentity({
                .PrivateKey = leafKeyPem,
                .CertChain = leafPem + issuerPem,
            }).GetError()));

        // Issuer certificate has been re-issued for the same key with a
        // different subject: the signature is valid, but the issuer name of
        // the leaf certificate does not match.
        const auto renamedIssuerPem = IssueCertificate(
            "intermediate-renamed",
            issuerKey.get(),
            nullptr,
            nullptr);
        AssertChainCannotBeBuilt(ValidateIdentity({
            .PrivateKey = leafKeyPem,
            .CertChain = leafPem + renamedIssuerPem,
        }));
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

    Y_UNIT_TEST(ShouldRejectIdentityPairWithMismatchedFiles)
    {
        TTempDir tempDir;
        const auto pair = CreateCertificatePair(
            tempDir.Name(),
            "identity",
            ReadCertResource("server1.key"),
            ReadCertResource("server2.crt"));

        UNIT_ASSERT_EXCEPTION_CONTAINS(
            LoadCertificatePairs({pair}),
            yexception,
            "does not match");
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

    Y_UNIT_TEST(ShouldRejectIdentityWithMismatchedKeyOrBrokenChain)
    {
        const auto key = ReadCertResource("server1.key");
        const auto cert = ReadCertResource("server1.crt");

        const auto mismatchedKey = ValidateIdentity({
            .PrivateKey = ReadCertResource("server2.key"),
            .CertChain = cert,
        });
        UNIT_ASSERT_STRING_CONTAINS(
            mismatchedKey.GetError().GetMessage(),
            "does not match");

        const auto brokenChain =
            ValidateIdentity({.PrivateKey = key, .CertChain = "broken"});
        UNIT_ASSERT_STRING_CONTAINS(
            brokenChain.GetError().GetMessage(),
            "Failed to parse");
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
        UNIT_ASSERT_VALUES_EQUAL(
            validCert + expiredCert,
            pairs[0].Content.CertChain);
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
            pairs[0].Content.PrivateKey);
        UNIT_ASSERT_VALUES_EQUAL(
            ReadCertResource("server1.crt"),
            pairs[0].Content.CertChain);
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

        TTempDir tempDir;
        const TString rootPath =
            TStringBuilder() << tempDir.Name() << "/ca.crt";
        WriteTextFile(rootPath, "not a certificate");
        UNIT_ASSERT_EXCEPTION(LoadRootCaPair(rootPath), yexception);
    }
}

}   // namespace NCloud::NTlsUtils
