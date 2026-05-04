#include "tls_utils.h"

#include <util/stream/file.h>
#include <util/string/builder.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <ctime>
#include <memory>
#include <vector>

namespace NCloud::NTlsUtils {

namespace {

using TBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;
using TX509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;
using TEvpPkeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using TX509StorePtr = std::unique_ptr<X509_STORE, decltype(&X509_STORE_free)>;
using TX509StoreCtxPtr = std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)>;
using TX509StackPtr = std::unique_ptr<STACK_OF(X509), decltype(&sk_X509_free)>;

TResultOrError<std::vector<TX509Ptr>> ParsePemCertificates(std::string_view pem)
{
    std::vector<TX509Ptr> certificates;
    TBioPtr bio(BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())), BIO_free);
    if (!bio) {
        return TErrorResponse(E_FAIL, "Failed to allocate BIO for PEM certificates");
    }

    while (true) {
        X509* cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);
        if (cert != nullptr) {
            certificates.emplace_back(cert, X509_free);
            continue;
        }

        const auto error = ERR_peek_last_error();
        if (error == 0) {
            break;
        }

        if (ERR_GET_LIB(error) == ERR_LIB_PEM &&
            ERR_GET_REASON(error) == PEM_R_NO_START_LINE)
        {
            ERR_clear_error();
            break;
        }

        ERR_clear_error();
        return TErrorResponse(E_FAIL, "Failed to parse PEM certificates");
    }

    return certificates;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> TryReadFile(const TString& path)
{
    try {
        TFileInput in(path);
        return in.ReadAll();
    } catch (const std::exception& e) {
        const auto message = TStringBuilder()
            << "Reading certificate file " << path.Quote()
            << " failed: " << e.what();
        return TErrorResponse(E_FAIL, message);
    }
}

TResultOrError<void> IsValidPemCertificate(std::string_view pem)
{
    if (pem.empty()) {
        return TErrorResponse(E_FAIL, "PEM certificate is empty");
    }

    auto parseResult = ParsePemCertificates(pem);
    if (HasError(parseResult.GetError())) {
        return parseResult.GetError();
    }
    if (parseResult.GetResult().empty()) {
        return TErrorResponse(E_FAIL, "No certificates found in PEM");
    }
    return TResultOrError<void>();
}

TResultOrError<void> PrivateKeyAndCertificateMatch(
    std::string_view privateKey,
    std::string_view certChain)
{
    TBioPtr certBio(
        BIO_new_mem_buf(certChain.data(), static_cast<int>(certChain.size())),
        BIO_free);
    if (!certBio) {
        return TErrorResponse(E_FAIL, "Failed to allocate BIO for certificate chain");
    }
    TX509Ptr cert(PEM_read_bio_X509(certBio.get(), nullptr, nullptr, nullptr), X509_free);
    if (!cert) {
        return TErrorResponse(E_FAIL, "Failed to parse certificate chain PEM");
    }
    TEvpPkeyPtr publicKey(X509_get_pubkey(cert.get()), EVP_PKEY_free);
    if (!publicKey) {
        return TErrorResponse(E_FAIL, "Failed to extract public key from certificate");
    }
    TBioPtr keyBio(
        BIO_new_mem_buf(privateKey.data(), static_cast<int>(privateKey.size())),
        BIO_free);
    if (!keyBio) {
        return TErrorResponse(E_FAIL, "Failed to allocate BIO for private key");
    }
    TEvpPkeyPtr privateKeyObj(
        PEM_read_bio_PrivateKey(keyBio.get(), nullptr, nullptr, nullptr),
        EVP_PKEY_free);
    if (!privateKeyObj) {
        return TErrorResponse(E_FAIL, "Failed to parse private key PEM");
    }
    if (EVP_PKEY_cmp(privateKeyObj.get(), publicKey.get()) != 1) {
        return TErrorResponse(E_FAIL, "Private key does not match certificate public key");
    }
    return TResultOrError<void>();
}

TResultOrError<void> ValidateIdentityCertificateWithRoot(
    std::string_view rootCertPem,
    std::string_view certChainPem)
{
    auto rootsResult = ParsePemCertificates(rootCertPem);
    auto identityChainResult = ParsePemCertificates(certChainPem);
    if (HasError(rootsResult.GetError()) || HasError(identityChainResult.GetError())) {
        return TErrorResponse(E_FAIL, "Failed to parse certificates for chain validation");
    }

    auto roots = rootsResult.ExtractResult();
    auto identityChain = identityChainResult.ExtractResult();
    if (roots.empty() || identityChain.empty()) {
        return TErrorResponse(E_FAIL, "Root or identity certificate chain is empty");
    }

    const X509* leaf = identityChain.front().get();
    if (X509_cmp_current_time(X509_get0_notBefore(leaf)) > 0 ||
        X509_cmp_current_time(X509_get0_notAfter(leaf)) < 0)
    {
        return TErrorResponse(E_FAIL, "Identity certificate is not currently valid");
    }

    TX509StorePtr store(X509_STORE_new(), X509_STORE_free);
    if (!store) {
        return TErrorResponse(E_FAIL, "Failed to create X509_STORE");
    }

    bool storeOk = true;
    for (const auto& root: roots) {
        if (X509_STORE_add_cert(store.get(), root.get()) != 1) {
            const auto error = ERR_peek_last_error();
            if (ERR_GET_LIB(error) != ERR_LIB_X509 ||
                ERR_GET_REASON(error) != X509_R_CERT_ALREADY_IN_HASH_TABLE)
            {
                storeOk = false;
                ERR_clear_error();
                break;
            }
            ERR_clear_error();
        }
    }

    if (!storeOk) {
        return TErrorResponse(E_FAIL, "Failed to add root certificates to X509_STORE");
    }

    TX509StackPtr untrusted(sk_X509_new_null(), sk_X509_free);
    if (!untrusted) {
        return TErrorResponse(E_FAIL, "Failed to allocate untrusted certificate stack");
    }
    bool chainOk = true;
    for (size_t i = 1; i < identityChain.size(); ++i) {
        if (sk_X509_push(untrusted.get(), identityChain[i].get()) == 0) {
            chainOk = false;
            break;
        }
    }
    if (!chainOk) {
        return TErrorResponse(E_FAIL, "Failed to build untrusted identity certificate chain");
    }

    TX509StoreCtxPtr ctx(X509_STORE_CTX_new(), X509_STORE_CTX_free);
    if (!ctx) {
        return TErrorResponse(E_FAIL, "Failed to create X509_STORE_CTX");
    }
    const bool initOk =
        X509_STORE_CTX_init(
            ctx.get(),
            store.get(),
            identityChain.front().get(),
            untrusted.get()) == 1;
    if (!initOk) {
        return TErrorResponse(E_FAIL, "Failed to initialize X509_STORE_CTX");
    }
    if (X509_verify_cert(ctx.get()) != 1) {
        return TErrorResponse(E_FAIL, "X509_verify_cert failed for identity certificate chain");
    }
    return TResultOrError<void>();
}

TResultOrError<ui64> GetCertificateNotAfterTimestampSec(std::string_view certChainPem)
{
    auto chainResult = ParsePemCertificates(certChainPem);
    if (HasError(chainResult.GetError())) {
        return chainResult.GetError();
    }

    auto chain = chainResult.ExtractResult();
    if (chain.empty()) {
        return TErrorResponse(E_FAIL, "Identity certificate chain is empty");
    }

    const X509* leaf = chain.front().get();
    const ASN1_TIME* notAfter = X509_get0_notAfter(leaf);
    if (!notAfter) {
        return TErrorResponse(E_FAIL, "Failed to get certificate notAfter field");
    }

    tm tmValue{};
    if (ASN1_TIME_to_tm(notAfter, &tmValue) != 1) {
        return TErrorResponse(E_FAIL, "Failed to parse certificate notAfter field");
    }

    const time_t timestamp = timegm(&tmValue);
    if (timestamp < 0) {
        return TErrorResponse(E_FAIL, "Invalid certificate notAfter timestamp");
    }

    return static_cast<ui64>(timestamp);
}

TResultOrError<TString> ReadAndValidateRootCertificate(const TString& rootCertPath)
{
    auto pem = TryReadFile(rootCertPath);
    if (HasError(pem.GetError())) {
        return pem.GetError();
    }
    auto certValidity = IsValidPemCertificate(pem.GetResult());
    if (HasError(certValidity.GetError())) {
        const auto message = TStringBuilder()
            << "Root certificate " << rootCertPath.Quote()
            << " is invalid PEM X509";
        return TErrorResponse(E_FAIL, message);
    }
    return pem.ExtractResult();
}

TResultOrError<grpc_core::PemKeyCertPairList> ReadAndValidateIdentityPair(
    const TCertificateFiles& files)
{
    auto privateKey = TryReadFile(files.PrivateKeyPath);
    if (HasError(privateKey.GetError())) {
        return privateKey.GetError();
    }
    auto certChain = TryReadFile(files.CertChainPath);
    if (HasError(certChain.GetError())) {
        return certChain.GetError();
    }
    auto keyMatchesCert = PrivateKeyAndCertificateMatch(
        privateKey.GetResult(),
        certChain.GetResult());
    if (HasError(keyMatchesCert.GetError())) {
        const auto message = TStringBuilder()
            << "Certificate/key mismatch for cert="
            << files.CertChainPath.Quote()
            << " key=" << files.PrivateKeyPath.Quote();
        return TErrorResponse(E_FAIL, message);
    }

    grpc_core::PemKeyCertPairList result;
    result.emplace_back(privateKey.ExtractResult(), certChain.ExtractResult());
    return result;
}

}   // namespace NCloud::NTlsUtils
