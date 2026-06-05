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

////////////////////////////////////////////////////////////////////////////////

using TBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;
using TX509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;
using TEvpPkeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using TX509StorePtr = std::unique_ptr<X509_STORE, decltype(&X509_STORE_free)>;
using TX509StoreCtxPtr =
    std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)>;
using TX509StackPtr = std::unique_ptr<STACK_OF(X509), decltype(&sk_X509_free)>;

////////////////////////////////////////////////////////////////////////////////

// Helper RAII-style class to clear Ssl error
struct TSslErrorQueueGuard
{
    ~TSslErrorQueueGuard()
    {
        ERR_clear_error();
    }
};

////////////////////////////////////////////////////////////////////////////////

TString GetLastOpenSslError()
{
    const ui64 error = ERR_peek_last_error();
    if (error == 0) {
        return {};
    }
    const char* lib = ERR_lib_error_string(error);
    const char* reason = ERR_reason_error_string(error);

    TStringBuilder result;
    if (lib) {
        result << lib;
    }
    if (reason) {
        if (!result.empty()) {
            result << ": ";
        }
        result << reason;
    }

    if (result.empty()) {
        // No textual description available. Try to use legacy
        // error reporting
        char buffer[256];
        ERR_error_string_n(error, buffer, sizeof(buffer));
        result << buffer;
    }

    return result;
}

TString OpenSslErrorToString(TStringBuf message)
{
    const auto details = GetLastOpenSslError();
    if (details.empty()) {
        return TString(message);
    }
    return TStringBuilder() << message << ": " << details;
}

TErrorResponse MakeOpenSslError(TStringBuf message)
{
    const ui64 error = ERR_peek_last_error();
    return {
        MAKE_SYSTEM_ERROR(ERR_GET_REASON(error)),
        OpenSslErrorToString(message)};
}

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TVector<TX509Ptr>> ParsePemCertificates(TStringBuf pem)
{
    TSslErrorQueueGuard errorGuard;

    TVector<TX509Ptr> certificates;
    TBioPtr bio(
        BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())),
        BIO_free);
    if (!bio) {
        return MakeOpenSslError("Failed to allocate BIO for PEM certificates");
    }

    while (true) {
        X509* cert = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);
        if (cert != nullptr) {
            certificates.emplace_back(cert, X509_free);
            continue;
        }

        const ui64 error = ERR_peek_last_error();
        if (error == 0) {
            break;
        }

        // Not an error. This way OpenSSL indicates the end of cert chain.
        if (ERR_GET_LIB(error) == ERR_LIB_PEM &&
            ERR_GET_REASON(error) == PEM_R_NO_START_LINE)
        {
            break;
        }

        return MakeOpenSslError("Failed to parse PEM certificates");
    }

    return certificates;
}

TResultOrError<TVector<TX509Ptr>> ParseNonEmptyPemCertificates(
    TStringBuf pem,
    TStringBuf description)
{
    auto result = ParsePemCertificates(pem);
    if (HasError(result.GetError())) {
        return result.GetError();
    }
    if (result.GetResult().empty()) {
        const auto message = TStringBuilder()
            << description << " contains no certificates";
        return TErrorResponse(E_INVALID_STATE, message);
    }
    return result;
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
        return TErrorResponse(E_IO, message);
    }
}

TResultOrError<void> IsValidPemCertificate(TStringBuf pem)
{
    if (pem.empty()) {
        return TErrorResponse(E_INVALID_STATE, "PEM certificate is empty");
    }

    auto parseResult = ParseNonEmptyPemCertificates(pem, "PEM");
    if (HasError(parseResult.GetError())) {
        return parseResult.GetError();
    }
    return {};
}

TResultOrError<void> PrivateKeyAndCertificateMatch(
    TStringBuf privateKey,
    TStringBuf certChain)
{
    TSslErrorQueueGuard errorGuard;

    TBioPtr certBio(
        BIO_new_mem_buf(certChain.data(), static_cast<int>(certChain.size())),
        BIO_free);
    if (!certBio) {
        return MakeOpenSslError(
            "Failed to allocate BIO for certificate chain");
    }
    TX509Ptr cert(
        PEM_read_bio_X509(certBio.get(), nullptr, nullptr, nullptr),
        X509_free);
    if (!cert) {
        return MakeOpenSslError("Failed to parse certificate chain PEM");
    }
    TEvpPkeyPtr publicKey(X509_get_pubkey(cert.get()), EVP_PKEY_free);
    if (!publicKey) {
        return MakeOpenSslError(
            "Failed to extract public key from certificate");
    }
    TBioPtr keyBio(
        BIO_new_mem_buf(privateKey.data(), static_cast<int>(privateKey.size())),
        BIO_free);
    if (!keyBio) {
        return MakeOpenSslError("Failed to allocate BIO for private key");
    }
    TEvpPkeyPtr privateKeyObj(
        PEM_read_bio_PrivateKey(keyBio.get(), nullptr, nullptr, nullptr),
        EVP_PKEY_free);
    if (!privateKeyObj) {
        return MakeOpenSslError("Failed to parse private key PEM");
    }
    if (EVP_PKEY_cmp(privateKeyObj.get(), publicKey.get()) != 1) {
        return TErrorResponse(
            E_INVALID_STATE,
            "Private key does not match certificate public key");
    }
    return {};
}

TResultOrError<void> ValidateIdentityCertificateWithRoot(
    TStringBuf rootCertPem,
    TStringBuf certChainPem)
{
    TSslErrorQueueGuard errorGuard;

    auto rootsResult =
        ParseNonEmptyPemCertificates(rootCertPem, "Root certificate chain");
    if (HasError(rootsResult.GetError())) {
        return rootsResult.GetError();
    }
    auto identityChainResult = ParseNonEmptyPemCertificates(
        certChainPem,
        "Identity certificate chain");
    if (HasError(identityChainResult.GetError())) {
        return identityChainResult.GetError();
    }

    auto roots = rootsResult.ExtractResult();
    auto identityChain = identityChainResult.ExtractResult();

    const X509* leaf = identityChain.front().get();
    if (X509_cmp_current_time(X509_get0_notBefore(leaf)) > 0 ||
        X509_cmp_current_time(X509_get0_notAfter(leaf)) < 0)
    {
        return TErrorResponse(
            E_INVALID_STATE,
            "Identity certificate is not currently valid");
    }

    TX509StorePtr store(X509_STORE_new(), X509_STORE_free);
    if (!store) {
        return MakeOpenSslError("Failed to create X509_STORE");
    }

    for (const auto& root: roots) {
        if (X509_STORE_add_cert(store.get(), root.get()) == 1) {
            continue;
        }
        const ui64 error = ERR_peek_last_error();
        if (ERR_GET_LIB(error) == ERR_LIB_X509 &&
            ERR_GET_REASON(error) == X509_R_CERT_ALREADY_IN_HASH_TABLE)
        {
            // Duplicate root certificate, not an error.
            ERR_clear_error();
            continue;
        }
        return MakeOpenSslError(
            "Failed to add root certificates to X509_STORE");
    }

    TX509StackPtr untrusted(sk_X509_new_null(), sk_X509_free);
    if (!untrusted) {
        return MakeOpenSslError(
            "Failed to allocate untrusted certificate stack");
    }
    bool chainOk = true;
    for (size_t i = 1; i < identityChain.size(); ++i) {
        if (sk_X509_push(untrusted.get(), identityChain[i].get()) == 0) {
            chainOk = false;
            break;
        }
    }
    if (!chainOk) {
        return MakeOpenSslError(
            "Failed to build untrusted identity certificate chain");
    }

    TX509StoreCtxPtr ctx(X509_STORE_CTX_new(), X509_STORE_CTX_free);
    if (!ctx) {
        return MakeOpenSslError("Failed to create X509_STORE_CTX");
    }
    const bool initOk =
        X509_STORE_CTX_init(
            ctx.get(),
            store.get(),
            identityChain.front().get(),
            untrusted.get()) == 1;
    if (!initOk) {
        return MakeOpenSslError("Failed to initialize X509_STORE_CTX");
    }
    if (X509_verify_cert(ctx.get()) != 1) {
        const int verifyError = X509_STORE_CTX_get_error(ctx.get());
        auto message = TStringBuilder()
            << "X509_verify_cert failed for identity certificate chain: "
            << X509_verify_cert_error_string(verifyError);
        return TErrorResponse(MAKE_SYSTEM_ERROR(verifyError), message);
    }
    return {};
}

TResultOrError<ui64> GetCertificateNotAfterTimestampSec(TStringBuf certChainPem)
{
    TSslErrorQueueGuard errorGuard;

    auto chainResult = ParseNonEmptyPemCertificates(
        certChainPem,
        "Identity certificate chain");
    if (HasError(chainResult.GetError())) {
        return chainResult.GetError();
    }

    auto chain = chainResult.ExtractResult();

    const X509* leaf = chain.front().get();
    const ASN1_TIME* notAfter = X509_get0_notAfter(leaf);
    if (!notAfter) {
        return TErrorResponse(
            E_INVALID_STATE,
            "Failed to get certificate notAfter field");
    }

    tm tmValue{};
    if (ASN1_TIME_to_tm(notAfter, &tmValue) != 1) {
        return MakeOpenSslError("Failed to parse certificate notAfter field");
    }

    const time_t timestamp = timegm(&tmValue);
    if (timestamp < 0) {
        return TErrorResponse(
            E_INVALID_STATE,
            "Invalid certificate notAfter timestamp");
    }

    return static_cast<ui64>(timestamp);
}

TResultOrError<TString> ReadAndValidateRootCertificate(
    const TString& rootCertPath)
{
    auto pem = TryReadFile(rootCertPath);
    if (HasError(pem.GetError())) {
        return pem.GetError();
    }
    auto certValidity = IsValidPemCertificate(pem.GetResult());
    if (HasError(certValidity.GetError())) {
        return certValidity.GetError();
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
        return keyMatchesCert.GetError();
    }

    grpc_core::PemKeyCertPairList result;
    result.emplace_back(privateKey.ExtractResult(), certChain.ExtractResult());
    return result;
}

TCertificatesUpdateResult UpdateCertificates(
    const TVector<TCertificatePair>& certificates,
    const TRootCaPair& root,
    TLog& log)
{
    TVector<TMaybe<PemKeyCertPairList>> identities(
        Certificates.size());
    TVector<TMaybe<ui64>> certNotAfterTs(Certificates.size());

    const TMaybe<TString> oldRoot = RootCertificate;
    const bool needsRoot = !!GetRootCertPath();
    if (!needsRoot) {
        RootCertificate = Nothing();
    } else {
        auto rootResult =
            NTlsUtils::ReadAndValidateRootCertificate(RootCertPath);
        if (HasError(rootResult.GetError())) {
            STORAGE_WARN(
                "Root certificate update is skipped: "
                << rootResult.GetError().GetMessage());
        } else {
            RootCertificate = rootResult.ExtractResult();
        }
    }

    for (size_t i = 0; i < Certificates.size(); ++i) {
        const auto& files = Certificates[i].Files;
        auto identityResult =
            NTlsUtils::ReadAndValidateIdentityPair(files);
        if (!HasError(identityResult.GetError())) {
            identities[i] = identityResult.ExtractResult();
        } else {
            STORAGE_WARN(
                "Identity certificate update is skipped for "
                << files.CertChainPath.Quote() << ": "
                << identityResult.GetError().GetMessage());
        }

        if (identities[i].Defined()) {
            const auto& pair = identities[i]->front();
            auto notAfterTs =
                NTlsUtils::GetCertificateNotAfterTimestampSec(
                    pair.cert_chain());
            if (HasError(notAfterTs.GetError())) {
                STORAGE_WARN(
                    "Unable to parse certificate notAfter date for "
                    << files.CertChainPath.Quote() << ": "
                    << FormatError(notAfterTs.GetError()));
                identities[i] = Nothing();
            } else {
                certNotAfterTs[i] = notAfterTs.ExtractResult();
            }
        }
    }

    const bool rootChanged = oldRoot != RootCertificate;
    bool hasUpdates = rootChanged;

    for (size_t i = 0; i < Certificates.size(); ++i) {
        if (Certificates[i].Metrics && certNotAfterTs[i].Defined()) {
            *Certificates[i].Metrics->GetCounter(
                "ExpireTs",
                false) = *certNotAfterTs[i];
        }

        if (!identities[i].Defined()) {
            continue;
        }
        const bool identityChanged =
            identities[i] != Certificates[i].IdentityKeyCertPairs;
        if (!rootChanged && !identityChanged) {
            continue;
        }

        Certificates[i].IdentityKeyCertPairs = *identities[i];
        hasUpdates = true;
    }

    if (hasUpdates || initial) {
        static_cast<TDerived*>(this)->OnCertificateUpdated();
    }
}

}   // namespace NCloud::NTlsUtils
