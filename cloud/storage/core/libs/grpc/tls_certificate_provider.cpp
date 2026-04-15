#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include "src/core/lib/gprpp/ref_counted_ptr.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_distributor.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h"

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/system/yassert.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/x509.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <string_view>
#include <thread>
#include <tuple>
#include <vector>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

namespace {

using grpc_core::PemKeyCertPairList;
using grpc_core::RefCountedPtr;

using TCertificateFiles = NCloud::TCertificateFiles;

using TBioPtr = std::unique_ptr<BIO, decltype(&BIO_free)>;
using TX509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;
using TEvpPkeyPtr = std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)>;
using TX509StorePtr = std::unique_ptr<X509_STORE, decltype(&X509_STORE_free)>;
using TX509StoreCtxPtr = std::unique_ptr<X509_STORE_CTX, decltype(&X509_STORE_CTX_free)>;
using TX509StackPtr = std::unique_ptr<STACK_OF(X509), decltype(&sk_X509_free)>;

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

TResultOrError<std::vector<TX509Ptr>> ParsePemCertificates(std::string_view pem)
{
    std::vector<TX509Ptr> certificates;
    TBioPtr bio(BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size())), BIO_free);
    if (!bio) {
        return certificates;
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

TResultOrError<TString> ReadAndValidateRootCertificate(
    const TString& rootCertPath)
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

TResultOrError<PemKeyCertPairList> ReadAndValidateIdentityPair(
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

    PemKeyCertPairList result;
    result.emplace_back(privateKey.ExtractResult(), certChain.ExtractResult());
    return result;
}

template <typename TDerived>
class TPeriodicCertificateProviderBase
{
public:
    struct TCertificateState
    {
        TCertificateFiles Files;
        y_absl::optional<PemKeyCertPairList> IdentityKeyCertPairs;
    };

    TLog Log;

private:
    const TString RootCertPath;
    const TDuration RefreshIntervalSec;
    mutable std::mutex Mutex;
    y_absl::optional<TString> RootCertificate;
    TVector<TCertificateState> Certificates;
    std::atomic<bool> Stopping = false;
    bool Started = false;
    std::condition_variable Wakeup;
    std::thread RefreshThread;

public:
    TPeriodicCertificateProviderBase(
        TLog log,
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
        : Log(std::move(log))
        , RootCertPath(std::move(rootCertPath))
        , RefreshIntervalSec(refreshIntervalSec)
    {
        Y_ENSURE(!certificates.empty(), "Certificates list should not be empty");

        for (const auto& certificate: certificates) {
            Y_ENSURE(certificate.PrivateKeyPath, "Empty PrivateKeyPath");
            Y_ENSURE(certificate.CertChainPath, "Empty CertChainPath");
            Certificates.push_back({certificate});
        }
    }

    virtual ~TPeriodicCertificateProviderBase()
    {
        if (!Started) {
            return;
        }
        Stopping.store(true);
        Wakeup.notify_all();
        if (RefreshThread.joinable()) {
            RefreshThread.join();
        }
    }

protected:
    size_t GetCertificatesCount() const
    {
        std::lock_guard lock(Mutex);
        return Certificates.size();
    }

    size_t ResolveCertificateIndex(const TString& certName) const
    {
        if (!certName) {
            return 0;
        }

        if (RootCertPath && certName == RootCertPath) {
            return 0;
        }

        for (size_t i = 0; i < Certificates.size(); ++i) {
            const auto& files = Certificates[i].Files;
            if (files.CertChainPath == certName) {
                return i;
            }
        }

        STORAGE_WARN(
            "Unknown certName " << certName.Quote()
            << " in watch callback. Fallback to first certificate triple");
        return 0;
    }

    TCertificateState GetCertificateState(size_t index) const
    {
        std::lock_guard lock(Mutex);
        return Certificates[index];
    }

    TVector<TCertificateState> GetCertificateStates() const
    {
        std::lock_guard lock(Mutex);
        return Certificates;
    }

    y_absl::optional<TString> GetRootCertificate() const
    {
        std::lock_guard lock(Mutex);
        return RootCertificate;
    }

    const TString& GetRootCertPath() const
    {
        return RootCertPath;
    }

    void Start()
    {
        if (Started) {
            return;
        }
        Started = true;

        auto error = ForceUpdate();
        if (HasError(error)) {
            STORAGE_WARN("Initial certificate update failed: " << FormatError(error));
        }
        RefreshThread = std::thread([this] {
            while (!Stopping.load()) {
                std::unique_lock lock(Mutex);
                if (Wakeup.wait_for(
                        lock,
                        std::chrono::seconds(RefreshIntervalSec.Seconds()),
                        [this] { return Stopping.load(); }))
                {
                    return;
                }
                lock.unlock();
                auto error = ForceUpdate();
                if (HasError(error)) {
                    STORAGE_WARN("Periodic certificate update failed: " << FormatError(error));
                }
            }
        });
    }

    NProto::TError UpdateNow()
    {
        return ForceUpdate();
    }

private:
    NProto::TError ForceUpdate()
    {
        TVector<y_absl::optional<PemKeyCertPairList>> identities(Certificates.size());
        TVector<bool> validSnapshots(Certificates.size(), false);
        TVector<bool> rootInvalid(Certificates.size(), false);
        TVector<bool> identityInvalid(Certificates.size(), false);
        TVector<size_t> updatedIndices;
        TVector<std::tuple<size_t, bool, bool>> errors;

        const bool needsRoot = !!GetRootCertPath();
        TResultOrError<TString> rootResult = TString{};
        if (needsRoot) {
            rootResult = ReadAndValidateRootCertificate(RootCertPath);
            if (HasError(rootResult.GetError())) {
                STORAGE_ERROR(rootResult.GetError().GetMessage());
            }
        }

        y_absl::optional<TString> root;
        const bool hasRoot = !needsRoot || !HasError(rootResult.GetError());
        if (needsRoot && !HasError(rootResult.GetError())) {
            root = rootResult.ExtractResult();
        }

        for (size_t i = 0; i < Certificates.size(); ++i) {
            const auto& files = Certificates[i].Files;
            auto identityResult = ReadAndValidateIdentityPair(files);
            if (!HasError(identityResult.GetError())) {
                identities[i] = identityResult.ExtractResult();
            } else {
                STORAGE_ERROR(identityResult.GetError().GetMessage());
            }

            if (needsRoot && hasRoot && identities[i].has_value()) {
                const auto& pair = identities[i]->front();
                auto rootValidation = ValidateIdentityCertificateWithRoot(*root, pair.cert_chain());
                if (HasError(rootValidation.GetError())) {
                    STORAGE_ERROR(
                        "Identity certificate chain from "
                        << files.CertChainPath.Quote()
                        << " is not trusted by root CA "
                        << RootCertPath.Quote());
                    identities[i] = y_absl::nullopt;
                }
            }

            validSnapshots[i] = hasRoot && identities[i].has_value();
            rootInvalid[i] = !hasRoot;
            identityInvalid[i] = !identities[i].has_value();
        }

        {
            std::lock_guard lock(Mutex);
            const bool rootChanged = root != RootCertificate;
            RootCertificate = root;

            for (size_t i = 0; i < Certificates.size(); ++i) {
                if (!validSnapshots[i]) {
                    errors.emplace_back(i, rootInvalid[i], identityInvalid[i]);
                    continue;
                }

                const bool identityChanged = identities[i] != Certificates[i].IdentityKeyCertPairs;
                if (!rootChanged && !identityChanged) {
                    continue;
                }

                Certificates[i].IdentityKeyCertPairs = identities[i];
                updatedIndices.push_back(i);
            }
        }

        for (const auto& [index, rootErr, identityErr]: errors) {
            static_cast<TDerived*>(this)->OnCertificateError(index, rootErr, identityErr);
        }
        for (const auto& index: updatedIndices) {
            static_cast<TDerived*>(this)->OnCertificateUpdated(index);
        }

        if (!errors.empty()) {
            return MakeError(E_FAIL);
        }
        return NProto::TError{};
    }
};

class TPeriodicCertificateProvider final
    : public grpc_tls_certificate_provider
    , public TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>
{
private:
    RefCountedPtr<grpc_tls_certificate_distributor> Distributor;

public:
    friend class TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>;

    TPeriodicCertificateProvider(
        TLog log,
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
        : TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>(
            std::move(log),
            std::move(rootCertPath),
            std::move(certificates),
            refreshIntervalSec)
        , Distributor(grpc_core::MakeRefCounted<grpc_tls_certificate_distributor>())
    {
        Distributor->SetWatchStatusCallback(
            [this](TString certName, bool rootBeingWatched, bool identityBeingWatched) {
                Y_UNUSED(rootBeingWatched);
                Y_UNUSED(identityBeingWatched);
                OnWatchStatusChanged(std::move(certName));
            });

        Start();
    }

    ~TPeriodicCertificateProvider() override
    {
        Distributor->SetWatchStatusCallback(nullptr);
    }

    RefCountedPtr<grpc_tls_certificate_distributor> distributor() const override
    {
        return Distributor;
    }

    grpc_core::UniqueTypeName type() const override
    {
        static grpc_core::UniqueTypeName::Factory kFactory("NCloudPeriodicCertificateProvider");
        return kFactory.Create();
    }

    NProto::TError UpdateNow()
    {
        return TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>::UpdateNow();
    }

private:
    void PublishAggregatedStateForWatch(const TString& certName)
    {
        auto states = GetCertificateStates();
        const auto rootCertificate = GetRootCertificate();

        const bool needsRoot = !!GetRootCertPath();
        const bool rootInvalid = needsRoot && !rootCertificate.has_value();

        bool identityInvalid = false;
        PemKeyCertPairList identityPairs;

        for (const auto& state: states) {
            if (!state.IdentityKeyCertPairs.has_value()) {
                identityInvalid = true;
            } else {
                const auto& pairs = *state.IdentityKeyCertPairs;
                identityPairs.insert(identityPairs.end(), pairs.begin(), pairs.end());
            }
        }

        if (!identityInvalid && !identityPairs.empty() &&
            (!needsRoot || !rootInvalid))
        {
            Distributor->SetKeyMaterials(certName, rootCertificate, std::move(identityPairs));
            return;
        }

        Distributor->SetErrorForCert(
            certName,
            needsRoot && rootInvalid
                ? GRPC_ERROR_CREATE("Unable to get valid root certificates.")
                : y_absl::OkStatus(),
            identityInvalid || identityPairs.empty()
                ? GRPC_ERROR_CREATE("Unable to get valid identity certificates.")
                : y_absl::OkStatus());
    }

    void OnWatchStatusChanged(TString certName)
    {
        if (!certName) {
            PublishAggregatedStateForWatch(certName);
            return;
        }

        STORAGE_WARN(
            "Unexpected non-empty certName in watch callback: "
            << certName.Quote() << ". Publish aggregated state for this watch.");
        PublishAggregatedStateForWatch(certName);
    }

    void OnCertificateUpdated(size_t index)
    {
        Y_UNUSED(index);
        PublishAggregatedStateForWatch("");
    }

    void OnCertificateError(size_t index, bool rootInvalid, bool identityInvalid)
    {
        Y_UNUSED(index);
        Y_UNUSED(rootInvalid);
        Y_UNUSED(identityInvalid);
        PublishAggregatedStateForWatch("");
    }

    int CompareImpl(const grpc_tls_certificate_provider* other) const override
    {
        return QsortCompare(static_cast<const grpc_tls_certificate_provider*>(this), other);
    }
};

class TPeriodicCertificateProviderWrapper final
    : public ICertificateProvider
{
private:
    grpc_core::RefCountedPtr<TPeriodicCertificateProvider> Provider;

public:
    TPeriodicCertificateProviderWrapper(
        TLog log,
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
    {
        Provider = grpc_core::RefCountedPtr<TPeriodicCertificateProvider>(
            new TPeriodicCertificateProvider(
                std::move(log),
                std::move(rootCertPath),
                std::move(certificates),
                refreshIntervalSec));
    }

    grpc_tls_certificate_provider* c_provider() override
    {
        return Provider.get();
    }

    NThreading::TFuture<NProto::TError> UpdateCertificates() override
    {
        return NThreading::MakeFuture(Provider->UpdateNow());
    }
};

}   // namespace

std::shared_ptr<ICertificateProvider> CreatePeriodicCertificateProvider(
    TLog log,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshIntervalSec)
{
    return std::make_shared<TPeriodicCertificateProviderWrapper>(
        std::move(log),
        std::move(rootCertPath),
        std::move(certificates),
        refreshIntervalSec);
}

}   // namespace NCloud
