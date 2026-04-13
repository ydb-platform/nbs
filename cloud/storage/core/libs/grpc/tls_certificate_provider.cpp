#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include "src/core/lib/gprpp/ref_counted_ptr.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_distributor.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h"

#include <util/stream/file.h>
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
#include <thread>
#include <tuple>
#include <vector>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

namespace {

using grpc_core::PemKeyCertPairList;
using grpc_core::RefCountedPtr;

using TCertificateFiles = NCloud::TCertificateFiles;

TLog Log;

template <typename TDerived>
class TPeriodicCertificateProviderBase
{
public:
    struct TCertificateState
    {
        TCertificateFiles Files;
        y_absl::optional<PemKeyCertPairList> IdentityKeyCertPairs;
    };

    TPeriodicCertificateProviderBase(
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
        : RootCertPath(std::move(rootCertPath))
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
    using TX509Ptr = std::unique_ptr<X509, decltype(&X509_free)>;

    static y_absl::optional<TString> TryReadFile(const TString& path)
    {
        try {
            TFileInput in(path);
            return in.ReadAll();
        } catch (const std::exception& e) {
            STORAGE_ERROR("Reading certificate file " << path.Quote() << " failed: " << e.what());
            return y_absl::nullopt;
        }
    }

    static bool IsValidPemCertificate(y_absl::string_view pem)
    {
        if (pem.empty()) {
            return false;
        }

        BIO* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
        if (!bio) {
            return false;
        }

        X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
        const bool ok = cert != nullptr;
        if (cert) {
            X509_free(cert);
        }
        BIO_free(bio);
        return ok;
    }

    static bool PrivateKeyAndCertificateMatch(
        y_absl::string_view privateKey,
        y_absl::string_view certChain)
    {
        BIO* certBio = BIO_new_mem_buf(certChain.data(), static_cast<int>(certChain.size()));
        if (!certBio) {
            return false;
        }
        X509* cert = PEM_read_bio_X509(certBio, nullptr, nullptr, nullptr);
        BIO_free(certBio);
        if (!cert) {
            return false;
        }
        EVP_PKEY* publicKey = X509_get_pubkey(cert);
        X509_free(cert);
        if (!publicKey) {
            return false;
        }
        BIO* keyBio = BIO_new_mem_buf(privateKey.data(), static_cast<int>(privateKey.size()));
        if (!keyBio) {
            EVP_PKEY_free(publicKey);
            return false;
        }
        EVP_PKEY* privateKeyObj = PEM_read_bio_PrivateKey(keyBio, nullptr, nullptr, nullptr);
        BIO_free(keyBio);
        if (!privateKeyObj) {
            EVP_PKEY_free(publicKey);
            return false;
        }
        const bool matches = EVP_PKEY_cmp(privateKeyObj, publicKey) == 1;
        EVP_PKEY_free(privateKeyObj);
        EVP_PKEY_free(publicKey);
        return matches;
    }

    static std::vector<TX509Ptr> ParsePemCertificates(y_absl::string_view pem)
    {
        std::vector<TX509Ptr> certs;
        BIO* bio = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
        if (!bio) {
            return {};
        }

        while (true) {
            X509* cert = PEM_read_bio_X509(bio, nullptr, nullptr, nullptr);
            if (cert == nullptr) {
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
                certs.clear();
                break;
            }
            certs.emplace_back(cert, X509_free);
        }

        BIO_free(bio);
        return certs;
    }

    static bool ValidateIdentityCertificateWithRoot(
        y_absl::string_view rootCertPem,
        y_absl::string_view certChainPem)
    {
        auto roots = ParsePemCertificates(rootCertPem);
        auto identityChain = ParsePemCertificates(certChainPem);
        if (roots.empty() || identityChain.empty()) {
            return false;
        }

        const X509* leaf = identityChain.front().get();
        if (X509_cmp_current_time(X509_get0_notBefore(leaf)) > 0 ||
            X509_cmp_current_time(X509_get0_notAfter(leaf)) < 0)
        {
            return false;
        }

        X509_STORE* store = X509_STORE_new();
        if (!store) {
            return false;
        }

        bool storeOk = true;
        for (const auto& root: roots) {
            if (X509_STORE_add_cert(store, root.get()) != 1) {
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
            X509_STORE_free(store);
            return false;
        }

        STACK_OF(X509)* untrusted = sk_X509_new_null();
        if (!untrusted) {
            X509_STORE_free(store);
            return false;
        }
        bool chainOk = true;
        for (size_t i = 1; i < identityChain.size(); ++i) {
            if (sk_X509_push(untrusted, identityChain[i].get()) == 0) {
                chainOk = false;
                break;
            }
        }
        if (!chainOk) {
            sk_X509_free(untrusted);
            X509_STORE_free(store);
            return false;
        }

        X509_STORE_CTX* ctx = X509_STORE_CTX_new();
        if (!ctx) {
            sk_X509_free(untrusted);
            X509_STORE_free(store);
            return false;
        }
        const bool initOk =
            X509_STORE_CTX_init(ctx, store, identityChain.front().get(), untrusted) == 1;
        const bool verifyOk = initOk && (X509_verify_cert(ctx) == 1);
        X509_STORE_CTX_free(ctx);
        sk_X509_free(untrusted);
        X509_STORE_free(store);
        return verifyOk;
    }

    static y_absl::optional<TString> ReadAndValidateRootCertificate(
        const TString& rootCertPath)
    {
        if (!rootCertPath) {
            return y_absl::nullopt;
        }
        auto pem = TryReadFile(rootCertPath);
        if (!pem.has_value()) {
            return y_absl::nullopt;
        }
        if (!IsValidPemCertificate(*pem)) {
            STORAGE_ERROR("Root certificate " << rootCertPath.Quote() << " is invalid PEM X509");
            return y_absl::nullopt;
        }
        return pem;
    }

    static y_absl::optional<PemKeyCertPairList> ReadAndValidateIdentityPair(
        const TCertificateFiles& files)
    {
        auto privateKey = TryReadFile(files.PrivateKeyPath);
        if (!privateKey.has_value()) {
            return y_absl::nullopt;
        }
        auto certChain = TryReadFile(files.CertChainPath);
        if (!certChain.has_value()) {
            return y_absl::nullopt;
        }
        if (!PrivateKeyAndCertificateMatch(*privateKey, *certChain)) {
            STORAGE_ERROR(
                "Certificate/key mismatch for cert="
                << files.CertChainPath.Quote()
                << " key=" << files.PrivateKeyPath.Quote());
            return y_absl::nullopt;
        }

        PemKeyCertPairList result;
        result.emplace_back(*privateKey, *certChain);
        return result;
    }

    NProto::TError ForceUpdate()
    {
        TVector<y_absl::optional<PemKeyCertPairList>> identities(Certificates.size());
        TVector<bool> validSnapshots(Certificates.size(), false);
        TVector<bool> rootInvalid(Certificates.size(), false);
        TVector<bool> identityInvalid(Certificates.size(), false);
        TVector<size_t> updatedIndices;
        TVector<std::tuple<size_t, bool, bool>> errors;

        const bool needsRoot = !!GetRootCertPath();
        auto root = ReadAndValidateRootCertificate(RootCertPath);
        const bool hasRoot = root.has_value();

        for (size_t i = 0; i < Certificates.size(); ++i) {
            const auto& files = Certificates[i].Files;
            identities[i] = ReadAndValidateIdentityPair(files);

            if (needsRoot && hasRoot && identities[i].has_value()) {
                const auto& pair = identities[i]->front();
                if (!ValidateIdentityCertificateWithRoot(*root, pair.cert_chain())) {
                    STORAGE_ERROR(
                        "Identity certificate chain from "
                        << files.CertChainPath.Quote()
                        << " is not trusted by root CA "
                        << RootCertPath.Quote());
                    identities[i] = y_absl::nullopt;
                }
            }

            validSnapshots[i] = (!needsRoot || hasRoot) && identities[i].has_value();
            rootInvalid[i] = needsRoot && !hasRoot;
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
};

class TPeriodicCertificateProvider final
    : public grpc_tls_certificate_provider
    , public TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>
{
public:
    friend class TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>;

    TPeriodicCertificateProvider(
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
        : TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>(
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

private:
    RefCountedPtr<grpc_tls_certificate_distributor> Distributor;
};

class TPeriodicCertificateProviderWrapper final
    : public ICertificateProvider
{
public:
    TPeriodicCertificateProviderWrapper(
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
    {
        Provider = grpc_core::RefCountedPtr<TPeriodicCertificateProvider>(
            new TPeriodicCertificateProvider(
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

private:
    grpc_core::RefCountedPtr<TPeriodicCertificateProvider> Provider;
};

}   // namespace

std::shared_ptr<ICertificateProvider> CreatePeriodicCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshIntervalSec)
{
    return std::make_shared<TPeriodicCertificateProviderWrapper>(
        std::move(rootCertPath),
        std::move(certificates),
        refreshIntervalSec);
}

}   // namespace NCloud
