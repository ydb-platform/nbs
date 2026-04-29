#include "tls_certificate_provider.h"
#include "tls_utils.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include "src/core/lib/gprpp/ref_counted_ptr.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_distributor.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/folder/dirut.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <exception>
#include <memory>
#include <mutex>
#include <thread>
#include <tuple>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

using grpc_core::PemKeyCertPairList;
using grpc_core::RefCountedPtr;

using TCertificateFiles = NCloud::TCertificateFiles;

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TPeriodicCertificateProviderBase
{
public:
    struct TCertificateState
    {
        TCertificateFiles Files;
        y_absl::optional<PemKeyCertPairList> IdentityKeyCertPairs;
        NMonitoring::TDynamicCountersPtr Metrics;
    };

private:
    struct TPendingUpdate
    {
        NThreading::TPromise<void> Promise;
    };

    const ILoggingServicePtr Logging;
    const TString LogComponent;
    const NMonitoring::TDynamicCountersPtr ServerGroup;
    const TString RootCertPath;
    const TDuration RefreshIntervalSec;
    mutable std::mutex WakeupMutex;
    y_absl::optional<TString> RootCertificate;
    TVector<TCertificateState> Certificates;
    std::atomic<bool> Stopping = false;
    bool UpdateRequested = false;
    bool UpdateInProgress = false;
    bool Started = false;
    y_absl::optional<TPendingUpdate> PendingUpdate;
    std::condition_variable Wakeup;
    std::thread RefreshThread;

public:
    TLog Log;

public:
    TPeriodicCertificateProviderBase(
            ILoggingServicePtr logging,
            TString logComponent,
            NMonitoring::TDynamicCountersPtr serverGroup,
            TString rootCertPath,
            TVector<TCertificateFiles> certificates,
            TDuration refreshIntervalSec)
        : Logging(std::move(logging))
        , LogComponent(std::move(logComponent))
        , ServerGroup(std::move(serverGroup))
        , RootCertPath(std::move(rootCertPath))
        , RefreshIntervalSec(refreshIntervalSec)
    {
        Y_ENSURE(
            !certificates.empty(),
            "Certificates list should not be empty");

        for (const auto& certificate: certificates) {
            Y_ENSURE(certificate.PrivateKeyPath, "Empty PrivateKeyPath");
            Y_ENSURE(certificate.CertChainPath, "Empty CertChainPath");
            Certificates.push_back({
                .Files = certificate,
                .IdentityKeyCertPairs = y_absl::nullopt,
                .Metrics = {}
            });
        }
    }

    virtual ~TPeriodicCertificateProviderBase()
    {
        Stop();
    }

protected:
    TVector<TCertificateState> GetCertificateStates() const
    {
        return Certificates;
    }

    y_absl::optional<TString> GetRootCertificate() const
    {
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
        Stopping.store(false);

        Log = Logging->CreateLog(LogComponent);

        NMonitoring::TDynamicCountersPtr tlsMetricsGroup;
        if (ServerGroup) {
            tlsMetricsGroup =
                ServerGroup->GetSubgroup("subsystem", "certificates");
        }

        for (auto& certificate: Certificates) {
            NMonitoring::TDynamicCountersPtr certMetrics;
            if (tlsMetricsGroup) {
                certMetrics = tlsMetricsGroup->GetSubgroup(
                    "cert",
                    GetBaseName(certificate.Files.CertChainPath));
            }
            certificate.Metrics = std::move(certMetrics);
        }

        ForceUpdate();
        RefreshThread = std::thread([this] {
            while (!Stopping.load()) {
                std::unique_lock lock(WakeupMutex);
                const bool isRequested = Wakeup.wait_for(
                    lock,
                    std::chrono::seconds(RefreshIntervalSec.Seconds()),
                    [this] {
                        return Stopping.load() || UpdateRequested;
                    });
                if (Stopping.load()) {
                    return;
                }
                if (isRequested) {
                    Y_ABORT_UNLESS(PendingUpdate.has_value());
                } else {
                    PendingUpdate = CreatePendingUpdate();
                }
                UpdateRequested = false;
                UpdateInProgress = true;
                lock.unlock();

                try {
                    ForceUpdate();
                    lock.lock();
                    UpdateInProgress = false;
                    CompletePendingUpdate();
                    lock.unlock();
                } catch (...) {
                    lock.lock();
                    UpdateInProgress = false;
                    FailPendingUpdate(std::current_exception());
                    lock.unlock();
                }
            }
        });
    }

    void Stop()
    {
        if (!Started) {
            return;
        }
        Started = false;
        Stopping.store(true);
        Wakeup.notify_all();
        if (RefreshThread.joinable()) {
            RefreshThread.join();
        }
    }

    NThreading::TFuture<void> UpdateNow()
    {
        NThreading::TFuture<void> future;
        {
            std::lock_guard lock(WakeupMutex);
            if (UpdateInProgress || UpdateRequested) {
                Y_ABORT_UNLESS(PendingUpdate.has_value());
                future = PendingUpdate->Promise.GetFuture();
            } else {
                PendingUpdate = CreatePendingUpdate();
                future = PendingUpdate->Promise.GetFuture();
                UpdateRequested = true;
            }
        }
        Wakeup.notify_all();
        return future;
    }

private:
    TPendingUpdate CreatePendingUpdate()
    {
        return {.Promise = NThreading::NewPromise<void>()};
    }

    void CompletePendingUpdate()
    {
        Y_ABORT_UNLESS(PendingUpdate.has_value());
        PendingUpdate->Promise.SetValue();
        PendingUpdate = y_absl::nullopt;
    }

    void FailPendingUpdate(std::exception_ptr ex)
    {
        Y_ABORT_UNLESS(PendingUpdate.has_value());
        PendingUpdate->Promise.SetException(std::move(ex));
        PendingUpdate = y_absl::nullopt;
    }

    void ForceUpdate()
    {
        TVector<y_absl::optional<PemKeyCertPairList>> identities(
            Certificates.size());
        TVector<y_absl::optional<ui64>> certNotAfterTs(Certificates.size());
        bool hasUpdates = false;

        const bool needsRoot = !!GetRootCertPath();
        TResultOrError<TString> rootResult = TString{};
        if (needsRoot) {
            rootResult =
                NTlsUtils::ReadAndValidateRootCertificate(RootCertPath);
            if (HasError(rootResult.GetError())) {
                STORAGE_WARN(
                    "Root certificate update is skipped: "
                    << rootResult.GetError().GetMessage());
            }
        }

        y_absl::optional<TString> newRoot;
        const bool hasNewRoot =
            !needsRoot || !HasError(rootResult.GetError());
        if (needsRoot && hasNewRoot) {
            newRoot = rootResult.ExtractResult();
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

            if (needsRoot && hasNewRoot && identities[i].has_value()) {
                const auto& pair = identities[i]->front();
                auto rootValidation =
                    NTlsUtils::ValidateIdentityCertificateWithRoot(
                        *newRoot,
                        pair.cert_chain());
                if (HasError(rootValidation.GetError())) {
                    STORAGE_WARN(
                        "Identity certificate chain from "
                        << files.CertChainPath.Quote()
                        << " is not trusted by root CA "
                        << RootCertPath.Quote()
                        << ", continue without failing update");
                }
            }

            if (identities[i].has_value()) {
                const auto& pair = identities[i]->front();
                auto notAfterTs =
                    NTlsUtils::GetCertificateNotAfterTimestampSec(
                        pair.cert_chain());
                if (HasError(notAfterTs.GetError())) {
                    STORAGE_WARN(
                        "Unable to parse certificate notAfter date for "
                        << files.CertChainPath.Quote() << ": "
                        << FormatError(notAfterTs.GetError()));
                    identities[i] = y_absl::nullopt;
                } else {
                    certNotAfterTs[i] = notAfterTs.ExtractResult();
                }
            }
        }

        bool rootChanged = false;
        if (!needsRoot) {
            rootChanged = RootCertificate.has_value();
            RootCertificate = y_absl::nullopt;
        } else if (hasNewRoot) {
            rootChanged = newRoot != RootCertificate;
            RootCertificate = newRoot;
        }
        hasUpdates = hasUpdates || rootChanged;

        for (size_t i = 0; i < Certificates.size(); ++i) {
            if (Certificates[i].Metrics && certNotAfterTs[i].has_value()) {
                *Certificates[i].Metrics->GetCounter(
                    "ExpireTs",
                    false) = *certNotAfterTs[i];
            }

            if (!identities[i].has_value()) {
                continue;
            }
            const bool identityChanged =
                identities[i] != Certificates[i].IdentityKeyCertPairs;
            if (!rootChanged && !identityChanged) {
                continue;
            }

            Certificates[i].IdentityKeyCertPairs = identities[i];
            hasUpdates = true;
        }

        if (hasUpdates) {
            static_cast<TDerived*>(this)->OnCertificateUpdated();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPeriodicCertificateProvider final
    : public grpc_tls_certificate_provider
    , public TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>
{
private:
    RefCountedPtr<grpc_tls_certificate_distributor> Distributor;

public:
    friend class TPeriodicCertificateProviderBase<TPeriodicCertificateProvider>;

    TPeriodicCertificateProvider(
            ILoggingServicePtr logging,
            TString logComponent,
            NMonitoring::TDynamicCountersPtr serverGroup,
            TString rootCertPath,
            TVector<TCertificateFiles> certificates,
            TDuration refreshIntervalSec)
        : TPeriodicCertificateProviderBase<
            TPeriodicCertificateProvider>(
              std::move(logging),
              std::move(logComponent),
              std::move(serverGroup),
              std::move(rootCertPath),
              std::move(certificates),
              refreshIntervalSec)
        , Distributor(
            grpc_core::MakeRefCounted<grpc_tls_certificate_distributor>())
    {
        Distributor->SetWatchStatusCallback(
            [this](
                TString certName,
                bool rootBeingWatched,
                bool identityBeingWatched)
            {
                Y_UNUSED(rootBeingWatched);
                Y_UNUSED(identityBeingWatched);
                OnWatchStatusChanged(std::move(certName));
            });

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
        static grpc_core::UniqueTypeName::Factory kFactory(
            "NCloudPeriodicCertificateProvider");
        return kFactory.Create();
    }

    NThreading::TFuture<void> UpdateNow()
    {
        return TPeriodicCertificateProviderBase<
            TPeriodicCertificateProvider>::UpdateNow();
    }

    void Start()
    {
        TPeriodicCertificateProviderBase<
            TPeriodicCertificateProvider>::Start();
    }

    void Stop()
    {
        TPeriodicCertificateProviderBase<
            TPeriodicCertificateProvider>::Stop();
    }

private:
    void PublishCerts(const TString& certName)
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
                identityPairs.insert(
                    identityPairs.end(),
                    pairs.begin(),
                    pairs.end());
            }
        }

        if (!identityInvalid && !identityPairs.empty() &&
            (!needsRoot || !rootInvalid))
        {
            Distributor->SetKeyMaterials(
                certName,
                rootCertificate,
                std::move(identityPairs));
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
        if (certName) {
            STORAGE_WARN(
                "Unexpected non-empty certName in watch callback: "
                << certName.Quote());
        }
    }

    void OnCertificateUpdated()
    {
        PublishCerts("");
    }

    int CompareImpl(const grpc_tls_certificate_provider* other) const override
    {
        return QsortCompare(
            static_cast<const grpc_tls_certificate_provider*>(this),
             other);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPeriodicCertificateProviderImpl final
    : public ICertificateProvider
{
private:
    grpc_core::RefCountedPtr<TPeriodicCertificateProvider> Provider;

public:
    TPeriodicCertificateProviderImpl(
        ILoggingServicePtr logging,
        TString logComponent,
        NMonitoring::TDynamicCountersPtr serverGroup,
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
    {
        Provider = grpc_core::RefCountedPtr<
            TPeriodicCertificateProvider>(
                new TPeriodicCertificateProvider(
                    std::move(logging),
                    std::move(logComponent),
                    std::move(serverGroup),
                    std::move(rootCertPath),
                    std::move(certificates),
                    refreshIntervalSec));
    }

    grpc_tls_certificate_provider* c_provider() override
    {
        return Provider.get();
    }

    NThreading::TFuture<void> UpdateCertificates() override
    {
        return Provider->UpdateNow();
    }

    void Start() override
    {
        Provider->Start();
    }

    void Stop() override
    {
        Provider->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TCertificateRefresher
    : public ICertificateRefresher
{
    std::once_flag InitFlag;
    std::shared_ptr<ICertificateProvider> Provider;

    void Init(
        ILoggingServicePtr logging,
        TString logComponent,
        NMonitoring::TDynamicCountersPtr serverGroup,
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
    {
        bool initializedNow = false;
        std::call_once(InitFlag, [&] {
            ICertificateProviderPtr provider =
                std::make_shared<TPeriodicCertificateProviderImpl>(
                    std::move(logging),
                    std::move(logComponent),
                    std::move(serverGroup),
                    std::move(rootCertPath),
                    std::move(certificates),
                    refreshIntervalSec);
            std::atomic_store_explicit(
                &Provider,
                std::move(provider),
                std::memory_order_release);
            initializedNow = true;
        });

        if (!initializedNow) {
            throw TServiceError(E_INVALID_STATE)
                << "TCertificateRefresher is already initialized";
        }
    }

    std::shared_ptr<ICertificateProvider> GetCertificateProvider()
    {
        return std::atomic_load_explicit(&Provider, std::memory_order_acquire);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICertificateRefresherPtr CreateCertificateRefresher()
{
    return std::make_shared<TCertificateRefresher>();
}

ICertificateRefresherPtr GetCertificateRefresher()
{
    static ICertificateRefresherPtr refresher =
        CreateCertificateRefresher();
    return refresher;
}

}   // namespace NCloud
