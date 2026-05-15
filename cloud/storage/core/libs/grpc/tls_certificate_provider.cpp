#include "tls_certificate_provider.h"
#include "tls_utils.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include "src/core/lib/gprpp/ref_counted_ptr.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_distributor.h"
#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/folder/dirut.h>
#include <util/generic/maybe.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/system/condvar.h>
#include <util/system/thread.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <atomic>
#include <memory>
#include <mutex>
#include <tuple>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

using grpc_core::PemKeyCertPairList;
using grpc_core::RefCountedPtr;

using TCertificateFiles = NCloud::TCertificateFiles;

////////////////////////////////////////////////////////////////////////////////

void ValidateCertificates(
    const TVector<TCertificateFiles>& certificates,
    bool requireNonEmpty)
{
    if (requireNonEmpty && certificates.empty()) {
        ythrow yexception()
            << "Certificates list should not be empty";
    }

    for (size_t i = 0; i < certificates.size(); ++i) {
        const auto& certificate = certificates[i];
        if (!certificate.PrivateKeyPath) {
            ythrow yexception()
                << "Empty PrivateKeyPath for certificate #"
                << i;
        }
        if (!certificate.CertChainPath) {
            ythrow yexception()
                << "Empty CertChainPath for certificate #"
                << i;
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& fileName)
{
    TFileInput in(fileName);
    return in.ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TDerived>
class TPeriodicCertificateProviderBase
{
public:
    struct TCertificateState
    {
        TCertificateFiles Files;
        TMaybe<PemKeyCertPairList> IdentityKeyCertPairs;
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
    mutable TMutex WakeupMutex;
    TMaybe<TString> RootCertificate;
    TVector<TCertificateState> Certificates;
    std::atomic<bool> Stopping = false;
    bool UpdateRequested = false;
    bool UpdateInProgress = false;
    bool Started = false;
    TMaybe<TPendingUpdate> PendingUpdate;
    TCondVar Wakeup;
    std::unique_ptr<TThread> RefreshThread;

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
        for (const auto& certificate: certificates) {
            Certificates.push_back({
                .Files = certificate,
                .IdentityKeyCertPairs = Nothing(),
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

    TMaybe<TString> GetRootCertificate() const
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
        RefreshThread = std::make_unique<TThread>([this] { RunRefreshThread(); });
        RefreshThread->Start();
    }

    void Stop()
    {
        if (!Started) {
            return;
        }
        Started = false;
        Stopping.store(true);
        Wakeup.BroadCast();
        if (RefreshThread) {
            RefreshThread->Join();
            RefreshThread.reset();
        }
    }

    NThreading::TFuture<void> UpdateNow()
    {
        NThreading::TFuture<void> future;
        with_lock (WakeupMutex) {
            if (UpdateInProgress || UpdateRequested) {
                STORAGE_VERIFY_C(
                    PendingUpdate.Defined(),
                    TWellKnownEntityTypes::TLS_CERTIFICATE_PROVIDER,
                    LogComponent,
                    "missing pending update while update is in progress");
                future = PendingUpdate->Promise.GetFuture();
            } else {
                PendingUpdate = CreatePendingUpdate();
                future = PendingUpdate->Promise.GetFuture();
                UpdateRequested = true;
            }
        }
        Wakeup.BroadCast();
        return future;
    }

private:
    void RunRefreshThread()
    {
        while (!Stopping.load()) {
            std::unique_lock lock(WakeupMutex);
            const bool isRequested = Wakeup.WaitT(
                WakeupMutex,
                RefreshIntervalSec,
                [this] {
                    return Stopping.load() || UpdateRequested;
                });
            if (Stopping.load()) {
                return;
            }
            if (isRequested) {
                STORAGE_VERIFY_C(
                    PendingUpdate.Defined(),
                    TWellKnownEntityTypes::TLS_CERTIFICATE_PROVIDER,
                    LogComponent,
                    "missing pending update for requested refresh");
            } else {
                PendingUpdate = CreatePendingUpdate();
            }
            UpdateRequested = false;
            UpdateInProgress = true;
            lock.unlock();

            ForceUpdate();
            lock.lock();
            UpdateInProgress = false;
            CompletePendingUpdate();
            lock.unlock();
        }
    }

    TPendingUpdate CreatePendingUpdate()
    {
        return {.Promise = NThreading::NewPromise<void>()};
    }

    void CompletePendingUpdate()
    {
        STORAGE_VERIFY_C(
            PendingUpdate.Defined(),
            TWellKnownEntityTypes::TLS_CERTIFICATE_PROVIDER,
            LogComponent,
            "complete called without pending update");
        PendingUpdate->Promise.SetValue();
        PendingUpdate = Nothing();
    }

    void ForceUpdate()
    {
        TVector<TMaybe<PemKeyCertPairList>> identities(
            Certificates.size());
        TVector<TMaybe<ui64>> certNotAfterTs(Certificates.size());

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

        TMaybe<TString> newRoot;
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

            if (identities[i].Defined()) {
                const auto& pair = identities[i]->front();
                if (needsRoot && hasNewRoot) {
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

        bool rootChanged = false;
        if (!needsRoot) {
            rootChanged = RootCertificate.Defined();
            RootCertificate = Nothing();
        } else if (hasNewRoot) {
            rootChanged = newRoot != RootCertificate;
            RootCertificate = newRoot;
        }
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

            Certificates[i].IdentityKeyCertPairs = identities[i];
            hasUpdates = true;
        }

        if (hasUpdates) {
            static_cast<TDerived*>(this)->OnCertificateUpdated();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcPeriodicCertificateProvider final
    : public grpc_tls_certificate_provider
    , public TPeriodicCertificateProviderBase<TGrpcPeriodicCertificateProvider>
{
    using TBase =
        TPeriodicCertificateProviderBase<TGrpcPeriodicCertificateProvider>;
private:
    RefCountedPtr<grpc_tls_certificate_distributor> Distributor;

public:
    friend class
        TPeriodicCertificateProviderBase<TGrpcPeriodicCertificateProvider>;

    TGrpcPeriodicCertificateProvider(
            ILoggingServicePtr logging,
            TString logComponent,
            NMonitoring::TDynamicCountersPtr serverGroup,
            TString rootCertPath,
            TVector<TCertificateFiles> certificates,
            TDuration refreshIntervalSec)
        : TPeriodicCertificateProviderBase<
            TGrpcPeriodicCertificateProvider>(
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

    ~TGrpcPeriodicCertificateProvider() override
    {
        TBase::Stop();
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
        return TBase::UpdateNow();
    }

    void Start()
    {
        TBase::Start();
    }

    void Stop()
    {
        TBase::Stop();
    }

private:
    void PublishCerts(const TString& certName)
    {
        auto states = GetCertificateStates();
        const auto rootCertificate = GetRootCertificate();

        const bool needsRoot = !!GetRootCertPath();
        const bool rootInvalid = needsRoot && !rootCertificate.Defined();

        bool identityInvalid = false;
        PemKeyCertPairList identityPairs;

        for (const auto& state: states) {
            if (!state.IdentityKeyCertPairs.Defined()) {
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
            y_absl::optional<TString> grpcRootCertificate = y_absl::nullopt;
            if (rootCertificate.Defined()) {
                grpcRootCertificate = *rootCertificate;
            }

            Distributor->SetKeyMaterials(
                certName,
                std::move(grpcRootCertificate),
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

class TGrpcCertificateProvider final
    : public grpc::experimental::CertificateProviderInterface
{
private:
    grpc_core::RefCountedPtr<TGrpcPeriodicCertificateProvider> Provider;

public:
    explicit TGrpcCertificateProvider(
        grpc_core::RefCountedPtr<TGrpcPeriodicCertificateProvider> provider)
        : Provider(std::move(provider))
    {}

    grpc_tls_certificate_provider* c_provider() override
    {
        return Provider.get();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPeriodicCertificateProvider final
    : public ICertificateProvider
{
private:
    grpc_core::RefCountedPtr<TGrpcPeriodicCertificateProvider> Provider;
    std::shared_ptr<TGrpcCertificateProvider> GrpcProvider;
    const bool HasRootCert;

public:
    TPeriodicCertificateProvider(
        ILoggingServicePtr logging,
        TString logComponent,
        NMonitoring::TDynamicCountersPtr serverGroup,
        TString rootCertPath,
        TVector<TCertificateFiles> certificates,
        TDuration refreshIntervalSec)
        : HasRootCert(!!rootCertPath)
    {
        Provider = grpc_core::RefCountedPtr<
            TGrpcPeriodicCertificateProvider>(
                new TGrpcPeriodicCertificateProvider(
                    std::move(logging),
                    std::move(logComponent),
                    std::move(serverGroup),
                    std::move(rootCertPath),
                    std::move(certificates),
                    refreshIntervalSec));

        GrpcProvider =
            std::make_shared<TGrpcCertificateProvider>(Provider);
    }

    NThreading::TFuture<void> UpdateCertificates() override
    {
        return Provider->UpdateNow();
    }

    std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() override
    {
        grpc::experimental::TlsChannelCredentialsOptions tlsOptions;
        tlsOptions.set_certificate_provider(GrpcProvider);
        tlsOptions.watch_identity_key_cert_pairs();
        if (HasRootCert) {
            tlsOptions.watch_root_certs();
        }
        return grpc::experimental::TlsCredentials(tlsOptions);
    }

    std::shared_ptr<grpc::ServerCredentials>
        CreateSecureServerCredentials() override
    {
        grpc::experimental::TlsServerCredentialsOptions tlsOptions(
            GrpcProvider);
        tlsOptions.set_cert_request_type(
            GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY);
        tlsOptions.watch_identity_key_cert_pairs();
        if (HasRootCert) {
            tlsOptions.watch_root_certs();
        }
        return grpc::experimental::TlsServerCredentials(tlsOptions);
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

class TStaticCertificateProvider final
    : public ICertificateProvider
{
private:
    const TString RootCertPath;
    const TVector<TCertificateFiles> Certificates;

public:
    TStaticCertificateProvider(
            TString rootCertPath,
            TVector<TCertificateFiles> certificates)
        : RootCertPath(std::move(rootCertPath))
        , Certificates(std::move(certificates))
    {}

    NThreading::TFuture<void> UpdateCertificates() override
    {
        auto promise = NThreading::NewPromise<void>();
        promise.SetValue();
        return promise.GetFuture();
    }

    std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() override
    {
        grpc::SslCredentialsOptions sslOptions;
        if (RootCertPath) {
            sslOptions.pem_root_certs = ReadFile(RootCertPath);
        }

        if (!Certificates.empty()) {
            const auto& cert = Certificates.front();
            sslOptions.pem_cert_chain = ReadFile(cert.CertChainPath);
            sslOptions.pem_private_key = ReadFile(cert.PrivateKeyPath);
        }

        return grpc::SslCredentials(sslOptions);
    }

    std::shared_ptr<grpc::ServerCredentials>
        CreateSecureServerCredentials() override
    {
        grpc::SslServerCredentialsOptions sslOptions;
        sslOptions.client_certificate_request =
            GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;

        if (RootCertPath) {
            sslOptions.pem_root_certs = ReadFile(RootCertPath);
        }

        for (const auto& cert: Certificates) {
            grpc::SslServerCredentialsOptions::PemKeyCertPair keyCert;

            keyCert.cert_chain = ReadFile(cert.CertChainPath);

            keyCert.private_key = ReadFile(cert.PrivateKeyPath);

            sslOptions.pem_key_cert_pairs.push_back(std::move(keyCert));
        }

        return grpc::SslServerCredentials(sslOptions);
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICertificateProviderPtr CreateCertificateProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    NMonitoring::TDynamicCountersPtr serverGroup,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshIntervalSec)
{
    if (refreshIntervalSec == TDuration::Zero()) {
        return CreateStaticCertificateProvider(
            std::move(rootCertPath),
            std::move(certificates));
    }

    ValidateCertificates(certificates, true);

    return std::make_shared<TPeriodicCertificateProvider>(
        std::move(logging),
        std::move(logComponent),
        std::move(serverGroup),
        std::move(rootCertPath),
        std::move(certificates),
        refreshIntervalSec);
}

ICertificateProviderPtr CreateStaticCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates)
{
    ValidateCertificates(certificates, false);

    return std::make_shared<TStaticCertificateProvider>(
        std::move(rootCertPath),
        std::move(certificates));
}

}   // namespace NCloud
