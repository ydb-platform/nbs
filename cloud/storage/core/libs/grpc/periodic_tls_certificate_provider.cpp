#include "tls_certificate_provider.h"
#include "stable_read.h"
#include "tls_utils.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/verify.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <src/core/lib/gprpp/ref_counted_ptr.h>
#include <src/core/lib/security/credentials/tls/grpc_tls_certificate_distributor.h>
#include <src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/digest/city.h>
#include <util/folder/dirut.h>
#include <util/generic/yexception.h>
#include <util/system/mutex.h>
#include <util/system/yassert.h>

#include <memory>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

using grpc_core::PemKeyCertPairList;
using grpc_core::RefCountedPtr;

////////////////////////////////////////////////////////////////////////////////

ui64 RootCaFingerprint(TStringBuf rootCa)
{
    // Dynamic counters export gauges as double. Keep only 53 bits to avoid
    // precision loss while preserving a high-quality fingerprint.
    return CityHash64(rootCa) & ((1ULL << 53) - 1);
}

////////////////////////////////////////////////////////////////////////////////

class TGrpcTlsCertificateProvider final
    : public grpc_tls_certificate_provider
{
private:
    RefCountedPtr<grpc_tls_certificate_distributor> Distributor;

public:
    TGrpcTlsCertificateProvider()
        : Distributor(
            grpc_core::MakeRefCounted<grpc_tls_certificate_distributor>())
    {}

    void PublishCerts(
        const TMaybe<TString>& rootCertificate,
        PemKeyCertPairList identityPairs)
    {
        Distributor->SetKeyMaterials(
            "",
            rootCertificate.Defined()
                ? *rootCertificate
                : std::optional<TString>{},
            std::move(identityPairs));
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

    int CompareImpl(const grpc_tls_certificate_provider* other) const override
    {
        // This is a GRPC way to compare grpc_tls_certificate_provider instances.
        // grpc_tls_certificate_provider instances are concidered distinct if
        // their addresses are disctinct. Internaly GRPC uses three-way compare
        // intead of < to sort and search grpc_tls_certificate_provider instances
        auto res = std::compare_three_way{}(
            static_cast<const grpc_tls_certificate_provider*>(this),
            other);
        if (res < 0) {
            return -1;
        }
        return res > 0 ? 1 : 0;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TGrpcCertificateProvider final
    : public grpc::experimental::CertificateProviderInterface
{
private:
    grpc_core::RefCountedPtr<TGrpcTlsCertificateProvider> Provider;

public:
    explicit TGrpcCertificateProvider(
            grpc_core::RefCountedPtr<TGrpcTlsCertificateProvider> provider)
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
    , public std::enable_shared_from_this<TPeriodicCertificateProvider>
{
    const ILoggingServicePtr Logging;
    const TString LogComponent;
    const NMonitoring::TDynamicCountersPtr ServerGroup;
    const TDuration RefreshInterval;
    const ISchedulerPtr Scheduler;
    const ITaskQueuePtr TaskQueue;

    grpc_core::RefCountedPtr<TGrpcTlsCertificateProvider> TlsProvider;
    std::shared_ptr<grpc::experimental::CertificateProviderInterface>
        GrpcProvider;

    NTlsUtils::TRootCaPair RootCaPair;
    TVector<NTlsUtils::TCertificatePair> Certificates;
    TStableRead<TString> RootCaStableRead;
    TVector<TStableRead<NTlsUtils::TIdentityContent>> IdentityStableReads;
    TVector<NMonitoring::TDynamicCountersPtr> CertificateMetrics;
    NMonitoring::TDynamicCountersPtr RootCaMetrics;

    mutable TMutex UpdateMutex;
    std::atomic<bool> Started = false;
    // Updates do not run concurrently and do not wait for each other.
    bool UpdateInProgress = false;
    // The pending on-demand request, or, while an update is in progress, the
    // result of that update.
    NThreading::TPromise<NProto::TError> Update;

    TLog Log;

public:
    TPeriodicCertificateProvider(
            ILoggingServicePtr logging,
            TString logComponent,
            ISchedulerPtr scheduler,
            ITaskQueuePtr taskQueue,
            NMonitoring::TDynamicCountersPtr serverGroup,
            TString rootCertPath,
            TVector<TCertificateFiles> certificates,
            TDuration refreshInterval)
        : Logging(std::move(logging))
        , LogComponent(std::move(logComponent))
        , ServerGroup(std::move(serverGroup))
        , RefreshInterval(refreshInterval)
        , Scheduler(std::move(scheduler))
        , TaskQueue(std::move(taskQueue))
        , TlsProvider(grpc_core::MakeRefCounted<TGrpcTlsCertificateProvider>())
        , GrpcProvider(std::make_shared<TGrpcCertificateProvider>(TlsProvider))
        , RootCaPair(NTlsUtils::LoadRootCaPair(std::move(rootCertPath)))
        , Certificates(NTlsUtils::LoadCertificatePairs(std::move(certificates)))
        , IdentityStableReads(Certificates.size())
        , CertificateMetrics(Certificates.size())
    {
    }

    ~TPeriodicCertificateProvider() override
    {
        Y_ABORT_UNLESS(Started.load() == false);
    }

    NThreading::TFuture<NProto::TError> UpdateCertificates() override
    {
        NThreading::TFuture<NProto::TError> future;
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (!Started) {
                return NThreading::MakeFuture(MakeError(
                    E_INVALID_STATE,
                    "Certificate provider is not started"));
            }
            if (Update.Initialized()) {
                return NThreading::MakeFuture(MakeError(
                    E_TRY_AGAIN,
                    "Another certificate update is pending or in progress"));
            }
            Update = NThreading::NewPromise<NProto::TError>();
            future = Update.GetFuture();
        }

        ScheduleUpdateAt(TInstant::Now(), /*periodic=*/false);
        return future;
    }

    std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() override
    {
        grpc::experimental::TlsChannelCredentialsOptions tlsOptions;
        tlsOptions.set_certificate_provider(GrpcProvider);
        if (!Certificates.empty()) {
            tlsOptions.watch_identity_key_cert_pairs();
        }
        if (RootCaPair.RootCaPath) {
            tlsOptions.watch_root_certs();
        }
        return grpc::experimental::TlsCredentials(tlsOptions);
    }

    std::shared_ptr<grpc::ServerCredentials>
        CreateSecureServerCredentials() override
    {
        grpc::experimental::TlsServerCredentialsOptions tlsOptions(GrpcProvider);
        tlsOptions.set_cert_request_type(
            GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY);
        if (!Certificates.empty()) {
            tlsOptions.watch_identity_key_cert_pairs();
        }
        if (RootCaPair.RootCaPath) {
            tlsOptions.watch_root_certs();
        }
        return grpc::experimental::TlsServerCredentials(tlsOptions);
    }

    void Start() override
    {
        if (Started.load()) {
            return;
        }

        Log = Logging->CreateLog(LogComponent);

        NMonitoring::TDynamicCountersPtr tlsMetricsGroup;
        if (ServerGroup) {
            tlsMetricsGroup =
                ServerGroup->GetSubgroup("subsystem", "certificates");
        }

        for (size_t i = 0; i < Certificates.size(); ++i) {
            const auto& certificate = Certificates[i];
            NMonitoring::TDynamicCountersPtr certMetrics;
            if (tlsMetricsGroup) {
                certMetrics = tlsMetricsGroup->GetSubgroup(
                    "cert",
                    GetBaseName(certificate.Files.CertChainPath));
            }
            CertificateMetrics[i] = std::move(certMetrics);
        }

        if (tlsMetricsGroup && RootCaPair.RootCaPath) {
            RootCaMetrics = tlsMetricsGroup
                ->GetSubgroup("cert", GetBaseName(RootCaPair.RootCaPath));
        }

        PublishInitialState();

        // Updates are rejected until the initialization is done.
        {
            TGuard<TMutex> lock(UpdateMutex);
            Started.store(true);
        }

        ScheduleUpdateAt(TInstant::Now() + RefreshInterval, true);
    }

    void Stop() override
    {
        NThreading::TFuture<NProto::TError> update;
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (!Started.load()) {
                return;
            }
            Started.store(false);

            if (UpdateInProgress) {
                update = Update.GetFuture();
            } else if (Update.Initialized()) {
                std::exchange(Update, {}).SetValue(MakeError(
                    E_INVALID_STATE,
                    "Certificate provider is stopped"));
            }
        }

        if (update.Initialized()) {
            update.Wait();
        }
    }

private:
    void ScheduleUpdateAt(TInstant deadline, bool periodic)
    {
        Scheduler->Schedule(deadline, [weak = weak_from_this(), periodic] {
            auto self = weak.lock();
            if (!self) {
                return;
            }
            self->TaskQueue->ExecuteSimple([weak = weak, periodic] {
                auto self = weak.lock();
                if (!self) {
                    return;
                }
                self->RunUpdate(periodic);
            });
        });
    }

    // Any update serves the pending on-demand request, if there is one: it
    // applies new content right away. An on-demand update that finds no
    // request has nothing to do, a periodic one that finds another update in
    // progress is skipped until the next interval.
    void RunUpdate(bool periodic)
    {
        bool run = false;
        bool onDemand = false;
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (Started && !UpdateInProgress) {
                if (Update.Initialized()) {
                    run = onDemand = true;
                } else if (periodic) {
                    Update = NThreading::NewPromise<NProto::TError>();
                    run = true;
                }
                UpdateInProgress = run;
            }
        }

        if (run) {
            auto result = RefreshCertificates(/*periodic=*/!onDemand);

            // Completed under UpdateMutex, so that Stop() does not return
            // before that.
            TGuard<TMutex> lock(UpdateMutex);
            UpdateInProgress = false;
            std::exchange(Update, {}).SetValue(std::move(result));
        }

        if (periodic && Started) {
            ScheduleUpdateAt(TInstant::Now() + RefreshInterval, true);
        }
    }



    void PublishRootCaFingerprint()
    {
        if (RootCaMetrics) {
            const ui64 fingerprint = RootCaFingerprint(RootCaPair.RootCa);
            *RootCaMetrics->GetCounter("Fingerprint", false) = fingerprint;
        }
    }

    void PublishExpireTs(size_t index, TInstant notValidAfter)
    {
        if (CertificateMetrics[index] && notValidAfter) {
            *CertificateMetrics[index]->GetCounter("ExpireTs", false) =
                notValidAfter.Seconds();
        }
    }

    void PublishCerts()
    {
        PemKeyCertPairList identityPairs;
        for (const auto& certificate: Certificates) {
            const auto& content = certificate.Content;
            if (content.PrivateKey.empty() || content.CertChain.empty()) {
                continue;
            }
            identityPairs.emplace_back(content.PrivateKey, content.CertChain);
        }

        TMaybe<TString> rootCert = RootCaPair.RootCa.empty()
            ? Nothing()
            : TMaybe<TString>(RootCaPair.RootCa);
        if (rootCert.Defined() || !identityPairs.empty()) {
            TlsProvider->PublishCerts(rootCert, std::move(identityPairs));
        }
    }

    // The initial load accepts certificates that fail validation so that the
    // service is able to start; they are only reported, and since unchanged
    // files are never re-validated, this is the only place where they are.
    void PublishInitialState()
    {
        PublishRootCaFingerprint();
        for (size_t i = 0; i < Certificates.size(); ++i) {
            const auto& cert = Certificates[i];
            auto validity = NTlsUtils::ValidateIdentity(cert.Content);
            if (HasError(validity.GetError())) {
                STORAGE_WARN(
                    "Identity certificate " << cert.Files.CertChainPath.Quote()
                    << " is loaded but not valid: "
                    << FormatError(validity.GetError()));
            }
            ApplyIdentity(i, cert.Content);
        }
        PublishCerts();
    }

    // Periodic checks apply new content after two of them read it unchanged
    // (see TStableRead), i.e. within two refresh intervals. On-demand updates
    // apply it right away. Content that fails to load or validate is logged
    // and the previous one is kept.
    NProto::TError RefreshCertificates(bool periodic)
    {
        NProto::TError error;
        bool changed = false;

        if (RefreshRootCa(periodic, error)) {
            PublishRootCaFingerprint();
            changed = true;
        }

        for (size_t i = 0; i < Certificates.size(); ++i) {
            if (RefreshIdentity(i, periodic, error)) {
                changed = true;
            }
        }

        // The distributor notifies gRPC on every publish, which rebuilds the
        // SSL context, so publish only when something has changed.
        if (changed) {
            PublishCerts();
        }

        return error;
    }

    static void KeepFirstError(
        NProto::TError& error,
        const TString& path,
        const NProto::TError& e)
    {
        if (!HasError(error)) {
            error = MakeError(
                e.GetCode(),
                TStringBuilder() << "Failed to update " << path.Quote()
                                 << ": " << e.GetMessage());
        }
    }

    template <typename T>
    static EStableReadDecision Decide(
        TStableRead<T>& stableRead,
        const T& current,
        const T& content,
        bool periodic)
    {
        if (periodic) {
            return stableRead.Observe(current, content);
        }

        // Reads made in between periodic checks break the sequence of
        // periodic reads that the stable read relies on.
        stableRead.Reset();
        return content == current
            ? EStableReadDecision::Unchanged
            : EStableReadDecision::Apply;
    }

    // Returns true if the root certificate has been replaced.
    bool RefreshRootCa(bool periodic, NProto::TError& error)
    {
        const auto& path = RootCaPair.RootCaPath;
        if (path.empty()) {
            return false;
        }

        auto content = NTlsUtils::TryReadFile(path);
        if (HasError(content.GetError())) {
            RootCaStableRead.Reset();
            STORAGE_WARN(
                "Root certificate update is skipped: "
                << FormatError(content.GetError()));
            KeepFirstError(error, path, content.GetError());
            return false;
        }

        switch (Decide(
            RootCaStableRead,
            RootCaPair.RootCa,
            content.GetResult(),
            periodic))
        {
            case EStableReadDecision::Unchanged:
                return false;
            case EStableReadDecision::Wait:
                STORAGE_INFO(
                    "New root certificate " << path.Quote()
                    << ", waiting for a stable read");
                return false;
            case EStableReadDecision::Apply:
                break;
        }

        auto validity = NTlsUtils::IsValidPemCertificate(content.GetResult());
        if (HasError(validity.GetError())) {
            STORAGE_WARN(
                "Root certificate update is skipped: "
                << FormatError(validity.GetError()));
            KeepFirstError(error, path, validity.GetError());
            return false;
        }

        RootCaPair.RootCa = content.ExtractResult();
        STORAGE_INFO(
            "Root certificate " << path.Quote() << " has been updated");
        return true;
    }

    // Returns true if the certificate has been replaced.
    bool RefreshIdentity(size_t index, bool periodic, NProto::TError& error)
    {
        auto& cert = Certificates[index];
        auto& stableRead = IdentityStableReads[index];
        const auto& path = cert.Files.CertChainPath;

        auto content = NTlsUtils::ReadIdentity(cert.Files);
        if (HasError(content.GetError())) {
            stableRead.Reset();
            STORAGE_WARN(
                "Identity certificate update is skipped for " << path.Quote()
                << ": " << FormatError(content.GetError()));
            KeepFirstError(error, path, content.GetError());
            return false;
        }

        switch (Decide(stableRead, cert.Content, content.GetResult(), periodic))
        {
            case EStableReadDecision::Unchanged:
                return false;
            case EStableReadDecision::Wait:
                STORAGE_INFO(
                    "New identity certificate " << path.Quote()
                    << ", waiting for a stable read");
                return false;
            case EStableReadDecision::Apply:
                break;
        }

        auto validity = NTlsUtils::ValidateIdentity(content.GetResult());
        if (HasError(validity.GetError())) {
            STORAGE_WARN(
                "Identity certificate update is skipped for " << path.Quote()
                << ": " << FormatError(validity.GetError()));
            KeepFirstError(error, path, validity.GetError());
            return false;
        }

        ApplyIdentity(index, content.ExtractResult());
        return true;
    }

    void ApplyIdentity(size_t index, NTlsUtils::TIdentityContent content)
    {
        auto& cert = Certificates[index];
        const auto& path = cert.Files.CertChainPath;

        TInstant notValidAfter;
        auto notAfterTs =
            NTlsUtils::GetCertificateNotAfterTimestampSec(content.CertChain);
        if (HasError(notAfterTs)) {
            STORAGE_WARN(
                "Unable to parse certificate notAfter date for "
                << path.Quote() << ": " << FormatError(notAfterTs.GetError()));
        } else {
            notValidAfter = TInstant::Seconds(notAfterTs.ExtractResult());
        }

        cert.Content = std::move(content);
        PublishExpireTs(index, notValidAfter);
        STORAGE_INFO(
            "Identity certificate " << path.Quote() << " is loaded"
            << ", expires at " << notValidAfter);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICertificateProviderPtr CreatePeriodicCertificateProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue,
    NMonitoring::TDynamicCountersPtr serverGroup,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshInterval)
{
    Y_ABORT_UNLESS(refreshInterval, "refreshInterval should not be zero");

    return std::make_shared<TPeriodicCertificateProvider>(
        std::move(logging),
        std::move(logComponent),
        std::move(scheduler),
        std::move(taskQueue),
        std::move(serverGroup),
        std::move(rootCertPath),
        std::move(certificates),
        refreshInterval);
}

}   // namespace NCloud
