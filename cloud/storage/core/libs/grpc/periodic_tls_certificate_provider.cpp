#include "tls_certificate_provider.h"
#include "stable_read.h"
#include "tls_utils.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/task_queue.h>
#include <cloud/storage/core/libs/common/timer.h>
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
    const ITimerPtr Timer;

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
    bool UpdateInProgress = false;
    NThreading::TPromise<void> PendingUpdate;

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
            TDuration refreshInterval,
            ITimerPtr timer)
        : Logging(std::move(logging))
        , LogComponent(std::move(logComponent))
        , ServerGroup(std::move(serverGroup))
        , RefreshInterval(refreshInterval)
        , Scheduler(std::move(scheduler))
        , TaskQueue(std::move(taskQueue))
        , Timer(std::move(timer))
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

    // Completes when the files have been re-read and any new content has
    // either been applied or rejected, which may take up to half of the
    // refresh interval, or when the provider is stopped.
    NThreading::TFuture<void> UpdateCertificates() override
    {
        NThreading::TFuture<void> future;
        bool scheduleUpdate = false;
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (!PendingUpdate.Initialized()) {
                PendingUpdate = NThreading::NewPromise<void>();
                if (!UpdateInProgress) {
                    scheduleUpdate = true;
                }
            }
            future = PendingUpdate.GetFuture();
        }
        if (scheduleUpdate) {
            ScheduleUpdateAt(Timer->Now(), /*periodic=*/false);
        }
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
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (Started.load()) {
                return;
            }
            Started.store(true);
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

        ScheduleUpdateAt(Timer->Now() + RefreshInterval, true);
    }

    void Stop() override
    {
        NThreading::TPromise<void> promise;
        NThreading::TFuture<void> waitUpdate;
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (!Started.load()) {
                return;
            }
            Started.store(false);
            if (UpdateInProgress) {
                waitUpdate = PendingUpdate.GetFuture();
            } else {
                promise = std::exchange(PendingUpdate, {});
            }
        }

        if (promise.Initialized()) {
            promise.SetValue();
        }
        if (waitUpdate.Initialized()) {
            waitUpdate.Wait();
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
                self->RunPeriodicUpdate(periodic);
            });
        });
    }

    void RunPeriodicUpdate(bool periodic)
    {
        bool run = false;
        {
            TGuard<TMutex> lock(UpdateMutex);
            if (Started && !UpdateInProgress) {
                UpdateInProgress = true;
                if (!PendingUpdate.Initialized()) {
                    PendingUpdate = NThreading::NewPromise<void>();
                }
                run = true;
            }
        }

        bool pending = false;
        if (run) {
            pending = RefreshCertificates();

            // The requested update is complete when no content is waiting
            // for a stable read anymore, or when the provider is stopped.
            // Until then repeated UpdateCertificates() calls share the same
            // future instead of triggering extra reads.
            NThreading::TPromise<void> promise;
            {
                TGuard<TMutex> lock(UpdateMutex);
                UpdateInProgress = false;
                if (!pending || !Started.load()) {
                    promise = std::exchange(PendingUpdate, {});
                }
            }
            if (promise.Initialized()) {
                promise.SetValue();
            }
        }

        bool alive = false;
        {
            TGuard<TMutex> lock(UpdateMutex);
            alive = Started;
        }
        if (!alive) {
            return;
        }

        // Files are checked once per interval. New content is re-checked
        // after half an interval, so that a change takes effect within one and
        // a half intervals without reading unchanged files more often.
        if (periodic) {
            ScheduleUpdateAt(
                Timer->Now() +
                    (pending ? RefreshInterval / 2 : RefreshInterval),
                true);
        } else if (pending) {
            ScheduleUpdateAt(Timer->Now() + RefreshInterval / 2, false);
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
            if (certificate.PrivateKey.empty() ||
                certificate.CertChain.empty())
            {
                continue;
            }
            identityPairs.emplace_back(
                certificate.PrivateKey,
                certificate.CertChain);
        }

        TMaybe<TString> rootCert = RootCaPair.RootCa.empty()
            ? Nothing()
            : TMaybe<TString>(RootCaPair.RootCa);
        if (rootCert.Defined() || !identityPairs.empty()) {
            TlsProvider->PublishCerts(rootCert, std::move(identityPairs));
        }
    }

    void PublishInitialState()
    {
        PublishRootCaFingerprint();
        for (size_t i = 0; i < Certificates.size(); ++i) {
            auto notAfterTs = NTlsUtils::GetCertificateNotAfterTimestampSec(
                Certificates[i].CertChain);
            if (HasError(notAfterTs)) {
                STORAGE_WARN(
                    "Unable to parse certificate notAfter date for "
                    << Certificates[i].Files.CertChainPath.Quote() << ": "
                    << FormatError(notAfterTs.GetError()));
                continue;
            }
            PublishExpireTs(i, TInstant::Seconds(notAfterTs.ExtractResult()));
        }
        PublishCerts();
    }

    // Re-reads the certificate files. New content is applied only after it
    // has stayed unchanged for half of the refresh interval, see TStableRead.
    // The last successfully loaded content is kept if the files cannot be
    // read, parsed or validated; new content that fails these checks is
    // reported on every check until the files change. Every certificate is
    // refreshed independently. Returns true if some content is waiting for a
    // stable read.
    bool RefreshCertificates()
    {
        const TInstant now = Timer->Now();
        const TDuration holdTime = RefreshInterval / 2;

        bool pending = false;
        bool changed = false;

        if (RefreshRootCa(now, holdTime, pending)) {
            PublishRootCaFingerprint();
            changed = true;
        }

        for (size_t i = 0; i < Certificates.size(); ++i) {
            if (RefreshIdentity(i, now, holdTime, pending)) {
                changed = true;
            }
        }

        // The distributor notifies gRPC on every publish, which rebuilds the
        // SSL context, so publish only when something has changed.
        if (changed) {
            PublishCerts();
        }

        return pending;
    }

    // Returns true if the root certificate has been replaced.
    bool RefreshRootCa(TInstant now, TDuration holdTime, bool& pending)
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
            return false;
        }

        switch (RootCaStableRead.Observe(
            RootCaPair.RootCa,
            content.GetResult(),
            now,
            holdTime))
        {
            case EStableReadDecision::Unchanged:
                return false;
            case EStableReadDecision::Wait:
                pending = true;
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
            return false;
        }

        RootCaPair.RootCa = content.ExtractResult();
        STORAGE_INFO(
            "Root certificate " << path.Quote() << " has been updated");
        return true;
    }

    // Returns true if the certificate has been replaced.
    bool RefreshIdentity(
        size_t index,
        TInstant now,
        TDuration holdTime,
        bool& pending)
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
            return false;
        }

        const NTlsUtils::TIdentityContent current{
            .PrivateKey = cert.PrivateKey,
            .CertChain = cert.CertChain,
        };
        switch (stableRead.Observe(current, content.GetResult(), now, holdTime))
        {
            case EStableReadDecision::Unchanged:
                return false;
            case EStableReadDecision::Wait:
                pending = true;
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
            return false;
        }

        TInstant notValidAfter;
        auto notAfterTs = NTlsUtils::GetCertificateNotAfterTimestampSec(
            content.GetResult().CertChain);
        if (HasError(notAfterTs)) {
            STORAGE_WARN(
                "Unable to parse certificate notAfter date for "
                << path.Quote() << ": " << FormatError(notAfterTs.GetError()));
        } else {
            notValidAfter = TInstant::Seconds(notAfterTs.ExtractResult());
        }

        auto identity = content.ExtractResult();
        cert.PrivateKey = std::move(identity.PrivateKey);
        cert.CertChain = std::move(identity.CertChain);
        PublishExpireTs(index, notValidAfter);
        STORAGE_INFO(
            "Identity certificate " << path.Quote() << " has been updated"
            << ", expires at " << notValidAfter);
        return true;
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
    TDuration refreshInterval,
    ITimerPtr timer)
{
    Y_ABORT_UNLESS(refreshInterval, "refreshInterval should not be zero");
    Y_ABORT_UNLESS(timer, "timer should not be null");

    return std::make_shared<TPeriodicCertificateProvider>(
        std::move(logging),
        std::move(logComponent),
        std::move(scheduler),
        std::move(taskQueue),
        std::move(serverGroup),
        std::move(rootCertPath),
        std::move(certificates),
        refreshInterval,
        std::move(timer));
}

}   // namespace NCloud
