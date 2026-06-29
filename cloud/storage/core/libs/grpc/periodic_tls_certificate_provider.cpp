#include "tls_certificate_provider.h"
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

    ~TGrpcTlsCertificateProvider()
    {}

    void PublishCerts(
        const TMaybe<TString>& rootCertificate,
        PemKeyCertPairList identityPairs)
    {
        if (identityPairs.empty()) {
            return;
        }
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
    struct TCertificateState
    {
        NTlsUtils::TCertificatePair Pair;
        NMonitoring::TDynamicCountersPtr Metrics;
    };

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
    TVector<TCertificateState> Certificates;

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
    {
        for (auto& pair: NTlsUtils::LoadCertificatePairs(std::move(certificates))) {
            Certificates.push_back({.Pair = std::move(pair)});
        }
    }

    ~TPeriodicCertificateProvider() override
    {
        STORAGE_VERIFY(
            Started.store(false),
            TWellKnownEntityTypes::DISK,
            GetDiskId(Request->GetDiskId()));
    }

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
            ScheduleUpdateAt(TInstant::Now(), /*periodic=*/false);
        }
        return future;
    }

    std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() override
    {
        grpc::experimental::TlsChannelCredentialsOptions tlsOptions;
        tlsOptions.set_certificate_provider(GrpcProvider);
        tlsOptions.watch_identity_key_cert_pairs();
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
        tlsOptions.watch_identity_key_cert_pairs();
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

        for (auto& certificate: Certificates) {
            NMonitoring::TDynamicCountersPtr certMetrics;
            if (tlsMetricsGroup) {
                certMetrics = tlsMetricsGroup->GetSubgroup(
                    "cert",
                    GetBaseName(certificate.Pair.Files.CertChainPath));
            }
            certificate.Metrics = std::move(certMetrics);
        }

        RefreshCertificates(true);

        ScheduleUpdateAt(TInstant::Now() + RefreshInterval, true);
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

        if (run) {
            RefreshCertificates(false);

            NThreading::TPromise<void> promise;
            {
                TGuard<TMutex> lock(UpdateMutex);
                promise = std::exchange(PendingUpdate, {});
                UpdateInProgress = false;
            }
            if (promise.Initialized()) {
                promise.SetValue();
            }
        }

        if (periodic) {
            bool alive = false;
            {
                TGuard<TMutex> lock(UpdateMutex);
                alive = Started;
            }
            if (alive) {
                ScheduleUpdateAt(
                    TInstant::Now() + RefreshInterval,
                    true);
            }
        }
    }

    void RefreshCertificates(bool initial)
    {
        TVector<NTlsUtils::TCertificatePair> certPairs;
        certPairs.reserve(Certificates.size());
        for (const auto& state: Certificates) {
            certPairs.push_back(state.Pair);
        }

        const TString oldRootCa = RootCaPair.RootCa;
        auto result = NTlsUtils::UpdateCertificates(certPairs, RootCaPair, Log);
        RootCaPair.RootCa = result.RootCa.GetOrElse(oldRootCa);

        const bool rootChanged = oldRootCa != RootCaPair.RootCa;
        bool hasUpdates = rootChanged;

        PemKeyCertPairList identityPairs;
        for (size_t i = 0; i < Certificates.size(); ++i) {
            auto& state = Certificates[i];
            const auto& newCert = result.Certificates[i];

            if (!newCert.Defined()) {
                continue;
            }

            if (state.Metrics && newCert->NotValidAfter) {
                *state.Metrics->GetCounter("ExpireTs", false) =
                    newCert->NotValidAfter.Seconds();
            }

            const auto& chain = newCert->CertificatesChain;
            const bool identityChanged =
                chain.front().private_key() != state.Pair.PrivateKey ||
                chain.front().cert_chain() != state.Pair.CertChain;

            if (identityChanged || rootChanged) {
                state.Pair.PrivateKey = TString(chain.front().private_key());
                state.Pair.CertChain = TString(chain.front().cert_chain());
                hasUpdates = true;
            }

            identityPairs.insert(identityPairs.end(), chain.begin(), chain.end());
        }

        if (hasUpdates || initial) {
            TMaybe<TString> rootCert = RootCaPair.RootCa.empty()
                ? Nothing()
                : TMaybe<TString>(RootCaPair.RootCa);
            TlsProvider->PublishCerts(rootCert, std::move(identityPairs));
        }
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
    Y_ENSURE(refreshInterval, "refreshInterval should not be zero");

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
