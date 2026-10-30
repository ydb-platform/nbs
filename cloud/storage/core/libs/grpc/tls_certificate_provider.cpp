#include "tls_certificate_provider.h"

#include "tls_certificate_watcher.h"
#include "tls_utils.h"

#include <src/core/lib/gprpp/ref_counted_ptr.h>
#include <src/core/lib/security/credentials/tls/grpc_tls_certificate_distributor.h>
#include <src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h>

#include <util/generic/maybe.h>
#include <util/system/mutex.h>

#include <memory>
#include <optional>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

using grpc_core::PemKeyCertPairList;
using grpc_core::RefCountedPtr;

////////////////////////////////////////////////////////////////////////////////

class TStaticCertificateProvider final
    : public ICertificateProvider
{
private:
    const NTlsUtils::TRootCaPair RootCaPair;
    const TVector<NTlsUtils::TCertificatePair> Certificates;

public:
    TStaticCertificateProvider(
            TString rootCertPath,
            TVector<TCertificateFiles> certificates)
        : RootCaPair(NTlsUtils::LoadRootCaPair(std::move(rootCertPath)))
        , Certificates(NTlsUtils::LoadCertificatePairs(std::move(certificates)))
    {}

    std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() override
    {
        grpc::SslCredentialsOptions sslOptions{
            .pem_root_certs = RootCaPair.RootCa,
        };

        if (!Certificates.empty()) {
            const auto& cert = Certificates.front();
            sslOptions.pem_cert_chain = cert.Content.CertChain;
            sslOptions.pem_private_key = cert.Content.PrivateKey;
        }

        return grpc::SslCredentials(sslOptions);
    }

    std::shared_ptr<grpc::ServerCredentials>
        CreateSecureServerCredentials() override
    {
        grpc::SslServerCredentialsOptions sslOptions;

        sslOptions.client_certificate_request =
            GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;

        sslOptions.pem_root_certs = RootCaPair.RootCa;

        for (const auto& cert: Certificates) {
            sslOptions.pem_key_cert_pairs.push_back({
                .cert_chain = cert.Content.CertChain,
                .private_key = cert.Content.PrivateKey,
            });
        }

        return grpc::SslServerCredentials(sslOptions);
    }

    void Start() override
    {}

    void Stop() override
    {}
};

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
            "NCloudCertificateProvider");
        return kFactory.Create();
    }

    int CompareImpl(const grpc_tls_certificate_provider* other) const override
    {
        // gRPC sorts and searches providers with a three-way compare of their
        // addresses.
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

class TWatchedCertificateProvider final
    : public ICertificateProvider
{
private:
    // Shared with the watcher callbacks, which may outlive the provider.
    struct TState
    {
        const grpc_core::RefCountedPtr<TGrpcTlsCertificateProvider>
            TlsProvider =
                grpc_core::MakeRefCounted<TGrpcTlsCertificateProvider>();

        TMutex Mutex;
        TString RootCa;
        TVector<NTlsUtils::TIdentityContent> Identities;

        // Under Mutex, so that two publishes cannot reorder.
        void Publish()
        {
            PemKeyCertPairList identityPairs;
            for (const auto& identity: Identities) {
                if (identity.PrivateKey.empty() || identity.CertChain.empty()) {
                    continue;
                }
                identityPairs.emplace_back(
                    identity.PrivateKey,
                    identity.CertChain);
            }

            TMaybe<TString> rootCa = RootCa.empty()
                ? Nothing()
                : TMaybe<TString>(RootCa);
            if (rootCa.Defined() || !identityPairs.empty()) {
                TlsProvider->PublishCerts(rootCa, std::move(identityPairs));
            }
        }
    };

    const std::shared_ptr<TState> State = std::make_shared<TState>();
    const std::shared_ptr<grpc::experimental::CertificateProviderInterface>
        GrpcProvider;
    bool HasRootCa = false;
    bool HasIdentities = false;

public:
    TWatchedCertificateProvider(
            const ICertificateWatcherPtr& watcher,
            const TString& rootCertPath,
            TVector<TCertificateFiles> certificates)
        : GrpcProvider(
            std::make_shared<TGrpcCertificateProvider>(State->TlsProvider))
    {
        auto certs =
            NTlsUtils::PrepareCertificateFilePairs(std::move(certificates));
        HasIdentities = !certs.empty();
        HasRootCa = !rootCertPath.empty();

        // Held across the subscriptions, so that an update delivered in
        // between cannot be overwritten by the initial content.
        TGuard<TMutex> lock(State->Mutex);

        State->Identities.resize(certs.size());
        for (size_t i = 0; i < certs.size(); ++i) {
            State->Identities[i] = watcher->WatchIdentity(
                certs[i],
                [state = State, i](const NTlsUtils::TIdentityContent& content)
                {
                    TGuard<TMutex> lock(state->Mutex);
                    state->Identities[i] = content;
                    state->Publish();
                });
        }

        if (HasRootCa) {
            State->RootCa = watcher->WatchRootCa(
                rootCertPath,
                [state = State](const TString& rootCa)
                {
                    TGuard<TMutex> lock(state->Mutex);
                    state->RootCa = rootCa;
                    state->Publish();
                });
        }

        State->Publish();
    }

    std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() override
    {
        grpc::experimental::TlsChannelCredentialsOptions tlsOptions;
        tlsOptions.set_certificate_provider(GrpcProvider);
        if (HasIdentities) {
            tlsOptions.watch_identity_key_cert_pairs();
        }
        if (HasRootCa) {
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
        if (HasIdentities) {
            tlsOptions.watch_identity_key_cert_pairs();
        }
        if (HasRootCa) {
            tlsOptions.watch_root_certs();
        }
        return grpc::experimental::TlsServerCredentials(tlsOptions);
    }

    void Start() override
    {}

    void Stop() override
    {}
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ICertificateProviderPtr CreateStaticCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates)
{
    return std::make_shared<TStaticCertificateProvider>(
        std::move(rootCertPath),
        std::move(certificates));
}

ICertificateProviderPtr CreateCertificateProviderStub()
{
    return CreateStaticCertificateProvider({}, {});
}

ICertificateProviderPtr CreateCertificateProvider(
    ICertificateWatcherPtr watcher,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates)
{
    return std::make_shared<TWatchedCertificateProvider>(
        watcher,
        rootCertPath,
        std::move(certificates));
}

}   // namespace NCloud
