#include "tls_certificate_provider.h"

#include "tls_utils.h"

#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

class TStaticCertificateProvider final
    : public ICertificateProvider
{
private:
    const TString RootCert;
    TVector<NTlsUtils::TCertificatePair> Certificates;

public:
    TStaticCertificateProvider(
            TString rootCertPath,
            TVector<TCertificateFiles> certificates)
        : RootCert(NTlsUtils::LoadRootCaPair(std::move(rootCertPath)).RootCa)
        , Certificates(NTlsUtils::LoadCertificatePairs(std::move(certificates)))
    {}

    NThreading::TFuture<void> UpdateCertificates() override
    {
        return NThreading::MakeFuture();
    }

    std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() override
    {
        grpc::SslCredentialsOptions sslOptions;
        if (RootCert) {
            sslOptions.pem_root_certs = RootCert;
        }

        if (!Certificates.empty()) {
            const auto& cert = Certificates.front();
            sslOptions.pem_cert_chain = cert.CertChain;
            sslOptions.pem_private_key = cert.PrivateKey;
        }

        return grpc::SslCredentials(sslOptions);
    }

    std::shared_ptr<grpc::ServerCredentials>
        CreateSecureServerCredentials() override
    {
        grpc::SslServerCredentialsOptions sslOptions;
        sslOptions.client_certificate_request =
            GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY;

        if (RootCert) {
            sslOptions.pem_root_certs = RootCert;
        }

        for (const auto& cert: Certificates) {
            grpc::SslServerCredentialsOptions::PemKeyCertPair keyCert;
            keyCert.cert_chain = cert.CertChain;
            keyCert.private_key = cert.PrivateKey;
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
    ILoggingServicePtr logging,
    TString logComponent,
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue,
    NMonitoring::TDynamicCountersPtr serverGroup,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshInterval)
{
    if (refreshInterval == TDuration::Zero()) {
        return CreateStaticCertificateProvider(
            std::move(rootCertPath),
            std::move(certificates));
    }

    auto certs = NTlsUtils::PrepareAndValidateCertificates(std::move(certificates));
    if (certs.empty()) {
        return CreateCertificateProviderStub();
    }

    return CreatePeriodicCertificateProvider(
        std::move(logging),
        std::move(logComponent),
        std::move(scheduler),
        std::move(taskQueue),
        std::move(serverGroup),
        std::move(rootCertPath),
        std::move(certs),
        refreshInterval);
}

}   // namespace NCloud
