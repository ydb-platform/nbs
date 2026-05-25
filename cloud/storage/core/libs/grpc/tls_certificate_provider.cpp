#include "tls_certificate_provider.h"

#include <util/generic/yexception.h>
#include <util/stream/file.h>

#include <memory>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

void ValidateCertificates(const TVector<TCertificateFiles>& certificates)
{
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
        return NThreading::MakeFuture();
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

ICertificateProviderPtr CreateStaticCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates)
{
    ValidateCertificates(certificates);

    return std::make_shared<TStaticCertificateProvider>(
        std::move(rootCertPath),
        std::move(certificates));
}

}   // namespace NCloud
