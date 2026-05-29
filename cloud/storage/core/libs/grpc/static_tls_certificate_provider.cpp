#include "tls_certificate_provider.h"

#include <util/generic/yexception.h>
#include <util/stream/file.h>

#include <memory>

namespace NCloud {

namespace {

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
    struct TCertificatePair
    {
        TString PrivateKeyPath;
        TString CertChainPath;
        TString PrivateKey;
        TString CertChain;
    };

private:
    const TString RootCert;
    TVector<TCertificatePair> Certificates;

public:
    TStaticCertificateProvider(
            const TString& rootCertPath,
            TVector<TCertificateFiles> certificates)
        : RootCert(rootCertPath ? ReadFile(rootCertPath) : "")
    {
        for (const auto& cert: certificates) {
            TCertificatePair certificate {
                .PrivateKeyPath = cert.PrivateKeyPath,
                .CertChainPath = cert.CertChainPath,
                .PrivateKey = ReadFile(cert.PrivateKeyPath),
                .CertChain = ReadFile(cert.CertChainPath)
            };
            Certificates.push_back(std::move(certificate));
        }
    }

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
    const TString& rootCertPath,
    TVector<TCertificateFiles> certificates)
{
    return std::make_shared<TStaticCertificateProvider>(
        rootCertPath,
        std::move(certificates));
}

ICertificateProviderPtr CreateCertificateProviderStub()
{
    return CreateStaticCertificateProvider({}, {});
}

}   // namespace NCloud