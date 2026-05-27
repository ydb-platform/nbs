#include "tls_certificate_provider.h"

#include <util/generic/yexception.h>
#include <util/stream/file.h>

#include <memory>

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

bool IsEmptyPair(const TCertificateFiles& certPair)
{
    return !certPair.PrivateKeyPath && !certPair.CertChainPath;
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
            TString rootCertPath,
            TVector<TCertificateFiles> certificates)
        : RootCert(ReadFile(rootCertPath))
    {
        for (size_t i = 0; i < certificates.size(); ++i) {
            const auto& cert = certificates[i];
            if (IsEmptyPair(cert)) {
                continue;
            }
            if (!cert.PrivateKeyPath) {
                ythrow yexception()
                    << "Empty PrivateKeyPath for certificate #"
                    << i;
            }
            if (!cert.CertChainPath) {
                ythrow yexception()
                    << "Empty CertChainPath for certificate #"
                    << i;
            }
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

}   // namespace NCloud
