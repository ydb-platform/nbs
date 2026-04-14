#pragma once

#include "public.h"
#include "tls_certificate_provider.h"

#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"

#include <library/cpp/logger/log.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/system/yassert.h>

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

inline TString ReadFile(const TString& fileName)
{
    TFileInput in(fileName);
    return in.ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TConfig>
std::shared_ptr<grpc::ChannelCredentials> CreateTcpClientChannelCredentials(
    TLog log,
    bool secureEndpoint,
    const TConfig& config)
{
    std::shared_ptr<grpc::ChannelCredentials> credentials;
    if (!secureEndpoint) {
        credentials = grpc::InsecureChannelCredentials();
    } else if (config.GetSkipCertVerification()) {
        grpc::experimental::TlsChannelCredentialsOptions tlsOptions;
        tlsOptions.set_verify_server_certs(false);
        credentials = grpc::experimental::TlsCredentials(tlsOptions);
    } else {
        const auto& rootCertsFile = config.GetRootCertsFile();
        const auto& certFile = config.GetCertFile();
        const auto& certPrivateKeyFile = config.GetCertPrivateKeyFile();
        const ui32 refreshCertsPeriod = config.GetRefreshCertsPeriod();
        const bool hasIdentity = !!certFile || !!certPrivateKeyFile;

        if (hasIdentity) {
            Y_ENSURE(certFile, "Empty CertFile");
            Y_ENSURE(certPrivateKeyFile, "Empty CertPrivateKeyFile");

            if (refreshCertsPeriod == 0) {
                grpc::SslCredentialsOptions sslOptions;
                if (rootCertsFile) {
                    sslOptions.pem_root_certs = ReadFile(rootCertsFile);
                }
                sslOptions.pem_private_key = ReadFile(certPrivateKeyFile);
                sslOptions.pem_cert_chain = ReadFile(certFile);
                credentials = grpc::SslCredentials(sslOptions);
            } else {
                TCertificateFiles certPaths;
                certPaths.PrivateKeyPath = certPrivateKeyFile;
                certPaths.CertChainPath = certFile;

                auto provider = CreatePeriodicCertificateProvider(
                    std::move(log),
                    rootCertsFile,
                    TVector<TCertificateFiles>{certPaths},
                    TDuration::Seconds(refreshCertsPeriod));

                grpc::experimental::TlsChannelCredentialsOptions tlsOptions;
                tlsOptions.set_certificate_provider(std::move(provider));

                tlsOptions.watch_identity_key_cert_pairs();
                if (rootCertsFile) {
                    tlsOptions.watch_root_certs();
                }

                credentials = grpc::experimental::TlsCredentials(tlsOptions);
            }
        } else {
            grpc::SslCredentialsOptions sslOptions;
            if (rootCertsFile) {
                sslOptions.pem_root_certs = ReadFile(rootCertsFile);
            }
            credentials = grpc::SslCredentials(sslOptions);
        }
    }
    return credentials;
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<grpc::ServerCredentials> CreateInsecureServerCredentials();

}   // namespace NCloud
