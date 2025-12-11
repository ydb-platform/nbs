#pragma once

#include "public.h"

#include "grpcpp/security/credentials.h"
#include "grpcpp/security/server_credentials.h"

#include <util/stream/file.h>

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
        grpc::SslCredentialsOptions sslOptions;

        if (const auto& rootCertsFile = config.GetRootCertsFile()) {
            sslOptions.pem_root_certs = ReadFile(rootCertsFile);
        }

        if (const auto& certFile = config.GetCertFile()) {
            sslOptions.pem_cert_chain = ReadFile(certFile);
            sslOptions.pem_private_key =
                ReadFile(config.GetCertPrivateKeyFile());
        }

        credentials = grpc::SslCredentials(sslOptions);
    }
    return credentials;
}

////////////////////////////////////////////////////////////////////////////////

std::shared_ptr<grpc::ServerCredentials> CreateInsecureServerCredentials();

}   // namespace NCloud
