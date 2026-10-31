#pragma once

#include "public.h"
#include "tls_utils.h"

#include <cloud/storage/core/libs/common/startable.h>

#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct ICertificateProvider
    : IStartable
{
    virtual std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() = 0;
    virtual std::shared_ptr<grpc::ServerCredentials>
        CreateSecureServerCredentials() = 0;
};

////////////////////////////////////////////////////////////////////////////////

// Loads the files once. For tools; daemons use CreateCertificateProvider.
ICertificateProviderPtr CreateStaticCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates);

ICertificateProviderPtr CreateCertificateProviderStub();

// Subscribes to the files in |watcher|, which loads them right away and
// throws if that fails. Empty pairs are skipped.
ICertificateProviderPtr CreateCertificateProvider(
    ICertificateWatcherPtr watcher,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates);

}   // namespace NCloud
