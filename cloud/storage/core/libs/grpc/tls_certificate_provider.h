#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <grpcpp/security/credentials.h>
#include <grpcpp/security/tls_certificate_provider.h>
#include <grpcpp/security/server_credentials.h>

#include <library/cpp/threading/future/core/future.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/datetime/base.h>
#include <util/system/types.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

#include <memory>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TCertificateFiles
{
    TString PrivateKeyPath;
    TString CertChainPath;
};

////////////////////////////////////////////////////////////////////////////////

struct ICertificateProvider
    : IStartable
{
    virtual NThreading::TFuture<void> UpdateCertificates() = 0;
    virtual std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() = 0;
    virtual std::shared_ptr<grpc::ServerCredentials>
        CreateSecureServerCredentials() = 0;
};

ICertificateProviderPtr CreateStaticCertificateProvider(
    const TString& rootCertPath,
    TVector<TCertificateFiles> certificates);

ICertificateProviderPtr CreateCertificateProviderStub();

ICertificateProviderPtr CreateCertificateProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    NMonitoring::TDynamicCountersPtr serverGroup,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshIntervalSec);

}   // namespace NCloud
