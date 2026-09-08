#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <grpcpp/security/credentials.h>
#include <grpcpp/security/server_credentials.h>

#include <library/cpp/threading/future/core/future.h>

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
    // Requests a re-read of the certificate files. The future completes when
    // any new content has been either applied or rejected, or when the
    // provider is stopped. New content is applied only after it has stayed
    // unchanged for a while (see TStableRead), so this takes at least that
    // long, and content that keeps changing keeps the future pending until it
    // settles or the provider is stopped: wait on it with a timeout.
    // Providers that do not refresh certificates complete it right away.
    virtual NThreading::TFuture<void> UpdateCertificates() = 0;
    virtual std::shared_ptr<grpc::ChannelCredentials>
        CreateSecureClientCredentials() = 0;
    virtual std::shared_ptr<grpc::ServerCredentials>
        CreateSecureServerCredentials() = 0;
};

////////////////////////////////////////////////////////////////////////////////

ICertificateProviderPtr CreateStaticCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    ILoggingServicePtr logging,
    TString logComponent,
    NMonitoring::TDynamicCountersPtr serverGroup);

ICertificateProviderPtr CreateStaticCertificateProvider(
    TString rootCertPath,
    TVector<TCertificateFiles> certificates);

ICertificateProviderPtr CreateCertificateProviderStub();

ICertificateProviderPtr CreatePeriodicCertificateProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue,
    NMonitoring::TDynamicCountersPtr serverGroup,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshInterval,
    ITimerPtr timer);

ICertificateProviderPtr CreateCertificateProvider(
    ILoggingServicePtr logging,
    TString logComponent,
    ISchedulerPtr scheduler,
    ITaskQueuePtr taskQueue,
    NMonitoring::TDynamicCountersPtr serverGroup,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshInterval,
    ITimerPtr timer);

}   // namespace NCloud
