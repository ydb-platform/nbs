#pragma once

#include "public.h"

#include <cloud/storage/core/libs/diagnostics/public.h>
#include <cloud/storage/core/protos/error.pb.h>

#include <grpcpp/security/tls_certificate_provider.h>

#include <library/cpp/threading/future/future.h>

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

struct ICertificateProvider
    : grpc::experimental::CertificateProviderInterface
{
    virtual NThreading::TFuture<NProto::TError> UpdateCertificates() = 0;
};

std::shared_ptr<ICertificateProvider> CreatePeriodicCertificateProvider(
    TLog log,
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    TDuration refreshIntervalSec);

}   // namespace NCloud
