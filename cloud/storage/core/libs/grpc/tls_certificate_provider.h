#pragma once

#include "public.h"

#include <grpcpp/security/tls_certificate_provider.h>

#include <cloud/storage/core/protos/error.pb.h>

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
    TString rootCertPath,
    TVector<TCertificateFiles> certificates,
    ui32 refreshIntervalSec);

}   // namespace NCloud
