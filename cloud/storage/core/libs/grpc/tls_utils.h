#pragma once

#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>

#include "src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h"

#include <string_view>

namespace NCloud::NTlsUtils {

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> TryReadFile(const TString& path);

TResultOrError<void> IsValidPemCertificate(std::string_view pem);

TResultOrError<void> PrivateKeyAndCertificateMatch(
    std::string_view privateKey,
    std::string_view certChain);

TResultOrError<void> ValidateIdentityCertificateWithRoot(
    std::string_view rootCertPem,
    std::string_view certChainPem);

TResultOrError<ui64> GetCertificateNotAfterTimestampSec(
    std::string_view certChainPem);

TResultOrError<TString> ReadAndValidateRootCertificate(
    const TString& rootCertPath);

TResultOrError<grpc_core::PemKeyCertPairList> ReadAndValidateIdentityPair(
    const TCertificateFiles& files);

}   // namespace NCloud::NTlsUtils
