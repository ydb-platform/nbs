#pragma once

#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>

#include <src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h>

#include <util/generic/strbuf.h>

namespace NCloud::NTlsUtils {

////////////////////////////////////////////////////////////////////////////////

struct TCertificatePair
{
    TString PrivateKeyPath;
    TString CertChainPath;
    TString PrivateKey;
    TString CertChain;
};

struct TRootCaPair
{
    TString RootCaPath;
    TString RootCa;
};

struct TCertificatesUpdateResult
{
    TVector<TMaybe<grpc_core::PemKeyCertPairList>> Certificates;
    TMaybe<TString> RootCa;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> TryReadFile(const TString& path);

TResultOrError<void> IsValidPemCertificate(TStringBuf pem);

TResultOrError<void> PrivateKeyAndCertificateMatch(
    TStringBuf privateKey,
    TStringBuf certChain);

TResultOrError<void> ValidateIdentityCertificateWithRoot(
    TStringBuf rootCertPem,
    TStringBuf certChainPem);

TResultOrError<ui64> GetCertificateNotAfterTimestampSec(
    TStringBuf certChainPem);

TResultOrError<TString> ReadAndValidateRootCertificate(
    const TString& rootCertPath);

TResultOrError<grpc_core::PemKeyCertPairList> ReadAndValidateIdentityPair(
    const TCertificateFiles& files);

}   // namespace NCloud::NTlsUtils
