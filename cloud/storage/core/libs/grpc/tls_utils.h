#pragma once

#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>

#include <util/generic/strbuf.h>

namespace NCloud::NTlsUtils {

////////////////////////////////////////////////////////////////////////////////

// Contents of a private key file and a certificate chain file.
struct TIdentityContent
{
    TString PrivateKey;
    TString CertChain;

    bool operator==(const TIdentityContent& other) const = default;
};

struct TCertificatePair
{
    TCertificateFiles Files;
    TIdentityContent Content;
};

struct TRootCaPair
{
    TString RootCaPath;
    TString RootCa;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> TryReadFile(const TString& path);

TResultOrError<void> IsValidPemCertificate(TStringBuf pem);

TResultOrError<ui64> GetCertificateNotAfterTimestampSec(
    TStringBuf certChainPem);

TResultOrError<TIdentityContent> ReadIdentity(const TCertificateFiles& files);

// Checks that the key matches the leaf, every certificate is currently valid
// and the chain can be built up to its last certificate. Whether that
// certificate is trusted is not checked: that is the client's job.
TResultOrError<void> ValidateIdentity(const TIdentityContent& identity);

TVector<TCertificateFiles> PrepareCertificateFilePairs(
    TVector<TCertificateFiles> certificates);

TVector<TCertificatePair> LoadCertificatePairs(
    TVector<TCertificateFiles> certificates);

TRootCaPair LoadRootCaPair(TString rootCaPath);

}   // namespace NCloud::NTlsUtils
