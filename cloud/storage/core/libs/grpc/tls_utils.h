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

// Checks that the private key matches the certificate, that every certificate
// in the chain is valid now and that the chain can be built from the leaf up
// to the last certificate the same way clients do it: issuer names,
// signatures, CA and name constraints. The last certificate serves as the
// trust anchor: there is no trust store here, and whether the chain ends at a
// trusted root is the client's job anyway. Applied to
// refreshed certificates; the initial load is lenient so that the service is
// able to start, see LoadCertificatePairs.
TResultOrError<void> ValidateIdentity(const TIdentityContent& identity);

TVector<TCertificateFiles> PrepareCertificateFilePairs(
    TVector<TCertificateFiles> certificates);

TVector<TCertificatePair> LoadCertificatePairs(
    TVector<TCertificateFiles> certificates);

TRootCaPair LoadRootCaPair(TString rootCaPath);

}   // namespace NCloud::NTlsUtils
