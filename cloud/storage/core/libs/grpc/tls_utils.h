#pragma once

#include "tls_certificate_provider.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>

#include <src/core/lib/security/credentials/tls/grpc_tls_certificate_provider.h>

#include <util/generic/strbuf.h>

namespace NCloud::NTlsUtils {

////////////////////////////////////////////////////////////////////////////////

struct TPendingIdentity
{
    TString PrivateKey;
    TString CertChain;

    bool operator==(const TPendingIdentity& other) const = default;
};

struct TCertificatePair
{
    TCertificateFiles Files;
    TString PrivateKey;
    TString CertChain;
    // Content that differs from the current one and has been read once, see
    // UpdateCertificates.
    TMaybe<TPendingIdentity> Pending;
};

struct TRootCaPair
{
    TString RootCaPath;
    TString RootCa;
    // Content that differs from the current one and has been read once, see
    // UpdateCertificates.
    TMaybe<TString> Pending;
};

struct TCertificateUpdate
{
    bool Changed = false;
    // Earliest notAfter of the chain, set when Changed.
    TInstant NotValidAfter;
};

struct TCertificatesUpdateResult
{
    bool RootCaChanged = false;
    TVector<TCertificateUpdate> Certificates;
    // Some content is waiting for a stable read.
    bool Pending = false;
};

////////////////////////////////////////////////////////////////////////////////

TResultOrError<TString> TryReadFile(const TString& path);

TResultOrError<void> IsValidPemCertificate(TStringBuf pem);

TResultOrError<void> PrivateKeyAndCertificateMatch(
    TStringBuf privateKey,
    TStringBuf certChain);

TResultOrError<void> ValidateIdentityCertificateValidity(
    TStringBuf certChainPem);

// Checks that the chain can be built from the leaf up to the last certificate
// the same way clients do it: issuer names, signatures, CA and name
// constraints. The last certificate serves as the trust anchor: there is no
// trust store here, and whether the chain ends at a trusted root is the
// client's job anyway.
TResultOrError<void> ValidateIdentityCertificateChain(
    TStringBuf certChainPem);

TResultOrError<ui64> GetCertificateNotAfterTimestampSec(
    TStringBuf certChainPem);

TResultOrError<TString> ReadAndValidateRootCertificate(
    const TString& rootCertPath);

TResultOrError<grpc_core::PemKeyCertPairList> ReadAndValidateIdentityPair(
    const TCertificateFiles& files);

TVector<TCertificateFiles> PrepareCertificateFilePairs(
    TVector<TCertificateFiles> certificates);

TVector<TCertificatePair> LoadCertificatePairs(
    TVector<TCertificateFiles> certificates);

TRootCaPair LoadRootCaPair(TString rootCaPath);

// Re-reads certificate files and updates |certificates| and |root| in place.
// Certificate files are rewritten by external tools, not necessarily
// atomically, and a partially written file may be syntactically valid, e.g. a
// chain without its intermediate certificate. Therefore new content is applied
// only after it has been read unchanged twice in a row (stable-read), a read
// error restarts the count. This is a heuristic that reduces the chance of
// picking up an intermediate state of a rewrite, not a guarantee: a writer
// that stalls for longer than the check interval is indistinguishable from a
// finished one. The last successfully loaded content is kept if the files
// cannot be read, parsed or validated; new content that fails these checks is
// reported on every call until the files change. Every certificate is
// refreshed independently.
TCertificatesUpdateResult UpdateCertificates(
    TVector<TCertificatePair>& certificates,
    TRootCaPair& root,
    TLog& log);

}   // namespace NCloud::NTlsUtils
