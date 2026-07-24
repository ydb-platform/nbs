#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/root_kms/iface/public.h>

#include <util/datetime/base.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCreateRootKmsClientParams
{
    TString Address;
    TString RootCertsFile;
    TString CertChainFile;
    TString PrivateKeyFile;
    TDuration RequestTimeout = TDuration::Minutes(5);
    TString SslTargetNameOverride;
};

IRootKmsClientPtr CreateRootKmsClient(
    ILoggingServicePtr logging,
    TCreateRootKmsClientParams params);

}   // namespace NCloud::NBlockStore
