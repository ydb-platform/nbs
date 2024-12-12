#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/root_kms/iface/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCreateRootKmsClientParams
{
    TString Address;
    TString RootCertsFile;
    TString CertChainFile;
    TString PrivateKeyFile;
};

IRootKmsClientPtr CreateRootKmsClient(
    ILoggingServicePtr logging,
    TCreateRootKmsClientParams params);

}   // namespace NCloud::NBlockStore
