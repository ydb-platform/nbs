#pragma once

#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/encryption/public.h>
#include <cloud/blockstore/libs/root_kms/iface/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

IRootKmsClientPtr CreateRootKmsClient(
    ILoggingServicePtr logging,
    TString address,
    TString rootCAPath,
    TString certKeyPath,
    TString privateKeyPath);

}   // namespace NCloud::NBlockStore
