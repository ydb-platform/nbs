
#pragma once

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ISwitchableBlockStore: public IBlockStore
{
    virtual void Switch(
        IBlockStorePtr newClient,
        const TString& newDiskId,
        const TString& newSessionId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISwitchableBlockStorePtr CreateSwitchableClient(
    ILoggingServicePtr logging,
    TString diskId,
    IBlockStorePtr client);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
