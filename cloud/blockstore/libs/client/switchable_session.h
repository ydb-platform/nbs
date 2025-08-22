#pragma once

#include "public.h"

#include <cloud/blockstore/libs/client/session.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct ISwitchableSession: public ISession
{
    virtual NThreading::TFuture<void> SwitchSession(
        const TString& newDiskId,
        ISessionPtr newSession,
        ISwitchableBlockStorePtr newSwitchableClient,
        IBlockStorePtr newDataClient,
        const TString& newSessionId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISwitchableSessionPtr CreateSwitchableSession(
    ILoggingServicePtr logging,
    ISchedulerPtr scheduler,
    TString diskId,
    ISessionPtr session,
    ISwitchableBlockStorePtr switchableClient,
    IBlockStorePtr dataClient);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NClient
