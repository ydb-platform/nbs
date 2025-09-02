#pragma once

#include "public.h"

#include <cloud/blockstore/libs/client/session.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////

struct ISwitchableSession: public ISession
{
    virtual NThreading::TFuture<void> Drain() = 0;
    virtual void SwitchSession(
        ISessionPtr newSession,
        const TString& newDiskId) = 0;
};

////////////////////////////////////////////////////////////////////////////////

ISwitchableSessionPtr CreateSwitchableSession(
    ILoggingServicePtr logging,
    TString diskId,
    ISessionPtr session);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NClient
