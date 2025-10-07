#pragma once

#include "public.h"

#include <cloud/blockstore/libs/client/session.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore::NClient {

////////////////////////////////////////////////////////////////////////////////
// The scheme of switching to a new session at runtime is as follows:
//
// 1. The new session is created.
// 2. The SwitchableSession is switched to the new session.
// 3. The old SwitchableClient is switched to the new SwitchableClient.
// 4. All new requests from Vhost redirected to new session.
// 5. Old requests that retried by old DurableClient and old Session are
//    redirected to the new SwitchableClient.
// 6. The old session stops doing periodic remounts.
// 7. The old session is drained.
// 8. The old session and all its underlying clients are deleted.
/*
         Vhost
           |
           v
        Endpoint
           |
           v
    SwitchableSession
           |        \
           X         \----------------\
           |                          |
           v                          v
        Session                   new Session
           |                          |
           v                          v
     ValidationClient          ValidationClient
           |                          |
           v                          v
     ThrottlingClient          ThrottlingClient
           |                          |
           v                          v
     EncryptionClient          EncryptionClient
           |                          |
           v                          v
      DurableClient             DurableClient
           |                          |
           v                          v
   DataIntegrityClient         DataIntegrityClient
           |                          |
           v                          v
    SwitchableClient   --->  new SwitchableClient
           |                          |
           X                          |
           |                          |
           v                          v
    StorageDataClient          StorageDataClient
*/

struct ISwitchableSession: public ISession
{
    virtual NThreading::TFuture<void> SwitchSession(
        const TString& newDiskId,
        const TString& newSessionId,
        ISessionPtr newSession,
        ISwitchableBlockStorePtr newSwitchableClient) = 0;
};

// Creates a wrapper session that allows you to switch to another session at
// runtime.
ISwitchableSessionPtr CreateSwitchableSession(
    ILoggingServicePtr logging,
    ISchedulerPtr scheduler,
    TString diskId,
    ISessionPtr session,
    ISwitchableBlockStorePtr switchableClient);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore::NClient
