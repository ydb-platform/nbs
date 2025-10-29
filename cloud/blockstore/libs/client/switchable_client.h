#pragma once

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ISwitchableBlockStore: public IBlockStore
{
    // Called before switching starts. All incoming requests for reading and
    // writing data are not started immediately after that, but are saved in
    // memory storage.
    virtual void BeforeSwitching() = 0;

    // Switches to a new client. All new received requests are redirected to the
    // new client.
    virtual void Switch(
        IBlockStorePtr newClient,
        const TString& newDiskId,
        const TString& newSessionId) = 0;

    // It is called after an attempt to switch to a new client. If the attempt
    // was successful, all requests stored in the temporary storage are
    // redirected to the new client. If the attempt was unsuccessful and
    // Switch() was not called, the requests stored in the temporary storage are
    // redirected to the original client.
    virtual void AfterSwitching() = 0;
};

////////////////////////////////////////////////////////////////////////////////

// ISwitchableBlockStore is used for dynamic switching between clients. By
// default, all requests are sent to the first client. After switching to the
// second client using the Switch() method, the read/write/zero requests start
// being sent to the second client, and the sessionId and diskId fields in
// requests are updated with new values. The remaining requests types continue
// to be sent to the first client. The redirection of requests and switching are
// performed without locking, with atomic variables.
// Only one switch can be made during the lifetime.

ISwitchableBlockStorePtr CreateSwitchableClient(
    ILoggingServicePtr logging,
    TString diskId,
    IBlockStorePtr client);

////////////////////////////////////////////////////////////////////////////////

// It allows to automate calls BeforeSwitching() and AfterSwitching() for
// ISwitchableBlockStore.

class TSessionSwitchingGuard;
using TSessionSwitchingGuardPtr = std::shared_ptr<TSessionSwitchingGuard>;

TSessionSwitchingGuardPtr CreateSessionSwitchingGuard(
    ISwitchableBlockStorePtr switchableDataClient);

////////////////////////////////////////////////////////////////////////////////

}   // namespace NCloud::NBlockStore
