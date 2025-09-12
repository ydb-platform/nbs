
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

}   // namespace NCloud::NBlockStore
