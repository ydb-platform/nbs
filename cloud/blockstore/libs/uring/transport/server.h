#pragma once

#include "public.h"

#include "config.h"

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/sglist.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

#include <memory>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

struct IServerHandler
{
    virtual ~IServerHandler() = default;

    virtual TCallContextBasePtr CreateCallContext() = 0;

    // Called on the IO thread. data lives in a transport buffer aligned to
    // 4 KiB and stays valid until SendResponse/SendError. proto is valid for
    // the duration of the call only. ctx is valid until SendResponse or
    // SendError, whatever happens to the session.
    virtual void HandleRequest(
        void* ctx,
        TCallContextBasePtr callContext,
        TStringBuf proto,
        TSgList data) = 0;
};

struct IServerEndpoint
{
    virtual ~IServerEndpoint() = default;

    // Any thread, exactly once per ctx. data is aliased when
    // DirectSgIoEnabled and owner is released once the kernel is done with
    // it; otherwise data is copied and owner released before this returns.
    // The request buffer handed to HandleRequest is released here, so it must
    // not be aliased by data.
    virtual void SendResponse(
        void* ctx,
        TStringBuf proto,
        TSgList data,
        std::shared_ptr<void> owner) = 0;

    virtual void SendError(void* ctx, ui32 error, TStringBuf message) = 0;
};

struct IServer: public IStartable
{
    virtual ~IServer() = default;

    virtual TResultOrError<IServerEndpointPtr>
    StartEndpoint(TString host, ui32 port, IServerHandlerPtr handler) = 0;

    virtual void DumpHtml(IOutputStream& out) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IServerPtr CreateServer(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TServerConfigPtr config);

// The port the endpoint actually listens on; useful with port 0 in tests.
TResultOrError<ui16> GetEndpointPort(const IServerEndpointPtr& endpoint);

}   // namespace NCloud::NBlockStore::NUring
