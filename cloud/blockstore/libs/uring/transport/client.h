#pragma once

#include "public.h"
#include "config.h"

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/guarded_sglist.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

#include <memory>

namespace NCloud::NBlockStore::NUring {

////////////////////////////////////////////////////////////////////////////////

class TNullContext
{
public:
    virtual ~TNullContext() = default;
};

// Proto is copied by the transport; SendData and RecvData are aliased when
// DirectSgIoEnabled, staged otherwise. Timeout 0 means MaxResponseDelay.
struct TClientRequest
{
    IClientRequestHandlerPtr Handler;
    std::unique_ptr<TNullContext> Context;

    TString Proto;
    TGuardedSgList SendData;
    TGuardedSgList RecvData;
    TDuration Timeout;
};

struct IClientRequestHandler
{
    virtual ~IClientRequestHandler() = default;

    // On an IO thread, after every guard of the request has been released;
    // inline on the caller's thread only for a request sent after the
    // endpoint has stopped. status == 0: proto is the response. Otherwise
    // proto is a serialized TError and dataBytes is 0. proto is valid for the
    // call only.
    virtual void HandleResponse(
        TClientRequestPtr req,
        ui32 status,
        TStringBuf proto,
        size_t dataBytes) = 0;
};

struct IClientEndpointHandler
{
    virtual ~IClientEndpointHandler() = default;

    virtual void HandleConnected() = 0;
    virtual void HandleDisconnected() = 0;
    virtual void HandleUnavailable() = 0;
};

struct IClientEndpoint
{
    virtual ~IClientEndpoint() = default;

    // Any thread. Returns the id for CancelRequest.
    virtual ui64 SendRequest(
        TClientRequestPtr req,
        TCallContextBasePtr callContext) = 0;

    virtual void CancelRequest(ui64 id) = 0;

    // Resolved after teardown on an IO thread; never wait on it from one.
    virtual NThreading::TFuture<void> Stop() = 0;

    virtual void TryForceReconnect() = 0;
};

struct IClient: public IStartable
{
    virtual ~IClient() = default;

    // The endpoint is returned before it has connected; the handler reports
    // the state from then on. It may be empty.
    virtual TResultOrError<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port,
        IClientEndpointHandlerPtr handler) = 0;

    virtual void DumpHtml(IOutputStream& out) const = 0;
};

////////////////////////////////////////////////////////////////////////////////

IClientPtr CreateClient(
    ILoggingServicePtr logging,
    IMonitoringServicePtr monitoring,
    TClientConfigPtr config);

}   // namespace NCloud::NBlockStore::NUring
