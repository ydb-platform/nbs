#pragma once

#include "public.h"

#include <cloud/blockstore/config/rdma.pb.h>

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/threading/future/future.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct TClientConfig
{
    ui32 QueueSize = 10;
    ui32 MaxBufferSize = 1024*1024;
    EWaitMode WaitMode = EWaitMode::Poll;
    ui32 PollerThreads = 1;
    TDuration MaxReconnectDelay = TDuration::Seconds(60);
    TDuration MaxResponseDelay = TDuration::Seconds(60);
    TDuration AdaptiveWaitSleepDelay = TDuration::MilliSeconds(10);
    TDuration AdaptiveWaitSleepDuration = TDuration::MicroSeconds(100);

    TClientConfig() = default;

    TClientConfig(const NProto::TRdmaClient& config);
};

////////////////////////////////////////////////////////////////////////////////

// TNullContext is the base class for the user context passed to TClientRequest.
class TNullContext
{
public:
    virtual ~TNullContext() = default;
};

////////////////////////////////////////////////////////////////////////////////

// TClientRequest encapsulates all information for executing the request:
// input and output buffers, response handler, user context.
// Do not create TClientRequest directly, use IClientHandler::AllocateRequest().
struct TClientRequest
{
    IClientHandlerPtr Handler;
    std::unique_ptr<TNullContext> Context;

    TStringBuf RequestBuffer;
    TStringBuf ResponseBuffer;

    virtual ~TClientRequest() = default;

protected:
    TClientRequest(
            IClientHandlerPtr handler,
            std::unique_ptr<TNullContext> context)
        : Handler(std::move(handler))
        , Context(std::move(context))
    {}
};

////////////////////////////////////////////////////////////////////////////////

// IClientHandler interface is used to process the response on the user side.
struct IClientHandler
{
    virtual ~IClientHandler() = default;

    virtual void HandleResponse(
        TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) = 0;
};

////////////////////////////////////////////////////////////////////////////////

// IClientEndpoint interface is used to create and execute a request.
struct IClientEndpoint
{
    virtual ~IClientEndpoint() = default;

    virtual TResultOrError<TClientRequestPtr> AllocateRequest(
        IClientHandlerPtr handler,
        std::unique_ptr<TNullContext> context,
        size_t requestBytes,
        size_t responseBytes) = 0;

    virtual void SendRequest(
        TClientRequestPtr req,
        TCallContextPtr callContext) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public IStartable
{
    virtual ~IClient() = default;

    virtual NThreading::TFuture<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port) = 0;
};

}   // namespace NCloud::NBlockStore::NRdma
