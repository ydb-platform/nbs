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
    // Keep the value greater then MaxSubRequestSize, MigrationRangeSize,
    // ResyncRangeSize in cloud/blockstore/libs/service/device_handler.cpp
    // cloud/blockstore/libs/storage/model/common_constants.h
    // cloud/blockstore/libs/storage/partition_nonrepl/part_mirror_resync_util.h
    // Keep sync with MaxBufferSize in cloud/blockstore/vhost-server/options.h
    // and cloud/blockstore/libs/rdma/iface/server.h
    ui32 MaxBufferSize = 4_MB + 4_KB;
    EWaitMode WaitMode = EWaitMode::Poll;
    ui32 PollerThreads = 1;
    TDuration MaxReconnectDelay = TDuration::Seconds(60);
    TDuration MaxResponseDelay = TDuration::Seconds(60);
    TDuration AdaptiveWaitSleepDelay = TDuration::MilliSeconds(10);
    TDuration AdaptiveWaitSleepDuration = TDuration::MicroSeconds(100);
    bool AlignedDataEnabled = false;
    ui8 IpTypeOfService = 0;

    TClientConfig() = default;

    TClientConfig(const NProto::TRdmaClient& config);

    void DumpHtml(IOutputStream& out) const;
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

    // Returns id of sent request. It can be used to cancel this request.
    virtual ui64 SendRequest(
        TClientRequestPtr req,
        TCallContextPtr callContext) = 0;

    virtual void CancelRequest(ui64 reqId) = 0;

    virtual NThreading::TFuture<void> Stop() = 0;

    // Attempts to do an instant reconnect. Does nothing if the connection is
    // established.
    virtual void TryForceReconnect() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public IStartable
{
    virtual ~IClient() = default;

    virtual NThreading::TFuture<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port) = 0;

    virtual void DumpHtml(IOutputStream& out) const = 0;

    virtual bool IsAlignedDataEnabled() const = 0;
};

}   // namespace NCloud::NBlockStore::NRdma
