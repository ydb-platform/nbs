#pragma once

#include "public.h"

#include "wait_mode.h"

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
    TDuration MaxReconnectDelay = TDuration::Minutes(10);
    TDuration MaxResponseDelay = TDuration::Minutes(10);
};

////////////////////////////////////////////////////////////////////////////////

struct TClientRequest
{
    void* Context = nullptr;

    TStringBuf RequestBuffer;
    TStringBuf ResponseBuffer;

    virtual ~TClientRequest() = default;
};

////////////////////////////////////////////////////////////////////////////////

struct IClientHandler
{
    virtual ~IClientHandler() = default;

    virtual void HandleResponse(
        TClientRequestPtr req,
        ui32 status,
        size_t responseBytes) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IClientEndpoint
{
    virtual ~IClientEndpoint() = default;

    virtual TResultOrError<TClientRequestPtr> AllocateRequest(
        void* context,
        size_t requestBytes,
        size_t responseBytes) = 0;

    virtual void SendRequest(
        TClientRequestPtr req,
        TCallContextPtr callContext) = 0;

    virtual void FreeRequest(TClientRequestPtr req) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IClient
    : public IStartable
{
    virtual ~IClient() = default;

    virtual NThreading::TFuture<IClientEndpointPtr> StartEndpoint(
        TString host,
        ui32 port,
        IClientHandlerPtr handler) = 0;
};

}   // namespace NCloud::NBlockStore::NRdma
