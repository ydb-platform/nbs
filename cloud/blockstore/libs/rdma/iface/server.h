#pragma once

#include "public.h"

#include <cloud/blockstore/config/rdma.pb.h>

#include <cloud/blockstore/libs/service/public.h>

#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NBlockStore::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
{
    ui32 Backlog = 10;
    ui32 QueueSize = 10;
    // Keep sync with MaxBufferSize in cloud/blockstore/libs/rdma/iface/client.h
    ui32 MaxBufferSize = 4_MB + 4_KB;
    TDuration KeepAliveTimeout = TDuration::Seconds(10);
    EWaitMode WaitMode = EWaitMode::Poll;
    ui32 PollerThreads = 1;
    bool StrictValidation = false;
    ui64 MaxInflightBytes = Max<ui64>();
    TDuration AdaptiveWaitSleepDelay = TDuration::MilliSeconds(10);
    TDuration AdaptiveWaitSleepDuration = TDuration::MicroSeconds(100);
    ui8 IpTypeOfService = 0;
    TString SourceInterface;

    TServerConfig() = default;

    explicit TServerConfig(const NProto::TRdmaServer& config);

    void DumpHtml(IOutputStream& out) const;
};

////////////////////////////////////////////////////////////////////////////////

struct IServerHandler
{
    virtual ~IServerHandler() = default;

    virtual void HandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IServerEndpoint
{
    virtual ~IServerEndpoint() = default;

    virtual void SendResponse(void* context, size_t responseBytes) = 0;
    virtual void SendError(void* context, ui32 error, TStringBuf message) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IServer
    : public IStartable
{
    virtual ~IServer() = default;

    virtual IServerEndpointPtr StartEndpoint(
        TString host,
        ui32 port,
        IServerHandlerPtr handler) = 0;

    virtual void DumpHtml(IOutputStream& out) const = 0;
};

}   // namespace NCloud::NBlockStore::NRdma
