#pragma once

#include "public.h"

#include "buffer.h"

#include <cloud/storage/core/libs/common/public.h>
#include <cloud/storage/core/libs/common/startable.h>
#include <cloud/storage/core/libs/diagnostics/public.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/system/defaults.h>

namespace NCloud::NStorage::NRdma {

////////////////////////////////////////////////////////////////////////////////

struct TServerConfig
{
    ui32 Backlog = 10;
    ui32 QueueSize = 10;
    // Keep sync with MaxBufferSize in cloud/storage/core/libs/rdma/iface/client.h
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
    bool VerbsQP = false;
    TBufferPoolConfig BufferPool;
    ui32 SendQueueSize = 0;
    ui32 RecvQueueSize = 0;
    ui8 QpRetryCount = 7;
    ui8 QpRnrRetryCount = 7;
    ui8 QpTimeout = 0;
    ui8 QpMinRnrTimer = 0;

    TServerConfig();

    void Validate(TLog& log);

    void DumpHtml(IOutputStream& out) const;
};

////////////////////////////////////////////////////////////////////////////////

struct IServerSession
{
    virtual ~IServerSession() = default;

    [[nodiscard]] virtual ui64 GetId() const = 0;
    [[nodiscard]] virtual TString GetPeer() const = 0;
    [[nodiscard]] virtual TInstant GetStartTs() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IServerRequest
{
    virtual ~IServerRequest() = default;

    // Id of the connection the request arrived from, the same one that was
    // reported by IServerHandler::OnSessionCreated().
    [[nodiscard]] virtual ui64 GetSessionId() const = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IServerHandler
{
    virtual ~IServerHandler() = default;

    virtual TCallContextBasePtr CreateCallContext() = 0;

    virtual void HandleRequest(
        IServerRequest* context,
        TCallContextBasePtr callContext,
        TStringBuf in,
        TStringBuf out) = 0;

    virtual void OnSessionCreated(const IServerSession&) noexcept
    {}

    virtual void OnSessionClosed(ui64 sessionId) noexcept
    {
        Y_UNUSED(sessionId);
    }
};

////////////////////////////////////////////////////////////////////////////////

struct IServerEndpoint
{
    virtual ~IServerEndpoint() = default;

    virtual void SendResponse(
        IServerRequest* context,
        size_t responseBytes) = 0;
    virtual void SendError(
        IServerRequest* context,
        ui32 error,
        TStringBuf message) = 0;
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

}   // namespace NCloud::NStorage::NRdma
