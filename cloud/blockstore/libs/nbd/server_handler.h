#pragma once

#include "public.h"

#include <cloud/blockstore/libs/diagnostics/incomplete_requests.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/diagnostics/server_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/request.h>
#include <cloud/storage/core/libs/common/task_queue.h>

#include <library/cpp/coroutine/engine/network.h>
#include <library/cpp/threading/future/future.h>

#include <util/generic/buffer.h>
#include <util/generic/string.h>
#include <util/stream/input.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NBD {

////////////////////////////////////////////////////////////////////////////////

struct TRequestContext
    : public TIntrusiveListItem<TRequestContext>
    , TAtomicRefCount<TRequestContext>
{
    TCallContextPtr CallContext;
    TMetricRequest MetricRequest;

    TRequestContext(ui64 requestId, EBlockStoreRequest requestType)
        : CallContext(MakeIntrusive<TCallContext>(requestId))
        , MetricRequest(requestType)
    {}
};

using TRequestContextPtr = TIntrusivePtr<TRequestContext>;

////////////////////////////////////////////////////////////////////////////////

struct TServerResponse
    : public TAtomicRefCount<TServerResponse>
{
    const TRequestContextPtr RequestContext;
    const NProto::TError Error;
    const size_t RequestBytes;
    const TBuffer HeaderBuffer;
    const TStorageBuffer DataBuffer;

    TServerResponse(
            TRequestContextPtr requestContext,
            NProto::TError error,
            size_t requestBytes,
            TBuffer headerBuffer,
            TStorageBuffer dataBuffer = {})
        : RequestContext(std::move(requestContext))
        , Error(std::move(error))
        , RequestBytes(requestBytes)
        , HeaderBuffer(std::move(headerBuffer))
        , DataBuffer(std::move(dataBuffer))
    {}
};

using TServerResponsePtr = TIntrusivePtr<TServerResponse>;

////////////////////////////////////////////////////////////////////////////////

struct IServerContext : TThrRefBase, ITaskQueue
{
    virtual bool AcquireRequest(size_t requestBytes) = 0;

    virtual const NProto::TReadBlocksLocalResponse& WaitFor(
        const NThreading::TFuture<NProto::TReadBlocksLocalResponse>& future) = 0;
    virtual const NProto::TWriteBlocksLocalResponse& WaitFor(
        const NThreading::TFuture<NProto::TWriteBlocksLocalResponse>& future) = 0;
    virtual const NProto::TZeroBlocksResponse& WaitFor(
        const NThreading::TFuture<NProto::TZeroBlocksResponse>& future) = 0;

    virtual void SendResponse(TServerResponsePtr response) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IServerHandler
    : public IIncompleteRequestProvider
{
    virtual ~IServerHandler() = default;

    virtual bool NegotiateClient(
        IInputStream& in,
        IOutputStream& out) = 0;

    virtual void SendResponse(
        IOutputStream& out,
        TServerResponse& response) = 0;

    virtual void ProcessRequests(
        IServerContextPtr ctx,
        IInputStream& in,
        IOutputStream& out,
        TCont* cont) = 0;

    virtual void ProcessException(std::exception_ptr e) = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct IServerHandlerFactory
{
    virtual ~IServerHandlerFactory() = default;

    virtual IServerHandlerPtr CreateHandler() = 0;
};

////////////////////////////////////////////////////////////////////////////////

struct TStorageOptions
{
    TString DiskId;
    TString ClientId;
    ui32 BlockSize = 0;
    ui64 BlocksCount = 0;
    TString CheckpointId;
    bool UnalignedRequestsDisabled = false;
    bool SendMinBlockSize = false;
    bool CheckBufferModificationDuringWriting = false;
    bool IsReliableMediaType = false;
};

////////////////////////////////////////////////////////////////////////////////

IServerHandlerFactoryPtr CreateServerHandlerFactory(
    IDeviceHandlerFactoryPtr deviceHandlerFactory,
    ILoggingServicePtr logging,
    IStoragePtr storage,
    IServerStatsPtr serverStats,
    IErrorHandlerPtr errorHandler,
    const TStorageOptions& options);

}   // namespace NCloud::NBlockStore::NBD
