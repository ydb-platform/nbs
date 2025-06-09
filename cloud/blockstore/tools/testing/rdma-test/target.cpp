#include "target.h"

#include "options.h"
#include "probes.h"
#include "protocol.h"
#include "runnable.h"
#include "storage.h"

#include <cloud/blockstore/tools/testing/rdma-test/protocol.pb.h>

#include <cloud/blockstore/libs/rdma/iface/protobuf.h>
#include <cloud/blockstore/libs/rdma/iface/server.h>
#include <cloud/blockstore/libs/service/context.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/task_queue.h>

#include <util/string/builder.h>
#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/system/condvar.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

LWTRACE_USING(BLOCKSTORE_TEST_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

#define Y_ENSURE_RETURN(expr, message)                                         \
    if (Y_UNLIKELY(!(expr))) {                                                 \
        return MakeError(E_ARGUMENT, TStringBuilder() << message);             \
    }                                                                          \
// Y_ENSURE_RETURN

template <typename T>
auto CreateRequest(NRdma::TProtoMessagePtr proto)
{
    return std::shared_ptr<T>(static_cast<T*>(proto.release()));
}

////////////////////////////////////////////////////////////////////////////////

class TRequestHandler final
    : public NRdma::IServerHandler
{
private:
    const TOptionsPtr Options;
    const ITaskQueuePtr TaskQueue;
    const IStoragePtr Storage;

    NRdma::IServerEndpointPtr Endpoint;
    NRdma::TProtoMessageSerializer* Serializer = TBlockStoreProtocol::Serializer();

public:
    TRequestHandler(
            TOptionsPtr options,
            ITaskQueuePtr taskQueue,
            IStoragePtr storage)
        : Options(std::move(options))
        , TaskQueue(std::move(taskQueue))
        , Storage(std::move(storage))
    {}

    void SetEndpoint(NRdma::IServerEndpointPtr endpoint)
    {
        Endpoint = std::move(endpoint);
    }

    void HandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        TaskQueue->ExecuteSimple([=, this] {
            auto error = SafeExecute<NProto::TError>([=, this] {
                return DoHandleRequest(context, callContext, in, out);
            });

            if (HasError(error)) {
                Endpoint->SendError(
                    context,
                    error.GetCode(),
                    error.GetMessage());
            }
        });
    }

private:
    NProto::TError DoHandleRequest(
        void* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out)
    {
        auto resultOrError = Serializer->Parse(in);
        if (HasError(resultOrError)) {
            return resultOrError.GetError();
        }

        auto request = resultOrError.ExtractResult();
        switch (request.MsgId) {
            case TBlockStoreProtocol::ReadBlocksRequest:
                return HandleReadBlocksRequest(
                    context,
                    callContext,
                    CreateRequest<NProto::TReadBlocksRequest>(std::move(request.Proto)),
                    request.Data,
                    out);

            case TBlockStoreProtocol::WriteBlocksRequest:
                return HandleWriteBlocksRequest(
                    context,
                    callContext,
                    CreateRequest<NProto::TWriteBlocksRequest>(std::move(request.Proto)),
                    request.Data,
                    out);

            default:
                return MakeError(E_NOT_IMPLEMENTED, TStringBuilder()
                    << "request message not supported: " << request.MsgId);
        }
    }

    NProto::TError HandleReadBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        TReadBlocksRequestPtr request,
        TStringBuf requestData,
        TStringBuf out)
    {
        size_t dataSize = static_cast<ui64>(request->GetBlocksCount()) * request->GetBlockSize();
        Y_ENSURE_RETURN(dataSize && dataSize < 16_MB, "invalid request");
        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        // TODO: remove extra memcpy
        auto buffer = Storage->AllocateBuffer(dataSize);

        TGuardedSgList guardedSgList({
            { buffer.get(), dataSize }
        });

        auto future = Storage->ReadBlocks(
            std::move(callContext),
            std::move(request),
            guardedSgList);

        future.Subscribe([=, this] (auto future) {
            auto response = ExtractResponse(future);

            TaskQueue->ExecuteSimple([=, this] {
                Y_UNUSED(buffer);

                auto guard = guardedSgList.Acquire();
                Y_ABORT_UNLESS(guard);

                const auto& sglist = guard.Get();
                size_t responseBytes =
                    NRdma::TProtoMessageSerializer::SerializeWithData(
                        out,
                        TBlockStoreProtocol::ReadBlocksResponse,
                        0,   // flags
                        response,
                        sglist);

                Endpoint->SendResponse(context, responseBytes);
            });
        });

        return {};
    }

    NProto::TError HandleWriteBlocksRequest(
        void* context,
        TCallContextPtr callContext,
        TWriteBlocksRequestPtr request,
        TStringBuf requestData,
        TStringBuf out)
    {
        size_t dataSize = static_cast<ui64>(request->GetBlocksCount()) * request->GetBlockSize();
        Y_ENSURE_RETURN(dataSize && dataSize < 16_MB, "invalid request");
        Y_ENSURE_RETURN(requestData.length() == dataSize, "invalid request");

        TGuardedSgList guardedSgList({
            { requestData.data(), requestData.length() }
        });

        auto future = Storage->WriteBlocks(
            std::move(callContext),
            std::move(request),
            guardedSgList);

        future.Subscribe([=, this] (auto future) {
            auto response = ExtractResponse(future);

            TaskQueue->ExecuteSimple([=, this] {
                Y_UNUSED(guardedSgList);

                size_t responseBytes =
                    NRdma::TProtoMessageSerializer::Serialize(
                        out,
                        TBlockStoreProtocol::WriteBlocksResponse,
                        0,   // flags
                        response);

                Endpoint->SendResponse(context, responseBytes);
            });
        });

        return {};
    }
};

using TRequestHandlerPtr = std::shared_ptr<TRequestHandler>;

////////////////////////////////////////////////////////////////////////////////

class TTestTarget final
    : public IRunnable
{
private:
    const TOptionsPtr Options;
    const ITaskQueuePtr TaskQueue;
    const IStoragePtr Storage;
    const NRdma::IServerPtr Server;

    TRequestHandlerPtr RequestHandler;

    TMutex WaitMutex;
    TCondVar WaitCondVar;

    TAtomic ShouldStop = 0;
    TAtomic ExitCode = 0;

public:
    TTestTarget(
            TOptionsPtr options,
            ITaskQueuePtr taskQueue,
            IStoragePtr storage,
            NRdma::IServerPtr server)
        : Options(std::move(options))
        , TaskQueue(std::move(taskQueue))
        , Storage(std::move(storage))
        , Server(std::move(server))
    {
        RequestHandler = std::make_shared<TRequestHandler>(
            Options,
            TaskQueue,
            Storage);

        auto serverEndpoint = Server->StartEndpoint(
            Options->Host,
            Options->Port,
            RequestHandler);

        RequestHandler->SetEndpoint(std::move(serverEndpoint));
    }

    int Run() override
    {
        WaitForShutdown();

        return AtomicGet(ExitCode);
    }

    void Stop(int exitCode) override
    {
        AtomicSet(ExitCode, exitCode);
        AtomicSet(ShouldStop, 1);

        WaitCondVar.Signal();
    }

private:
    void WaitForShutdown()
    {
        with_lock (WaitMutex) {
            while (AtomicGet(ShouldStop) == 0) {
                WaitCondVar.WaitT(WaitMutex, WAIT_TIMEOUT);
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IRunnablePtr CreateTestTarget(
    TOptionsPtr options,
    ITaskQueuePtr taskQueue,
    IStoragePtr storage,
    NRdma::IServerPtr server)
{
    return std::make_shared<TTestTarget>(
        std::move(options),
        std::move(taskQueue),
        std::move(storage),
        std::move(server));
}

}   // namespace NCloud::NBlockStore
