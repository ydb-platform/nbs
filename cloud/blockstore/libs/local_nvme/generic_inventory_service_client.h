#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/init.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/string.h>

#include <thread>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

template <typename TServiceTraits>
class TGenericInventoryServiceClient
{
    using TSelf = TGenericInventoryServiceClient<TServiceTraits>;

    using TService = typename TServiceTraits::TService;
    using TServiceStub = typename TService::Stub;
    using TProtoRequest = typename TServiceTraits::TListDevicesRequest;
    using TProtoResponse = typename TServiceTraits::TListDevicesResponse;

    struct IRequestHandler
    {
        virtual ~IRequestHandler() = default;
        virtual void Complete() = 0;
    };

    struct TListDevicesRequestHandler: IRequestHandler
    {
        using TReader = grpc::ClientAsyncResponseReader<TProtoResponse>;

        grpc::ClientContext ClientContext;
        grpc::Status Status;

        std::unique_ptr<TReader> Reader;

        TProtoRequest Request;
        TProtoResponse Response;

        NThreading::TPromise<TProtoResponse> Promise =
            NThreading::NewPromise<TProtoResponse>();

        explicit TListDevicesRequestHandler(TProtoRequest request)
            : Request(std::move(request))
        {}

        auto Execute(TServiceStub& service, grpc::CompletionQueue& cq)
            -> NThreading::TFuture<TProtoResponse>
        {
            auto future = Promise.GetFuture();

            ClientContext.set_wait_for_ready(true);

            Reader = TServiceTraits::AsyncListDevices(
                service,
                ClientContext,
                Request,
                cq);
            Reader->Finish(&Response, &Status, this);

            return future;
        }

        void Complete() override
        {
            if (!Status.ok()) {
                Promise.SetException(
                    std::make_exception_ptr(
                        TServiceError(MAKE_GRPC_ERROR(Status.error_code()))
                        << Status.error_message()));
            } else {
                Promise.SetValue(std::move(Response));
            }
        }
    };

private:
    TGrpcInitializer GrpcInitializer;

    const ILoggingServicePtr Logging;
    const TString SocketPath;

    std::thread Thread;
    grpc::CompletionQueue CQ;

    std::shared_ptr<TServiceStub> Service;

protected:
    TLog Log;

public:
    TGenericInventoryServiceClient(
        ILoggingServicePtr logging,
        TString socketPath)
        : Logging(std::move(logging))
        , SocketPath(std::move(socketPath))
    {}

    ~TGenericInventoryServiceClient()
    {
        Stop();
    }

    void Start()
    {
        Log = Logging->CreateLog("BLOCKSTORE_NVME");

        STORAGE_INFO("Connecting to " << SocketPath << " socket");

        auto channel = grpc::CreateChannel(
            "unix://" + SocketPath,
            grpc::InsecureChannelCredentials());

        Service.reset(TService::NewStub(std::move(channel)).release());

        Thread = std::thread{&TSelf::ThreadFn, this};
    }

    void Stop()
    {
        if (!Thread.joinable()) {
            return;
        }

        STORAGE_INFO("Stopping...");

        CQ.Shutdown();
        Thread.join();

        STORAGE_INFO("Stopped");

        Log = {};
    }

    [[nodiscard]] auto ListDevices(TProtoRequest request)
        -> NThreading::TFuture<TProtoResponse>
    {
        auto handler =
            std::make_unique<TListDevicesRequestHandler>(std::move(request));

        auto future = handler->Execute(*Service, CQ);
        Y_UNUSED(handler.release());
        return future;
    }

private:
    void ThreadFn()
    {
        SetCurrentThreadName("NVMeDP");

        void* tag = nullptr;
        bool ok = false;

        while (CQ.Next(&tag, &ok)) {
            std::unique_ptr<IRequestHandler> requestHandler(
                static_cast<IRequestHandler*>(tag));
            requestHandler->Complete();
        }
    }
};

}   // namespace NCloud::NBlockStore
