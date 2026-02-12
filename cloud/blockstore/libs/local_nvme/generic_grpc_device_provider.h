#pragma once

#include "public.h"

#include "device_provider.h"

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

template <typename TTraits>
class TGenericGrpcDeviceProvider: public ILocalNVMeDeviceProvider
{
    using TSelf = TGenericGrpcDeviceProvider<TTraits>;

    using TService = typename TTraits::TService;
    using TServiceStub = typename TService::Stub;

    struct IRequestHandler
    {
        virtual ~IRequestHandler() = default;
        virtual void Complete() = 0;
    };

    struct TListDevicesRequestHandler: IRequestHandler
    {
        using TProtoRequest = typename TTraits::TListDevicesRequest;
        using TProtoResponse = typename TTraits::TListDevicesResponse;
        using TReader = grpc::ClientAsyncResponseReader<TProtoResponse>;
        using TResult = TVector<NProto::TNVMeDevice>;

        grpc::ClientContext ClientContext;
        grpc::Status Status;

        std::unique_ptr<TReader> Reader;

        TProtoRequest Request;
        TProtoResponse Response;

        NThreading::TPromise<TResult> Promise =
            NThreading::NewPromise<TResult>();

        auto Execute(TServiceStub& service, grpc::CompletionQueue& cq)
            -> NThreading::TFuture<TResult>
        {
            auto future = Promise.GetFuture();

            Reader = TTraits::AsyncListDevices(
                service,
                &ClientContext,
                Request,
                &cq);
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
                Promise.SetValue(TTraits::GetResult(std::move(Response)));
            }
        }
    };

private:
    TGrpcInitializer GrpcInitializer;

    const ILoggingServicePtr Logging;
    const TString SocketPath;

    TLog Log;

    std::thread Thread;
    grpc::CompletionQueue CQ;

    std::shared_ptr<TServiceStub> Service;

public:
    TGenericGrpcDeviceProvider(ILoggingServicePtr logging, TString socketPath)
        : Logging(std::move(logging))
        , SocketPath(std::move(socketPath))
    {}

    ~TGenericGrpcDeviceProvider() override
    {
        StopImpl();
    }

    void Start() final
    {
        Log = Logging->CreateLog("BLOCKSTORE_NVME");

        STORAGE_INFO("Connecting to " << SocketPath << " socket");

        auto channel = grpc::CreateChannel(
            "unix://" + SocketPath,
            grpc::InsecureChannelCredentials());

        Service.reset(TService::NewStub(std::move(channel)).release());

        Thread = std::thread{&TSelf::ThreadFn, this};
    }

    void Stop() final
    {
        StopImpl();
    }

    [[nodiscard]] auto ListNVMeDevices()
        -> NThreading::TFuture<TVector<NProto::TNVMeDevice>> final
    {
        auto handler = std::make_unique<TListDevicesRequestHandler>();
        auto future = handler->Execute(*Service, CQ);
        Y_UNUSED(handler.release());
        return future;
    }

private:
    void StopImpl()
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
