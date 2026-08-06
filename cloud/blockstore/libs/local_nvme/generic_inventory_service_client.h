#pragma once

#include "public.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/thread.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/time_point_specialization.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash_set.h>
#include <util/generic/scope.h>
#include <util/generic/string.h>

#include <mutex>
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
        virtual void Complete(bool ok) = 0;
        virtual void Cancel() = 0;
    };

    struct TListDevicesRequestHandler final: IRequestHandler
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

        void Execute(
            TServiceStub& service,
            grpc::CompletionQueue& cq,
            TInstant deadline)
        {
            ClientContext.set_wait_for_ready(true);
            ClientContext.set_deadline(deadline);

            Reader = TServiceTraits::AsyncListDevices(
                service,
                ClientContext,
                Request,
                cq);
            Reader->Finish(&Response, &Status, this);
        }

        void Complete(bool ok) final
        {
            if (!ok) {
                Promise.SetException(
                    std::make_exception_ptr(
                        TServiceError(E_REJECTED)
                        << "gRPC completion operation was cancelled"));
                return;
            }

            if (!Status.ok()) {
                Promise.SetException(
                    std::make_exception_ptr(
                        TServiceError(MAKE_GRPC_ERROR(Status.error_code()))
                        << Status.error_message()));
            } else {
                Promise.SetValue(std::move(Response));
            }
        }

        void Cancel() final
        {
            ClientContext.TryCancel();
        }
    };

private:
    TGrpcInitializer GrpcInitializer;

    const ILoggingServicePtr Logging;
    const TString SocketPath;
    const TDuration RequestTimeout = TDuration::Seconds(30);

    std::thread Thread;
    grpc::CompletionQueue CQ;

    std::shared_ptr<TServiceStub> Service;

    std::mutex Mutex;
    THashSet<IRequestHandler*> ActiveRequests;

    bool ShouldStop = false;

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
        Y_DEBUG_ABORT_UNLESS(!Thread.joinable());
    }

    void Start()
    {
        Y_DEBUG_ABORT_UNLESS(!Thread.joinable());

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
        {
            std::lock_guard guard(Mutex);
            if (ShouldStop) {
                return;
            }

            ShouldStop = true;

            STORAGE_INFO(
                "Stopping, active requests: " << ActiveRequests.size());

            for (auto* request: ActiveRequests) {
                request->Cancel();
            }

            CQ.Shutdown();
        }

        Thread.join();
        Y_DEBUG_ABORT_UNLESS(ActiveRequests.empty());

        Service.reset();

        STORAGE_INFO("Stopped");

        Log = {};
    }

    [[nodiscard]] auto ListDevices(TProtoRequest request)
        -> NThreading::TFuture<TProtoResponse>
    {
        auto handler =
            std::make_unique<TListDevicesRequestHandler>(std::move(request));

        auto future = handler->Promise.GetFuture();

        {
            std::lock_guard guard(Mutex);

            if (ShouldStop) {
                handler->Promise.SetException(
                    std::make_exception_ptr(
                        TServiceError(E_REJECTED)
                        << "Inventory service is stopping"));
                return future;
            }

            ActiveRequests.insert(handler.get());
            Y_DEFER
            {
                if (handler) {
                    ActiveRequests.erase(handler.get());
                }
            };

            handler->Execute(*Service, CQ, RequestTimeout.ToDeadLine());
            Y_UNUSED(handler.release());
        }

        return future;
    }

private:
    void UnregisterRequest(IRequestHandler* request)
    {
        std::lock_guard guard(Mutex);
        ActiveRequests.erase(request);
    }

    void ThreadFn()
    {
        SetCurrentThreadName("NVMeDP");

        void* tag = nullptr;
        bool ok = false;

        while (CQ.Next(&tag, &ok)) {
            std::unique_ptr<IRequestHandler> requestHandler(
                static_cast<IRequestHandler*>(tag));

            UnregisterRequest(requestHandler.get());

            requestHandler->Complete(ok);
        }
    }
};

}   // namespace NCloud::NBlockStore
