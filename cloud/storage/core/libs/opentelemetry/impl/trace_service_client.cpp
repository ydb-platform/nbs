#include "trace_service_client.h"


#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/time_point_specialization.h>
#include <cloud/storage/core/libs/opentelemetry/iface/trace_service_client.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.grpc.pb.h>
#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/collector/trace/v1/trace_service.pb.h>

#include <util/string/builder.h>
#include <util/string/join.h>
#include <util/system/thread.h>

namespace NCloud {

using namespace NThreading;

namespace trace = opentelemetry::proto::collector::trace::v1;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr char AuthHeader[] = "authorization";
constexpr char AuthMethod[] = "Bearer";

////////////////////////////////////////////////////////////////////////////////

class TRequestHandler final
{
    using TReader =
        grpc::ClientAsyncResponseReader<trace::ExportTraceServiceResponse>;
    using TRequest = ITraceServiceClient::TRequest;
    using TResponse = ITraceServiceClient::TResponse;

private:
    grpc::ClientContext ClientContext;
    grpc::Status Status;

    std::unique_ptr<TReader> Reader;

    trace::ExportTraceServiceRequest Request;
    trace::ExportTraceServiceResponse Response;

    TPromise<TResponse> Promise = NewPromise<TResponse>();

public:
    TRequestHandler(TDuration timeout, TString authToken, TRequest request)
    {
        if (timeout != TDuration::Zero()) {
            ClientContext.set_deadline(TInstant::Now() + timeout);
        }

        if (authToken) {
            ClientContext.AddMetadata(
                AuthHeader,
                TStringBuilder() << AuthMethod << " " << authToken);
        }

        Request = std::move(request);
    }

    TFuture<TResponse> Execute(
        trace::TraceService::Stub& service,
        grpc::CompletionQueue* cq,
        void* tag)
    {
        Reader = service.AsyncExport(&ClientContext, Request, cq);
        Reader->Finish(&Response, &Status, tag);
        return Promise;
    }

    void Complete()
    {
        if (!Status.ok()) {
            Promise.SetValue(MakeError(
                MAKE_GRPC_ERROR(Status.error_code()),
                Status.error_message()));
        } else {
            Promise.SetValue(std::move(Response));
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TTraceServiceClient final
    : public ISimpleThread
    , public ITraceServiceClient
{
private:
    TGrpcInitializer GrpcInitializer;

    const ILoggingServicePtr Logging;
    const NProto::TGrpcClientConfig Config;

    TLog Log;

    grpc::CompletionQueue CQ;
    std::shared_ptr<trace::TraceService::Stub> Service;

public:
    TTraceServiceClient(
            ILoggingServicePtr logging,
            NProto::TGrpcClientConfig config)
        : Logging(std::move(logging))
        , Config(std::move(config))
        , Log(Logging->CreateLog("BLOCKSTORE_SERVER"))
    {}

    ~TTraceServiceClient()
    {
        Stop();
    }

    void Start() override
    {

        STORAGE_INFO("Connecting to " << Config.GetAddress());

        auto creds = Config.GetInsecure()
                         ? grpc::InsecureChannelCredentials()
                         : grpc::SslCredentials(grpc::SslCredentialsOptions());

        grpc::ChannelArguments args;
        if (Config.GetSslTargetNameOverride()) {
            args.SetSslTargetNameOverride(Config.GetSslTargetNameOverride());
        }

        auto channel = grpc::CreateCustomChannel(
            Config.GetAddress(),
            std::move(creds),
            args);

        Service = std::shared_ptr<trace::TraceService::Stub>(
            trace::TraceService::NewStub(std::move(channel)));

        ISimpleThread::Start();
    }

    void Stop() override
    {
        CQ.Shutdown();
        Join();
    }

    TFuture<TResponse> Export(
        TRequest traces,
        const TString& authToken) override
    {
        auto requestHandler = std::make_unique<TRequestHandler>(
            TDuration::MilliSeconds(Config.GetRequestTimeout()),
            authToken,
            std::move(traces));

        auto future =
            requestHandler->Execute(*Service, &CQ, requestHandler.release());

        return future;
    }

private:
    void* ThreadProc() override
    {
        void* tag;
        bool ok;
        while (CQ.Next(&tag, &ok)) {
            std::unique_ptr<TRequestHandler> requestHandler(
                static_cast<TRequestHandler*>(tag));
            requestHandler->Complete();
        }
        return nullptr;
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

ITraceServiceClientPtr CreateTraceServiceClient(
    ILoggingServicePtr logging,
    NProto::TGrpcClientConfig config)
{
    auto a = std::make_shared<TTraceServiceClient>(
        std::move(logging),
        std::move(config));
    return a;
}

}   // namespace NCloud
