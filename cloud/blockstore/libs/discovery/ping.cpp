#include "ping.h"

#include <cloud/blockstore/public/api/grpc/service.grpc.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/grpc/init.h>
#include <cloud/storage/core/libs/grpc/time_point_specialization.h>

#include <contrib/libs/grpc/include/grpcpp/channel.h>
#include <contrib/libs/grpc/include/grpcpp/create_channel.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/async_unary_call.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/client_context.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/status.h>
#include <contrib/libs/grpc/include/grpcpp/security/credentials.h>

#include <ydb/library/actors/prof/tag.h>

#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/system/thread.h>

using namespace NThreading;

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TRequestContext
{
    using TReader = grpc::ClientAsyncResponseReader<NProto::TPingResponse>;

    grpc::CompletionQueue& CQ;
    TPingResponseInfo ResponseInfo;
    TInstant RequestStartTs;

    NProto::TBlockStoreService::Stub Service;
    grpc::ClientContext ClientContext;
    std::unique_ptr<TReader> Reader;
    grpc::Status Status;

    TPromise<TPingResponseInfo> Promise;

    TRequestContext(
            grpc::CompletionQueue& cq,
            TString host,
            ui16 port,
            TDuration timeout,
            std::shared_ptr<grpc::ChannelCredentials> channelCredentials)
        : CQ(cq)
        , ResponseInfo{std::move(host), port, {}, {}}
        , Service(grpc::CreateChannel(
            Sprintf("%s:%u", ResponseInfo.Host.c_str(), port),
            channelCredentials
        ))
        , Promise(NewPromise<TPingResponseInfo>())
    {
        RequestStartTs = Now();
        ClientContext.set_deadline(RequestStartTs + timeout);

        Reader = Service.AsyncPing(&ClientContext, NProto::TPingRequest(), &CQ);
    }
};

////////////////////////////////////////////////////////////////////////////////

class TPingClient final
    : public ISimpleThread
    , public IPingClient
{
private:
    TGrpcInitializer GrpcInitializer;

    grpc::CompletionQueue CQ;
    std::shared_ptr<grpc::ChannelCredentials> ChannelCredentials;

public:
    explicit TPingClient(std::shared_ptr<grpc::ChannelCredentials> channelCreds)
        : ChannelCredentials(std::move(channelCreds))
    {}

    void* ThreadProc() override
    {
        NProfiling::TMemoryTagScope tagScope("BLOCKSTORE_THREAD_PING_CLIENT");

        void* tag;
        bool ok;
        while (CQ.Next(&tag, &ok)) {
            std::unique_ptr<TRequestContext> requestContext(
                static_cast<TRequestContext*>(tag)
            );

            if (!requestContext->Status.ok()) {
                auto* e = requestContext->ResponseInfo.Record.MutableError();
                e->SetCode(MAKE_GRPC_ERROR(requestContext->Status.error_code()));
                e->SetMessage(requestContext->Status.error_message());
            }
            requestContext->ResponseInfo.Time =
                Now() - requestContext->RequestStartTs;

            requestContext->Promise.SetValue(
                std::move(requestContext->ResponseInfo)
            );
        }

        return nullptr;
    }

    TFuture<TPingResponseInfo> Ping(TString host, ui16 port, TDuration timeout) override
    {
        auto requestContext = std::make_unique<TRequestContext>(
            CQ,
            std::move(host),
            port,
            timeout,
            ChannelCredentials
        );

        auto future = requestContext->Promise.GetFuture();

        requestContext->Reader->Finish(
            &requestContext->ResponseInfo.Record,
            &requestContext->Status,
            &*requestContext
        );

        // At this point, requestContext object can already be freed after
        // request completion. It is not safe to use it.
        requestContext.release();

        return future;
    }

    void Start() override
    {
        ISimpleThread::Start();
    }

    void Stop() override
    {
        CQ.Shutdown();
        Join();
    }
};

TString ReadFile(const TString& name)
{
    return name
        ? TFileInput(name).ReadAll()
        : TString();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IPingClientPtr CreateSecurePingClient(const TString& rootCertFile)
{
    grpc::SslCredentialsOptions options{
        .pem_root_certs = ReadFile(rootCertFile)
    };

    return std::make_shared<TPingClient>(
        grpc::SslCredentials(options)
    );
}

IPingClientPtr CreateInsecurePingClient()
{
    return std::make_shared<TPingClient>(
        grpc::InsecureChannelCredentials()
    );
}

}   // namespace NCloud::NBlockStore::NDiscovery
