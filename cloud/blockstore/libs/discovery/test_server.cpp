#include "test_server.h"

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/grpc/init.h>

#include <cloud/blockstore/public/api/grpc/service.grpc.pb.h>

#include <contrib/libs/grpc/include/grpcpp/impl/codegen/completion_queue.h>
#include <contrib/libs/grpc/include/grpcpp/impl/codegen/status.h>
#include <contrib/libs/grpc/include/grpcpp/security/server_credentials.h>
#include <contrib/libs/grpc/include/grpcpp/server.h>
#include <contrib/libs/grpc/include/grpcpp/server_builder.h>

#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/printf.h>
#include <util/system/mutex.h>
#include <util/thread/factory.h>

#include <chrono>

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

TString ReadFile(const TString& name)
{
    return TFileInput(name).ReadAll();
}

////////////////////////////////////////////////////////////////////////////////

struct TRequestContext
{
    NProto::TPingRequest Request;
    NProto::TPingResponse Response;
    grpc::ServerContext ServerContext;
    grpc::ServerAsyncResponseWriter<NProto::TPingResponse> Writer;
    bool Done = false;

    TRequestContext(
            NProto::TBlockStoreService::AsyncService& service,
            grpc::ServerCompletionQueue& cq)
        : Writer(&ServerContext)
    {
        service.RequestPing(
            &ServerContext,
            &Request,
            &Writer,
            &cq,
            &cq,
            this
        );
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

struct TFakeBlockStoreServer::TImpl
{
    TGrpcInitializer GrpcInitializer;

    ui16 Port;
    ui16 SecurePort;
    TString RootCertsFile;
    TString KeyFile;
    TString CertFile;

    NProto::TBlockStoreService::AsyncService Service;
    std::unique_ptr<grpc::ServerCompletionQueue> CQ;
    std::unique_ptr<grpc::Server> Server;
    THolder<IThreadFactory::IThread> Thread;

    // response
    TMutex Lock;
    TString ErrorMessage;
    ui32 LastByteCount = 0;
    bool DropPingRequests = false;
    TVector<std::unique_ptr<TRequestContext>> DroppedRequests;
    TAtomic ShouldStop = 0;

    TImpl(
            ui16 port,
            ui16 securePort,
            const TString& rootCertsFile,
            const TString& keyFile,
            const TString& certFile)
        : Port(port)
        , SecurePort(securePort)
        , RootCertsFile(rootCertsFile)
        , KeyFile(keyFile)
        , CertFile(certFile)
    {
    }

    ~TImpl()
    {
        with_lock (Lock) {
            DropPingRequests = false;
            DroppedRequests.clear();
        }

        if (Server) {
            // Need to stop the loop explicitly to avoid possible data race
            // with grpc server shutdown.
            // See https://github.com/ydb-platform/nbs/issues/2809
            AtomicSet(ShouldStop, 1);
            if (Thread) {
                Thread->Join();
            }

            Server->Shutdown();
            CQ->Shutdown();
            // Need to drain completion queue, otherwise it fails on assert when destroying:
            // https://github.com/ydb-platform/nbs/blob/fbf6b9fa568b7b3861fbccffcf3177ee445f498a/contrib/libs/grpc/src/core/lib/surface/completion_queue.cc#L258
            DrainCompletionQueue();
        }
    }

    void Wait()
    {
        Thread->Join();
    }

    void PreStart()
    {
        grpc::ServerBuilder sb;
        sb.AddListeningPort(
            Sprintf("0.0.0.0:%u", Port),
            grpc::InsecureServerCredentials()
        );

        if (SecurePort) {
            grpc::SslServerCredentialsOptions options(
                GRPC_SSL_REQUEST_CLIENT_CERTIFICATE_AND_VERIFY);

            options.pem_root_certs = ReadFile(RootCertsFile);
            options.pem_key_cert_pairs.push_back({
                ReadFile(KeyFile),
                ReadFile(CertFile)
            });

            sb.AddListeningPort(
                Sprintf("0.0.0.0:%u", SecurePort),
                grpc::SslServerCredentials(options)
            );
        }

        sb.RegisterService(&Service);
        CQ = sb.AddCompletionQueue();
        Server = sb.BuildAndStart();

        Y_ENSURE(Server, "failed to start server");
    }

    void Loop()
    {
        new TRequestContext(Service, *CQ);

        void* tag;
        bool ok;
        while (!AtomicGet(ShouldStop)) {
            const auto deadline =
                std::chrono::system_clock::now() +
                std::chrono::milliseconds(100);
            auto status = CQ->AsyncNext(&tag, &ok, deadline);
            if (status != grpc::CompletionQueue::GOT_EVENT) {
                continue;
            }

            auto* requestContext = static_cast<TRequestContext*>(tag);

            if (requestContext->Done || !ok) {
                delete requestContext;
            } else {
                new TRequestContext(Service, *CQ);

                with_lock (Lock) {
                    if (DropPingRequests) {
                        DroppedRequests.emplace_back(requestContext);
                    } else {
                        FinishRequest(requestContext);
                    }
                }
            }
        }
    }

    void DrainCompletionQueue()
    {
        void* tag;
        bool ok;
        while (CQ->Next(&tag, &ok)) {
            auto* requestContext = static_cast<TRequestContext*>(tag);

            if (requestContext->Done || !ok) {
                delete requestContext;
            } else {
                FinishRequest(requestContext);
            }
        }
    }

    void FinishRequest(TRequestContext* requestContext)
    {
        requestContext->Done = true;

        auto& r = requestContext->Response;
        r.SetLastByteCount(LastByteCount);
        if (ErrorMessage) {
            r.MutableError()->SetCode(E_FAIL);
            r.MutableError()->SetMessage(ErrorMessage);
        }
        requestContext->Writer.Finish(
            r,
            grpc::Status::OK,
            requestContext
        );
    }

    void ForkStart()
    {
        PreStart();

        Thread = SystemThreadFactory()->Run([=, this] () {
            Loop();
        });
    }

    void Start()
    {
        PreStart();
        Loop();
    }
};

TFakeBlockStoreServer::TFakeBlockStoreServer(
    ui16 port,
    ui16 securePort,
    const TString& rootCertsFile,
    const TString& keyFile,
    const TString& certFile)
    : Impl(new TImpl(port, securePort, rootCertsFile, keyFile, certFile))
{
}

TFakeBlockStoreServer::~TFakeBlockStoreServer()
{
}

void TFakeBlockStoreServer::ForkStart()
{
    Impl->ForkStart();
}

void TFakeBlockStoreServer::Start()
{
    Impl->Start();
}

ui16 TFakeBlockStoreServer::Port() const
{
    return Impl->Port;
}

ui16 TFakeBlockStoreServer::SecurePort() const
{
    return Impl->SecurePort;
}

void TFakeBlockStoreServer::SetLastByteCount(ui64 count)
{
    with_lock (Impl->Lock) {
        Impl->LastByteCount = count;
    }
}

void TFakeBlockStoreServer::SetErrorMessage(TString error)
{
    with_lock (Impl->Lock) {
        Impl->ErrorMessage = std::move(error);
    }
}

void TFakeBlockStoreServer::SetDropPingRequests(bool drop)
{
    with_lock (Impl->Lock) {
        Impl->DropPingRequests = drop;
    }
}

}   // namespace NCloud::NBlockStore::NDiscovery
