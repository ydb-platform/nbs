#include "rdma_target.h"

#include "mount_registry.h"
#include "rdma_protocol.h"

#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/service/service.h>

#include <cloud/storage/core/libs/common/block_data_ref.h>
#include <cloud/storage/core/libs/common/thread_pool.h>
#include <cloud/storage/core/libs/coroutine/executor.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>
#include <cloud/storage/core/libs/rdma/iface/probes.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>
#include <cloud/storage/core/libs/rdma/iface/protocol.h>
#include <cloud/storage/core/libs/rdma/iface/server.h>

#include <library/cpp/monlib/service/pages/html_mon_page.h>
#include <library/cpp/monlib/service/pages/index_mon_page.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/hash.h>
#include <util/generic/list.h>
#include <util/stream/format.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NThreading;
using namespace NMonitoring;
using namespace NCloud::NStorage::NRdma;

LWTRACE_USING(STORAGE_RDMA_PROVIDER);

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr size_t MaxRealProtoSize =
    4_KB - NCloud::NStorage::NRdma::RDMA_PROTO_HEADER_SIZE;

////////////////////////////////////////////////////////////////////////////////

#define Y_ENSURE_RETURN(expr, message)                             \
    if (Y_UNLIKELY(!(expr))) {                                     \
        return MakeError(E_ARGUMENT, TStringBuilder() << message); \
    }                                                              \
    // Y_ENSURE_RETURN

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_RETURN_TRUE_CASE(name, ...)         \
    case TBlockStoreServerProtocol::Ev##name##Request: \
        return true;                                   \
                                                       \
        // BLOCKSTORE_RETURN_TRUE_CASE

#undef BLOCKSTORE_RETURN_TRUE_CASE

////////////////////////////////////////////////////////////////////////////////

template <typename TResponse>
void FillResponse(const TCallContextPtr& callContext, TResponse& response)
{
    const ui64 postponeTime =
        callContext->Time(EProcessingStage::Postponed).MicroSeconds();
    response.SetDeprecatedThrottlerDelay(postponeTime);
    response.MutableHeaders()->MutableThrottler()->SetDelay(postponeTime);

    const ui64 shapingTime =
        callContext->Time(EProcessingStage::Shaping).MicroSeconds();
    response.MutableHeaders()->MutableThrottler()->SetShapingDelay(shapingTime);
}

////////////////////////////////////////////////////////////////////////////////

TMountInfo MakeMountInfo(const NProto::TMountVolumeRequest& request)
{
    TMountInfo info;
    info.DiskId = request.GetDiskId();
    info.ClientId = request.GetHeaders().GetClientId();
    info.VolumeAccessMode = request.GetVolumeAccessMode();
    info.VolumeMountMode = request.GetVolumeMountMode();
    info.MountSeqNumber = request.GetMountSeqNumber();
    return info;
}

////////////////////////////////////////////////////////////////////////////////

// Thread-safe. After Init() public method HandleRequest() can be called
// from any thread.
class TRequestHandler final
    : public NCloud::NStorage::NRdma::IServerHandler
    , public std::enable_shared_from_this<TRequestHandler>
{
    IBlockStorePtr Service;
    ITraceSerializerPtr TraceSerializer;
    ITaskQueuePtr TaskQueue;

    // not set when nobody is going to look at the connections
    TMountRegistryPtr MountRegistry;

    TLog Log;
    std::weak_ptr<NCloud::NStorage::NRdma::IServerEndpoint> Endpoint;

    const NCloud::NStorage::NRdma::TProtoMessageSerializer* Serializer =
        TBlockStoreServerProtocol::Serializer();

public:
    TRequestHandler(
        IBlockStorePtr service,
        ITraceSerializerPtr traceSerializer,
        ITaskQueuePtr taskQueue,
        TMountRegistryPtr mountRegistry)
        : Service(std::move(service))
        , TraceSerializer(std::move(traceSerializer))
        , TaskQueue(std::move(taskQueue))
        , MountRegistry(std::move(mountRegistry))
    {}

    void Init(
        const NCloud::NStorage::NRdma::IServerEndpointPtr& endpoint,
        TLog log)
    {
        Endpoint = endpoint;
        Log = std::move(log);
    }

    TCallContextBasePtr CreateCallContext() override
    {
        return NCloud::NBlockStore::CreateCallContext();
    }

    void OnSessionCreated(
        const NCloud::NStorage::NRdma::IServerSession& session) noexcept override
    {
        if (!MountRegistry) {
            return;
        }

        // the session reference is only valid for the duration of this call,
        // so everything the registry needs is copied out right here
        MountRegistry->AddConnection(
            session.GetId(),
            session.GetPeer(),
            session.GetStartTs());
    }

    void OnSessionClosed(ui64 sessionId) noexcept override
    {
        if (!MountRegistry) {
            return;
        }

        MountRegistry->RemoveConnection(sessionId);
    }

private:

#define BLOCKSTORE_HANDLE_REQUEST(name, ...)                             \
    case TBlockStoreServerProtocol::Ev##name##Request:                   \
        return Handle##name##Request(                                    \
            context,                                                     \
            std::move(callContext),                                      \
            static_cast<NProto::T##name##Request*>(&*parseResult.Proto), \
            parseResult.Data,                                            \
            out);

    // BLOCKSTORE_HANDLE_REQUEST

    NProto::TError DoHandleRequest(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TCallContextPtr callContext,
        TStringBuf in,
        TStringBuf out) const
    {
        auto [parseResult, error] = Serializer->Parse(in);

        if (HasError(error)) {
            STORAGE_ERROR("Can't parse input: %s", FormatError(error).c_str())
            return error;
        }

        STORAGE_TRACE("Processing req with msgId %u", parseResult.MsgId);

        switch (parseResult.MsgId) {
            BLOCKSTORE_HANDLE_REQUEST(ReadBlocks)
            BLOCKSTORE_HANDLE_REQUEST(WriteBlocks)
            BLOCKSTORE_HANDLE_REQUEST(ZeroBlocks)
            BLOCKSTORE_HANDLE_REQUEST(Ping)
            BLOCKSTORE_HANDLE_REQUEST(MountVolume)
            BLOCKSTORE_HANDLE_REQUEST(UnmountVolume)

            default:
                return MakeError(
                    E_NOT_IMPLEMENTED,
                    TStringBuilder()
                        << "Request with msg id " << parseResult.MsgId
                        << " is not supported by blockstore server RDMA "
                           "target");
        }
    }

#undef BLOCKSTORE_HANDLE_REQUEST

    void HandleRequest(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TCallContextBasePtr callContext,
        TStringBuf in,
        TStringBuf out) override
    {
        TaskQueue->ExecuteSimple(
            [=,
             endpoint = Endpoint,
             callContext =
                 ToBlockStoreCallContext(std::move(callContext))]() mutable
            {
                auto error = SafeExecute<NProto::TError>(
                    [=]()
                    { return DoHandleRequest(context, callContext, in, out); });

                if (HasError(error)) {
                    if (auto ep = endpoint.lock()) {
                        ep->SendError(
                            context,
                            error.GetCode(),
                            error.GetMessage());
                    }
                }
            });
    }

    void OnVolumeMounted(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TMountInfo info) const
    {
        if (!context || !MountRegistry) {
            return;
        }

        MountRegistry->AddMount(context->GetSessionId(), std::move(info));
    }

    void OnVolumeUnmounted(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TString diskId,
        TString clientId) const
    {
        if (!context || !MountRegistry) {
            return;
        }

        MountRegistry->RemoveMount(
            context->GetSessionId(),
            std::move(diskId),
            std::move(clientId));
    }

    size_t SerializeReadBlocksResponse(
        const NProto::TReadBlocksResponse& response,
        TStringBuf out,
        ui32 flags,
        TBlockDataRef data) const
    {
        return SUCCEEDED(response.GetError().GetCode())
                   ? TProtoMessageSerializer::SerializeWithData(
                         out,
                         TBlockStoreServerProtocol::EvReadBlocksResponse,
                         flags,
                         response,
                         TBlockDataRefSpan{{data}})
                   : TProtoMessageSerializer::Serialize(
                         out,
                         TBlockStoreServerProtocol::EvReadBlocksResponse,
                         flags,
                         response);
    }

    template <typename TResponse>
    void OnSerializeException(
        TResponse& response,
        TStringBuf responseName) const
    {
        STORAGE_ERROR(
            TStringBuilder()
            << "Unable to serialize " << responseName << " protobuf: ["
            << response.ShortDebugString()
            << "] with exception: " << CurrentExceptionMessage());
    }

    NProto::TError HandleReadBlocksRequest(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TCallContextPtr callContext,
        NProto::TReadBlocksRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");
        Y_ENSURE_RETURN(request->GetBlockSize() != 0, "empty BlockSize");

        TGuardedBuffer buffer(
            TString::Uninitialized(
                static_cast<size_t>(request->GetBlockSize()) *
                request->GetBlocksCount()));

        auto [sglist, error] = SgListNormalize(
            TBlockDataRef{buffer.Get().data(), buffer.Get().length()},
            request->GetBlockSize());
        Y_ENSURE_RETURN(error.GetCode() == 0, "cannot create sgList");

        auto guardedSgList = buffer.CreateGuardedSgList(std::move(sglist));

        auto req = std::make_shared<NProto::TReadBlocksLocalRequest>();
        req->CopyFrom(*request);

        req->Sglist = guardedSgList;

        auto future = Service->ReadBlocksLocal(callContext, std::move(req));

        future.Subscribe(
            [=,
             buffer = std::move(buffer),
             guardedSgList = std::move(guardedSgList),
             blockSize = request->GetBlockSize(),
             taskQueue = TaskQueue,
             endpoint = Endpoint,
             weakSelf = weak_from_this()](
                const TFuture<NProto::TReadBlocksLocalResponse>& future) mutable
            {
                taskQueue->ExecuteSimple(
                    [=,
                     future = future,
                     buffer = std::move(buffer),
                     guardedSgList = std::move(guardedSgList),
                     weakSelf = std::move(weakSelf)]() mutable
                    {
                        auto response =
                            SafeExecute<NProto::TReadBlocksLocalResponse>(
                                [&] { return future.GetValue();});
                        FillResponse(callContext, response);

                        guardedSgList.Close();

                        if (response.ByteSizeLong() > MaxRealProtoSize) {
                            // TODO: consider variable length proto size
                            // or switch from lwtrace to open telemetry like
                            // solution to avoid sending traces between nodes
                            response.MutableDeprecatedTrace()->Clear();
                            response.MutableHeaders()->ClearTrace();
                        }

                        ui32 flags = 0;
                        SetProtoFlag(
                            flags,
                            NCloud::NStorage::NRdma::
                                RDMA_PROTO_FLAG_DATA_AT_THE_END);

                        size_t responseBytes = 0;
                        try {
                            responseBytes = SerializeReadBlocksResponse(
                                response,
                                out,
                                flags,
                                TBlockDataRef{
                                    buffer.Get().data(),
                                    buffer.Get().length()});
                        } catch (...) {
                            if (auto self = weakSelf.lock()) {
                                self->OnSerializeException(
                                    response,
                                    "ReadBlocks");
                            }
                            if (auto ep = endpoint.lock()) {
                                ep->SendError(
                                    context,
                                    E_REJECTED,
                                    "unable to serialize ReadBlocks response");
                            }
                            return;
                        }

                        if (auto ep = endpoint.lock()) {
                            ep->SendResponse(context, responseBytes);
                        }
                    });
            });

        return {};
    }

    NProto::TError HandleWriteBlocksRequest(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TCallContextPtr callContext,
        NProto::TWriteBlocksRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() > 0, "invalid request");
        Y_ENSURE_RETURN(request->GetBlockSize() != 0, "empty BlockSize");

        auto [sglist, error] = SgListNormalize(
            {requestData.data(), requestData.length()},
            request->GetBlockSize());
        Y_ENSURE_RETURN(error.GetCode() == 0, "cannot create sgList");

        TGuardedSgList guardedSgList(sglist);

        auto req = std::make_shared<NProto::TWriteBlocksLocalRequest>();
        req->CopyFrom(*request);

        req->Sglist = guardedSgList;
        req->SetBlockSize(request->GetBlockSize());
        req->BlocksCount = requestData.length() / req->GetBlockSize();

        auto future = Service->WriteBlocksLocal(callContext, std::move(req));

        future.Subscribe(
            [=,
             guardedSgList = std::move(guardedSgList),
             taskQueue = TaskQueue,
             endpoint = Endpoint,
             weakSelf = weak_from_this()](
                const TFuture<NProto::TWriteBlocksLocalResponse>&
                    future) mutable
            {
                taskQueue->ExecuteSimple(
                    [=,
                     guardedSgList = std::move(guardedSgList),
                     future = future]() mutable
                    {
                        auto response =
                            SafeExecute<NProto::TWriteBlocksLocalResponse>(
                                [&] { return future.GetValue(); });
                        FillResponse(callContext, response);
                        guardedSgList.Close();

                        if (response.ByteSizeLong() > MaxRealProtoSize) {
                            // TODO: consider variable length proto size
                            // or switch from lwtrace to open telemetry like
                            // solution to avoid sending traces between nodes
                            response.MutableDeprecatedTrace()->Clear();
                            response.MutableHeaders()->ClearTrace();
                        }

                        ui32 flags = 0;
                        SetProtoFlag(
                            flags,
                            NCloud::NStorage::NRdma::
                                RDMA_PROTO_FLAG_DATA_AT_THE_END);

                        size_t responseBytes = 0;
                        try {
                            responseBytes = TProtoMessageSerializer::Serialize(
                                out,
                                TBlockStoreServerProtocol::
                                    EvWriteBlocksResponse,
                                flags,   // flags
                                response);
                        } catch (...) {
                            if (auto self = weakSelf.lock()) {
                                self->OnSerializeException(
                                    response,
                                    "WriteBlocks");
                            }
                            if (auto ep = endpoint.lock()) {
                                ep->SendError(
                                    context,
                                    E_REJECTED,
                                    "unable to serialize WriteBlocks response");
                            }
                            return;
                        }
                        if (auto ep = endpoint.lock()) {
                            ep->SendResponse(context, responseBytes);
                        }
                    });
            });

        return {};
    }

    NProto::TError HandleZeroBlocksRequest(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TCallContextPtr callContext,
        NProto::TZeroBlocksRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        auto req =
            std::make_shared<NProto::TZeroBlocksRequest>(std::move(*request));

        auto future = Service->ZeroBlocks(callContext, std::move(req));

        future.Subscribe(
            [out = out,
             context = context,
             endpoint = Endpoint,
             callContext = std::move(callContext),
             weakSelf = weak_from_this()](auto future)
            {
                auto response = ExtractResponse(future);
                FillResponse(callContext, response);

                if (response.ByteSizeLong() > MaxRealProtoSize) {
                    // TODO: consider variable length proto size
                    // or switch from lwtrace to open telemetry like
                    // solution to avoid sending traces between nodes
                    response.MutableDeprecatedTrace()->Clear();
                    response.MutableHeaders()->ClearTrace();
                }

                ui32 flags = 0;
                SetProtoFlag(
                    flags,
                    NCloud::NStorage::NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);

                size_t responseBytes = 0;
                try {
                    responseBytes = TProtoMessageSerializer::Serialize(
                        out,
                        TBlockStoreServerProtocol::EvZeroBlocksResponse,
                        flags,   // flags
                        response);
                } catch (...) {
                    if (auto self = weakSelf.lock()) {
                        self->OnSerializeException(
                            response,
                            "ZeroBlocks");
                    }
                    response = NProto::TZeroBlocksResponse{};
                    *response.MutableError() =
                        MakeError(
                            E_REJECTED,
                            "Unable to serialize ZeroBlocks response");
                    if (auto ep = endpoint.lock()) {
                        ep->SendError(
                            context,
                            E_REJECTED,
                            "unable to serialize ZeroBlocks response");
                    }
                    return;
                }
                if (auto ep = endpoint.lock()) {
                    ep->SendResponse(context, responseBytes);
                }
            });

        return {};
    }

    NProto::TError HandlePingRequest(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TCallContextPtr callContext,
        NProto::TPingRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        Y_UNUSED(callContext);
        Y_UNUSED(request);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        NProto::TPingResponse response;

        ui32 flags = 0;
        SetProtoFlag(
            flags,
            NCloud::NStorage::NRdma::RDMA_PROTO_FLAG_DATA_AT_THE_END);

        size_t responseBytes = TProtoMessageSerializer::Serialize(
            out,
            TBlockStoreServerProtocol::EvPingResponse,
            flags,   // flags
            response);

        if (auto ep = Endpoint.lock()) {
            ep->SendResponse(context, responseBytes);
        }

        return {};
    }

    NProto::TError HandleMountVolumeRequest(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TCallContextPtr callContext,
        NProto::TMountVolumeRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        request->SetForceRemoteBinding(true);

        // has to be collected before the request is moved out
        auto mountInfo = MakeMountInfo(*request);

        auto req =
            std::make_shared<NProto::TMountVolumeRequest>(std::move(*request));

        auto future = Service->MountVolume(callContext, std::move(req));

        future.Subscribe(
            [out = out,
             context = context,
             endpoint = Endpoint,
             mountInfo = std::move(mountInfo),
             callContext = std::move(callContext),
             weakSelf = weak_from_this()](auto future) mutable
            {
                auto response = ExtractResponse(future);

                if (!HasError(response.GetError())) {
                    if (auto self = weakSelf.lock()) {
                        self->OnVolumeMounted(context, std::move(mountInfo));
                    }
                }

                ui32 flags = 0;
                size_t responseBytes = 0;
                try {
                    responseBytes = TProtoMessageSerializer::Serialize(
                        out,
                        TBlockStoreServerProtocol::EvMountVolumeResponse,
                        flags,   // flags
                        response);
                } catch (...) {
                    if (auto self = weakSelf.lock()) {
                        self->OnSerializeException(
                            response,
                            "MountVolume");
                    }
                    response = NProto::TMountVolumeResponse{};
                    *response.MutableError() =
                        MakeError(
                            E_REJECTED,
                            "Unable to serialize MountVolume response");
                    if (auto ep = endpoint.lock()) {
                        ep->SendError(
                            context,
                            E_REJECTED,
                            "unable to serialize MountVolume response");
                    }
                    return;
                }

                if (auto ep = endpoint.lock()) {
                    ep->SendResponse(context, responseBytes);
                }
            });

        return {};
    }

    NProto::TError HandleUnmountVolumeRequest(
        NCloud::NStorage::NRdma::IServerRequest* context,
        TCallContextPtr callContext,
        NProto::TUnmountVolumeRequest* request,
        TStringBuf requestData,
        TStringBuf out) const
    {
        if (TraceSerializer->HandleTraceRequest(
                request->GetHeaders().GetInternal().GetTrace(),
                callContext->LWOrbit))
        {
            request->MutableHeaders()->MutableInternal()->SetTraceTs(
                GetCycleCount());
        }

        LWTRACK(RequestReceived_RdmaTarget, callContext->LWOrbit);

        Y_ENSURE_RETURN(requestData.length() == 0, "invalid request");

        // have to be collected before the request is moved out
        auto diskId = request->GetDiskId();
        auto clientId = request->GetHeaders().GetClientId();

        auto req = std::make_shared<NProto::TUnmountVolumeRequest>(
            std::move(*request));

        auto future = Service->UnmountVolume(callContext, std::move(req));

        future.Subscribe(
            [out = out,
             context = context,
             endpoint = Endpoint,
             diskId = std::move(diskId),
             clientId = std::move(clientId),
             callContext = std::move(callContext),
             weakSelf = weak_from_this()](auto future)
            {
                auto response = ExtractResponse(future);

                if (!HasError(response.GetError())) {
                    if (auto self = weakSelf.lock()) {
                        self->OnVolumeUnmounted(context, diskId, clientId);
                    }
                }

                ui32 flags = 0;
                size_t responseBytes = 0;
                try {
                    responseBytes = TProtoMessageSerializer::Serialize(
                        out,
                        TBlockStoreServerProtocol::EvUnmountVolumeResponse,
                        flags,   // flags
                        response);
                } catch (...) {
                    if (auto self = weakSelf.lock()) {
                        self->OnSerializeException(
                            response,
                            "UnmountVolume");
                    }
                    response = NProto::TUnmountVolumeResponse{};
                    *response.MutableError() =
                        MakeError(
                            E_REJECTED,
                            "Unable to serialize UnmountVolume response");
                    if (auto ep = endpoint.lock()) {
                        ep->SendError(
                            context,
                            E_REJECTED,
                            "unable to serialize UnmountVolume response");
                    }
                    return;
                }

                if (auto ep = endpoint.lock()) {
                    ep->SendResponse(context, responseBytes);
                }
            });

        return {};
    }
};

///////////////////////////////////////////////////////////////////////////////

class TRdmaTarget final: public IStartable
{
    // number of mount related columns, see DumpHtml()
    static constexpr size_t MountColumnCount = 5;

    const TBlockstoreServerRdmaTargetConfigPtr Config;

    ILoggingServicePtr Logging;
    ITraceSerializerPtr TraceSerializer;
    NCloud::NStorage::NRdma::IServerPtr Server;
    ITaskQueuePtr TaskQueue;

    // not set when nobody is going to look at the connections
    TMountRegistryPtr MountRegistry;
    std::shared_ptr<TRequestHandler> Handler;

    TLog Log;

public:
    TRdmaTarget(
        TBlockstoreServerRdmaTargetConfigPtr rdmaTargetConfig,
        ILoggingServicePtr logging,
        ITraceSerializerPtr traceSerializer,
        NCloud::NStorage::NRdma::IServerPtr server,
        ITaskQueuePtr taskQueue,
        TMountRegistryPtr mountRegistry,
        IBlockStorePtr service)
        : Config(std::move(rdmaTargetConfig))
        , Logging(std::move(logging))
        , TraceSerializer(std::move(traceSerializer))
        , Server(std::move(server))
        , TaskQueue(std::move(taskQueue))
        , MountRegistry(std::move(mountRegistry))
    {
        Handler = std::make_shared<TRequestHandler>(
            std::move(service),
            TraceSerializer,
            TaskQueue,
            MountRegistry);
    }

    void Start() override
    {
        if (MountRegistry) {
            MountRegistry->Start();
        }

        auto endpoint =
            Server->StartEndpoint(Config->Host, Config->Port, Handler);

        Log = Logging->CreateLog("BLOCKSTORE_SERVER");
        if (endpoint == nullptr) {
            STORAGE_ERROR("unable to set up RDMA endpoint");
            return;
        }

        Handler->Init(endpoint, std::move(Log));
    }

    void Stop() override
    {
        Server->Stop();
        TaskQueue->Stop();

        if (MountRegistry) {
            MountRegistry->Stop();
        }
    }

    // Renders the client connections along with the volumes mounted over them.
    void DumpHtml(IOutputStream& out) const
    {
        if (!MountRegistry) {
            out << "Connection tracking is disabled";
            return;
        }

        auto connections = MountRegistry->GetConnections();

        HTML(out) {
            TAG(TH3) {
                out << "Connections"
                    << " <font color=gray>" << connections.size() << "</font>";
            }

            TABLE_SORTABLE_CLASS("table table-bordered") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { out << "SessionId"; }
                        TABLEH() { out << "Peer"; }
                        TABLEH() { out << "Connected"; }
                        TABLEH() { out << "DiskId"; }
                        TABLEH() { out << "ClientId"; }
                        TABLEH() { out << "AccessMode"; }
                        TABLEH() { out << "MountMode"; }
                        TABLEH() { out << "MountSeqNumber"; }
                    }
                }

                for (const auto& connection: connections) {
                    if (connection.Mounts.empty()) {
                        RenderConnection(out, connection, nullptr);
                        continue;
                    }

                    for (const auto& mount: connection.Mounts) {
                        RenderConnection(out, connection, &mount);
                    }
                }
            }
        }
    }

private:
    static void RenderConnection(
        IOutputStream& out,
        const TConnectionInfo& connection,
        const TMountInfo* mount)
    {
        HTML(out) {
            TABLER() {
                TABLED() { out << Hex(connection.SessionId, HF_FULL); }
                TABLED() { out << connection.Peer; }
                TABLED() { out << connection.StartTs; }

                if (mount) {
                    TABLED() { out << mount->DiskId; }
                    TABLED() { out << mount->ClientId; }
                    TABLED() {
                        out << NProto::EVolumeAccessMode_Name(
                            mount->VolumeAccessMode);
                    }
                    TABLED() {
                        out << NProto::EVolumeMountMode_Name(
                            mount->VolumeMountMode);
                    }
                    TABLED() { out << mount->MountSeqNumber; }
                } else {
                    // a connection that hasn't mounted anything (yet)
                    for (size_t i = 0; i < MountColumnCount; ++i) {
                        TABLED() { out << "-"; }
                    }
                }
            }
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRdmaTargetMonPage final: public THtmlMonPage
{
    const std::shared_ptr<TRdmaTarget> Target;

public:
    explicit TRdmaTargetMonPage(std::shared_ptr<TRdmaTarget> target)
        : THtmlMonPage("RdmaTarget", "RdmaTarget", true)
        , Target(std::move(target))
    {}

    void OutputContent(IMonHttpRequest& request) override
    {
        Target->DumpHtml(request.Output());
    }
};

}   // namespace

IStartablePtr CreateBlockstoreServerRdmaTarget(
    TBlockstoreServerRdmaTargetConfigPtr rdmaTargetConfig,
    ILoggingServicePtr logging,
    ITraceSerializerPtr traceSerializer,
    IMonitoringServicePtr monitoring,
    NCloud::NStorage::NRdma::IServerPtr server,
    IBlockStorePtr service)
{
    auto threadPool = CreateThreadPool("RDMA", rdmaTargetConfig->WorkerThreads);
    threadPool->Start();

    // without a page there is nobody to read the connections, so they are not
    // tracked at all and the handler is left with an empty registry pointer
    auto mountRegistry =
        monitoring && rdmaTargetConfig->ConnectionMonitoringEnabled
            ? CreateMountRegistry(logging)
            : nullptr;

    auto target = std::make_shared<TRdmaTarget>(
        std::move(rdmaTargetConfig),
        std::move(logging),
        std::move(traceSerializer),
        std::move(server),
        std::move(threadPool),
        mountRegistry,
        std::move(service));

    if (mountRegistry) {
        auto rootPage = monitoring->RegisterIndexPage("blockstore", "BlockStore");
        static_cast<TIndexMonPage&>(*rootPage).Register(
            new TRdmaTargetMonPage(target));
    }

    return target;
}

}   // namespace NCloud::NBlockStore::NStorage
