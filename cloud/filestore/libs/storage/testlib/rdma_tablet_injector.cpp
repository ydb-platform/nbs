#include "rdma_tablet_injector.h"

#include <cloud/filestore/libs/rdma/protocol.h>
#include <cloud/filestore/libs/rdma/server.h>
#include <cloud/filestore/libs/rdma_test/test_server.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/tablet_rdma/rdma_server_handler.h>
#include <cloud/filestore/libs/storage/tablet_rdma/rdma_tablet_forwarder.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/rdma/iface/protobuf.h>

#include <google/protobuf/message.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NCloud::NStorage::NRdma;

namespace {

////////////////////////////////////////////////////////////////////////////////

// Untouched pages of the response buffers are never committed.
constexpr size_t MaxResponseBytes = 64_MB;

std::unique_ptr<IEventBase> MakeResponse(ui32 methodId, TStringBuf body)
{
    switch (methodId) {

#define FILESTORE_RDMA_RESPONSE_CASE(name, ns)                                 \
    case ns::TEv##name##Response::EventType: {                                 \
        auto response = std::make_unique<ns::TEv##name##Response>();           \
        if (!response->Record.ParseFromArray(body.data(), body.size())) {      \
            return std::make_unique<ns::TEv##name##Response>(                  \
                MakeError(E_FAIL, "failed to parse RDMA response"));           \
        }                                                                      \
        return response;                                                       \
    }                                                                          \
// FILESTORE_RDMA_RESPONSE_CASE

        FILESTORE_SERVICE_REQUESTS(FILESTORE_RDMA_RESPONSE_CASE, TEvService)
        FILESTORE_TABLET_REQUESTS(FILESTORE_RDMA_RESPONSE_CASE, TEvIndexTablet)

#undef FILESTORE_RDMA_RESPONSE_CASE

        default:
            Y_ABORT("unexpected RDMA response method %u", methodId);
    }
}

std::unique_ptr<IEventBase> MakeErrorResponse(
    ui32 requestMethodId,
    NProto::TError error)
{
    switch (requestMethodId) {

#define FILESTORE_RDMA_ERROR_CASE(name, ns)                                    \
    case ns::TEv##name##Request::EventType:                                    \
        return std::make_unique<ns::TEv##name##Response>(std::move(error));    \
// FILESTORE_RDMA_ERROR_CASE

        FILESTORE_SERVICE_REQUESTS(FILESTORE_RDMA_ERROR_CASE, TEvService)
        FILESTORE_TABLET_REQUESTS(FILESTORE_RDMA_ERROR_CASE, TEvIndexTablet)

#undef FILESTORE_RDMA_ERROR_CASE

        default:
            Y_ABORT("unexpected RDMA request method %u", requestMethodId);
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

// Owns the requests in flight; outlives the injector until the RDMA target
// completes them, since the target writes responses into their buffers.
struct TRdmaTabletInjector::TState
{
    struct TRequest
    {
        TString In;
        TString Out;
        ui32 MethodId = 0;
        TActorId Sender;
        ui64 Cookie = 0;
    };

    NKikimr::TTestActorRuntime* const Runtime;
    const ui32 NodeIdx;
    TActorId ForwarderId;
    THashMap<void*, std::unique_ptr<TRequest>> Requests;
    bool Detached = false;

    TState(NKikimr::TTestActorRuntime* runtime, ui32 nodeIdx)
        : Runtime(runtime)
        , NodeIdx(nodeIdx)
    {}

    void OnResponse(void* context, size_t responseBytes)
    {
        auto request = Extract(context);
        if (Detached) {
            return;
        }

        auto resultOrError = TTabletRdmaProtocol::Serializer()->Parse(
            TStringBuf(request->Out).Head(responseBytes));
        Y_ABORT_UNLESS(
            !HasError(resultOrError),
            "%s",
            FormatError(resultOrError.GetError()).c_str());

        auto result = resultOrError.ExtractResult();
        Y_ABORT_UNLESS(result.MsgId == TTabletRdmaProtocol::TabletResponse);

        const auto* envelope =
            static_cast<NProtoPrivate::TTabletRdmaResponse*>(
                result.Proto.get());
        Deliver(*request, MakeResponse(envelope->GetMethodId(), result.Data));
    }

    void OnError(void* context, ui32 error, TStringBuf message)
    {
        auto request = Extract(context);
        if (Detached) {
            return;
        }

        Deliver(
            *request,
            MakeErrorResponse(
                request->MethodId,
                MakeError(error, TString(message))));
    }

private:
    std::unique_ptr<TRequest> Extract(void* context)
    {
        auto it = Requests.find(context);
        Y_ABORT_UNLESS(it != Requests.end());
        auto request = std::move(it->second);
        Requests.erase(it);
        return request;
    }

    void Deliver(const TRequest& request, std::unique_ptr<IEventBase> response)
    {
        Runtime->Send(
            new IEventHandle(
                request.Sender,
                ForwarderId,
                response.release(),
                0,   // flags
                request.Cookie),
            NodeIdx);
    }
};

////////////////////////////////////////////////////////////////////////////////

TRdmaTabletInjector::TRdmaTabletInjector(
        NKikimr::TTestActorRuntime& runtime,
        ui32 nodeIdx)
    : Runtime(runtime)
    , NodeIdx(nodeIdx)
    , State(std::make_shared<TState>(&runtime, nodeIdx))
{
    auto counters = std::make_shared<TRdmaForwarderCounters>(
        MakeIntrusive<NMonitoring::TDynamicCounters>());

    ForwarderId = Runtime.Register(
        CreateRdmaTabletForwarderActor(TDuration::Zero(), counters).release(),
        NodeIdx);
    Runtime.EnableScheduleForActor(ForwarderId);
    State->ForwarderId = ForwarderId;

    Server = std::make_shared<TTestRdmaServer>(
        [state = State](void* context, size_t responseBytes)
        {
            state->OnResponse(context, responseBytes);
        },
        [state = State](void* context, ui32 error, TStringBuf message)
        {
            state->OnError(context, error, message);
        });

    Target = CreateFileStoreRdmaServiceTarget(
        std::make_shared<TFileStoreRdmaServerConfig>(),
        CreateLoggingService("console"),
        Server,
        CreateActorSystemRdmaRequestHandler(
            Runtime.GetActorSystem(NodeIdx),
            ForwarderId,
            std::move(counters)));
    Target->Start();
}

TRdmaTabletInjector::~TRdmaTabletInjector()
{
    State->Detached = true;
}

void TRdmaTabletInjector::Send(
    ui64 tabletId,
    ui32 methodId,
    const google::protobuf::Message& record,
    TActorId sender,
    ui64 cookie)
{
    NProtoPrivate::TTabletRdmaRequest envelope;
    envelope.SetTabletId(tabletId);
    envelope.SetMethodId(methodId);

    auto request = std::make_unique<TState::TRequest>();
    request->MethodId = methodId;
    request->Sender = sender;
    request->Cookie = cookie;

    const size_t bodyBytes = record.ByteSizeLong();
    request->In = TString::Uninitialized(
        TProtoMessageSerializer::MessageByteSize(envelope, bodyBytes));
    const size_t offset = TProtoMessageSerializer::SerializeWithDataLength(
        request->In,
        TTabletRdmaProtocol::TabletRequest,
        0,   // flags
        envelope,
        bodyBytes);
    Y_ABORT_UNLESS(
        record.SerializeToArray(request->In.begin() + offset, bodyBytes));

    request->Out = TString::Uninitialized(MaxResponseBytes);

    void* context = request.get();
    TStringBuf in = request->In;
    TStringBuf out = request->Out;
    State->Requests.emplace(context, std::move(request));

    auto handler = Server->GetHandler();
    handler->HandleRequest(context, handler->CreateCallContext(), in, out);
}

}   // namespace NCloud::NFileStore::NStorage
