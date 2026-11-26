#include "rdma_server_handler.h"

#include "rdma_private.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/storage/api/components.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>

#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/diagnostics/logging.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/event.h>

#include <util/string/builder.h>

#include <atomic>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

// The sender of the request is the forwarder, which serves every vhost of a
// node and must not become the client of the pipe: the filesystem events go
// to the client that has registered the pipe.
template <typename T>
void DisableFileSystemEventClientRegistration(T& record)
{
    if constexpr (requires { record.MutableHeaders(); }) {
        record.MutableHeaders()
            ->SetFileSystemEventClientRegistrationDisabled(true);
    }
}

////////////////////////////////////////////////////////////////////////////////

class TActorSystemRdmaRequestHandler final
    : public IRdmaTabletRequestHandler
{
private:
    const IActorSystemPtr ActorSystem;
    const TActorId ForwarderId;
    const TRdmaTabletRegistryPtr Registry;
    TLog Log;

    std::atomic<ui64> LastCookie = 0;

public:
    TActorSystemRdmaRequestHandler(
            IActorSystemPtr actorSystem,
            TActorId forwarderId,
            TRdmaTabletRegistryPtr registry)
        : ActorSystem(std::move(actorSystem))
        , ForwarderId(forwarderId)
        , Registry(std::move(registry))
        , Log(ActorSystem->CreateLog(
              GetComponentName(TFileStoreComponents::RDMA)))
    {}

    TFuture<TTabletTransportResponse> HandleRequest(
        const NProtoPrivate::TTabletRdmaRequest& envelope,
        TStringBuf requestBody,
        TCallContextBasePtr callContext) override
    {
        Y_UNUSED(callContext);

        const ui32 methodId = envelope.GetMethodId();
        const TActorId pipeServerId(
            envelope.GetPipeServerId().GetRawX1(),
            envelope.GetPipeServerId().GetRawX2());

        if (!pipeServerId) {
            return MakeFuture(
                Reject(E_ARGUMENT, "RDMA request without pipe server"));
        }

        if (!IsRdmaTabletMethod(methodId)) {
            return MakeFuture(Reject(
                E_NOT_IMPLEMENTED,
                TStringBuilder()
                    << "method is not supported over RDMA: " << methodId));
        }

        const TActorId tabletActorId = Registry->FindTablet(pipeServerId);
        if (!tabletActorId) {
            return MakeFuture(Reject(
                E_REJECTED,
                TStringBuilder()
                    << "unknown pipe server: " << pipeServerId.ToString()));
        }

        auto promise = NewPromise<TTabletTransportResponse>();

        switch (methodId) {

#define FILESTORE_RDMA_FORWARD_CASE(name, ns)                                  \
    case ns::TEv##name##Request::EventType: {                                  \
        using TMethod = ns::T##name##Method;                                   \
        auto request = std::make_unique<typename TMethod::TRequest>();         \
        if (!request->Record.ParseFromArray(                                   \
                requestBody.data(),                                            \
                requestBody.size()))                                           \
        {                                                                      \
            const TString message = TStringBuilder()                           \
                << "malformed RDMA request body for method " << methodId;      \
            ReportTabletRdmaCritEvent(message);                                \
            promise.SetValue(Reject(E_ARGUMENT, message));                     \
            break;                                                             \
        }                                                                      \
        DisableFileSystemEventClientRegistration(request->Record);             \
        Send(std::move(request), tabletActorId, pipeServerId, promise);        \
        break;                                                                 \
    }                                                                          \
// FILESTORE_RDMA_FORWARD_CASE

            FILESTORE_RDMA_REQUESTS(FILESTORE_RDMA_FORWARD_CASE)

#undef FILESTORE_RDMA_FORWARD_CASE

            default: {
                const TString message = TStringBuilder()
                    << "unexpected RDMA method " << methodId;
                ReportTabletRdmaCritEvent(message);
                promise.SetValue(Reject(E_NOT_IMPLEMENTED, message));
                break;
            }
        }

        return promise.GetFuture();
    }

private:
    TTabletTransportResponse Reject(ui32 code, TString message)
    {
        STORAGE_WARN("Reject RDMA request: " << message);
        return TTabletTransportResponse(MakeError(code, std::move(message)));
    }

    void Send(
        std::unique_ptr<IEventBase> request,
        const TActorId& tabletActorId,
        const TActorId& pipeServerId,
        TPromise<TTabletTransportResponse> promise)
    {
        const ui64 cookie = ++LastCookie;

        // The forwarder must know the cookie before the tablet can answer.
        ActorSystem->Send(std::make_unique<IEventHandle>(
            ForwarderId,
            TActorId(),
            new TEvRdmaTabletPrivate::TEvTrackRequest(
                tabletActorId,
                std::move(promise)),
            0,   // flags
            cookie));

        // Addressed to the client's pipe server and rewritten to the tablet,
        // the request reaches the tablet as one received through that pipe.
        auto ev = std::make_unique<IEventHandle>(
            pipeServerId,
            ForwarderId,
            request.release(),
            IEventHandle::FlagTrackDelivery,
            cookie);
        ev->Rewrite(ev->GetTypeRewrite(), tabletActorId);
        ActorSystem->Send(std::move(ev));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsRdmaTabletMethod(ui32 methodId)
{
    switch (methodId) {

#define FILESTORE_RDMA_METHOD_CASE(name, ns)                                   \
    case ns::TEv##name##Request::EventType:                                    \
// FILESTORE_RDMA_METHOD_CASE

        FILESTORE_RDMA_REQUESTS(FILESTORE_RDMA_METHOD_CASE)

#undef FILESTORE_RDMA_METHOD_CASE

            return true;

        default:
            return false;
    }
}

IRdmaTabletRequestHandlerPtr CreateActorSystemRdmaRequestHandler(
    IActorSystemPtr actorSystem,
    TActorId forwarderId,
    TRdmaTabletRegistryPtr registry)
{
    return std::make_shared<TActorSystemRdmaRequestHandler>(
        std::move(actorSystem),
        forwarderId,
        std::move(registry));
}

}   // namespace NCloud::NFileStore::NStorage
