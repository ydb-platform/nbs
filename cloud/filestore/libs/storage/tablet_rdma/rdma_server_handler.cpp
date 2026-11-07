#include "rdma_server_handler.h"

#include "rdma_tablet_forwarder.h"

#include <cloud/filestore/libs/rdma/transport.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/string/builder.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TActorSystemRdmaRequestHandler final
    : public IRdmaTabletRequestHandler
{
private:
    TActorSystem* const ActorSystem;
    const TActorId ForwarderId;
    const TRdmaForwarderCountersPtr Counters;

public:
    TActorSystemRdmaRequestHandler(
            TActorSystem* actorSystem,
            TActorId forwarderId,
            TRdmaForwarderCountersPtr counters)
        : ActorSystem(actorSystem)
        , ForwarderId(forwarderId)
        , Counters(std::move(counters))
    {}

    TFuture<TTabletTransportResponse> HandleRequest(
        ui64 tabletId,
        ui32 methodId,
        TStringBuf requestBody,
        TCallContextBasePtr callContext) override
    {
        Y_UNUSED(callContext);

        if (!tabletId) {
            Counters->InvalidRequests->Inc();
            return MakeFuture(TTabletTransportResponse(
                MakeError(E_ARGUMENT, "RDMA request without tablet id")));
        }

        // Malformed bodies and unsupported methods are retryable: corruption
        // in transit or version skew during deployment.
        if (!IsRdmaTabletMethod(methodId)) {
            Counters->UnsupportedMethods->Inc();
            return MakeFuture(TTabletTransportResponse(MakeError(
                E_REJECTED,
                TStringBuilder()
                    << "method is not supported over RDMA: " << methodId)));
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
            Counters->ParseErrors->Inc();                                      \
            promise.SetValue(TTabletTransportResponse(                         \
                MakeError(E_REJECTED, "malformed RDMA request body")));        \
            break;                                                             \
        }                                                                      \
        ActorSystem->Send(                                                     \
            ForwarderId,                                                       \
            new TEvRdmaTabletForwarder::TEvForwardRequest(                     \
                tabletId,                                                      \
                std::move(request),                                            \
                promise));                                                     \
        break;                                                                 \
    }                                                                          \
// FILESTORE_RDMA_FORWARD_CASE

            FILESTORE_SERVICE_REQUESTS(FILESTORE_RDMA_FORWARD_CASE, TEvService)
            FILESTORE_TABLET_REQUESTS(
                FILESTORE_RDMA_FORWARD_CASE,
                TEvIndexTablet)

#undef FILESTORE_RDMA_FORWARD_CASE

            default:
                Y_ABORT("unexpected RDMA method %u", methodId);
        }

        return promise.GetFuture();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

bool IsRdmaTabletMethod(ui32 methodId)
{
    // The tablet makes the CreateSession sender the session owner and kills it
    // on session takeover, while the forwarder serves every client of a node.
    if (methodId == TEvIndexTablet::EvCreateSessionRequest) {
        return false;
    }

    switch (methodId) {

#define FILESTORE_RDMA_METHOD_CASE(name, ns)                                   \
    case ns::TEv##name##Request::EventType:                                    \
// FILESTORE_RDMA_METHOD_CASE

        FILESTORE_SERVICE_REQUESTS(FILESTORE_RDMA_METHOD_CASE, TEvService)
        FILESTORE_TABLET_REQUESTS(FILESTORE_RDMA_METHOD_CASE, TEvIndexTablet)

#undef FILESTORE_RDMA_METHOD_CASE

            return true;

        default:
            return false;
    }
}

IRdmaTabletRequestHandlerPtr CreateActorSystemRdmaRequestHandler(
    TActorSystem* actorSystem,
    TActorId forwarderId,
    TRdmaForwarderCountersPtr counters)
{
    return std::make_shared<TActorSystemRdmaRequestHandler>(
        actorSystem,
        forwarderId,
        std::move(counters));
}

}   // namespace NCloud::NFileStore::NStorage
