#include "rdma_tablet_forwarder.h"

#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/helpers.h>

#include <contrib/ydb/core/tablet/tablet_pipe_client_cache.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/message.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<NTabletPipe::IClientCache> CreateForwarderPipeClientCache()
{
    NTabletPipe::TClientConfig clientConfig;
    clientConfig.RetryPolicy = {
        .RetryLimitCount = 4,
        .MinRetryTime = TDuration::Seconds(1),
        .MaxRetryTime = TDuration::Seconds(4),
    };

    return std::unique_ptr<NTabletPipe::IClientCache>(
        NTabletPipe::CreateUnboundedClientCache(clientConfig));
}

////////////////////////////////////////////////////////////////////////////////

class TRdmaTabletForwarderActor final
    : public TActorBootstrapped<TRdmaTabletForwarderActor>
{
    struct TInFlight
    {
        TPromise<TTabletTransportResponse> Promise;
        ui64 TabletId;
        TInstant Deadline;
    };

private:
    const TDuration RequestTimeout;
    const TRdmaForwarderCountersPtr Counters;

    std::unique_ptr<NTabletPipe::IClientCache> ClientCache;
    ui64 Cookie = 0;
    THashMap<ui64, TInFlight> InFlight;
    bool SweepScheduled = false;

public:
    TRdmaTabletForwarderActor(
            TDuration requestTimeout,
            TRdmaForwarderCountersPtr counters)
        : RequestTimeout(requestTimeout)
        , Counters(std::move(counters))
        , ClientCache(CreateForwarderPipeClientCache())
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        Become(&TThis::StateWork);
    }

private:
    void Complete(ui64 cookie, TTabletTransportResponse response)
    {
        auto it = InFlight.find(cookie);
        if (it == InFlight.end()) {
            // late response: the request timed out or its pipe failed
            return;
        }

        if (HasError(response.Error)) {
            Counters->ErrorReplies->Inc();
        } else {
            Counters->OkReplies->Inc();
        }

        it->second.Promise.SetValue(std::move(response));
        InFlight.erase(it);
        Counters->InFlightRequests->Dec();
    }

    void FailTabletRequests(ui64 tabletId, const NProto::TError& error)
    {
        for (auto it = InFlight.begin(); it != InFlight.end();) {
            if (it->second.TabletId == tabletId) {
                it->second.Promise.SetValue(TTabletTransportResponse(error));
                InFlight.erase(it++);
                Counters->PipeFailures->Inc();
                Counters->InFlightRequests->Dec();
            } else {
                ++it;
            }
        }
    }

    void HandleForwardRequest(
        const TEvRdmaTabletForwarder::TEvForwardRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        const ui64 cookie = ++Cookie;

        auto event = std::make_unique<IEventHandle>(
            MakeIndexTabletProxyServiceId(),
            ctx.SelfID,
            msg->Request.release(),
            0,        // flags
            cookie);

        auto clientId = ClientCache->Prepare(ctx, msg->TabletId);
        LOG_TRACE(ctx, TFileStoreComponents::RDMA,
            "Forward RDMA request %lu to tablet %lu (pipe: %s)",
            cookie,
            msg->TabletId,
            ToString(clientId).c_str());
        NCloud::PipeSend(ctx, clientId, std::move(event));

        InFlight.emplace(
            cookie,
            TInFlight{
                std::move(msg->Promise),
                msg->TabletId,
                RequestTimeout ? ctx.Now() + RequestTimeout : TInstant::Max()});
        Counters->Requests->Inc();
        Counters->InFlightRequests->Inc();

        ScheduleSweep(ctx);
    }

    void ScheduleSweep(const TActorContext& ctx)
    {
        if (!RequestTimeout || SweepScheduled || InFlight.empty()) {
            return;
        }
        SweepScheduled = true;
        ctx.Schedule(
            Max(RequestTimeout / 4, TDuration::Seconds(1)),
            new TEvents::TEvWakeup());
    }

    void HandleWakeup(
        const TEvents::TEvWakeup::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        SweepScheduled = false;

        const auto now = ctx.Now();
        for (auto it = InFlight.begin(); it != InFlight.end();) {
            if (it->second.Deadline <= now) {
                it->second.Promise.SetValue(TTabletTransportResponse(
                    MakeError(E_TIMEOUT, "RDMA tablet request timed out")));
                InFlight.erase(it++);
                Counters->Timeouts->Inc();
                Counters->InFlightRequests->Dec();
            } else {
                ++it;
            }
        }

        ScheduleSweep(ctx);
    }

    void HandleClientConnected(
        TEvTabletPipe::TEvClientConnected::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        if (!ClientCache->OnConnect(ev)) {
            LOG_WARN(ctx, TFileStoreComponents::RDMA,
                "RDMA forwarder cannot connect to tablet %lu: %s",
                msg->TabletId,
                ToString(msg->Status).c_str());

            FailTabletRequests(
                msg->TabletId,
                MakeError(E_REJECTED, "rdma forwarder pipe connect failed"));
        }
    }

    void HandleClientDestroyed(
        TEvTabletPipe::TEvClientDestroyed::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto* msg = ev->Get();
        ClientCache->OnDisconnect(ev);

        LOG_WARN(ctx, TFileStoreComponents::RDMA,
            "RDMA forwarder pipe to tablet %lu destroyed",
            msg->TabletId);

        FailTabletRequests(
            msg->TabletId,
            MakeError(E_REJECTED, "rdma forwarder pipe disconnected"));
    }

    void HandleFileSystemEvent(
        const TEvIndexTabletProxy::TEvFileSystemEvent::TPtr& ev,
        const TActorContext& ctx)
    {
        LOG_DEBUG(ctx, TFileStoreComponents::RDMA,
            "Drop FileSystemEvent from %s",
            ev->Sender.ToString().c_str());
        Counters->DroppedFileSystemEvents->Inc();
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        for (auto& [cookie, inflight]: InFlight) {
            Y_UNUSED(cookie);
            inflight.Promise.SetValue(TTabletTransportResponse(
                MakeError(E_REJECTED, "rdma forwarder stopped")));
        }
        Counters->InFlightRequests->Sub(InFlight.size());
        InFlight.clear();
        Die(ctx);
    }

    bool HandleResponses(STFUNC_SIG)
    {
#define FILESTORE_HANDLE_RDMA_RESPONSE(name, ns)                               \
    case ns::TEv##name##Response::EventType: {                                 \
        auto* x = reinterpret_cast<ns::TEv##name##Response::TPtr*>(&ev);       \
        using TRecord = std::decay_t<decltype((*x)->Get()->Record)>;           \
        if ((*x)->Get()->GetPayloadCount()) {                                  \
            Complete(                                                          \
                (*x)->Cookie,                                                  \
                TTabletTransportResponse(MakeError(                            \
                    E_REJECTED,                                                \
                    "response payload is not supported over RDMA")));          \
            break;                                                             \
        }                                                                      \
        Complete(                                                              \
            (*x)->Cookie,                                                      \
            TTabletTransportResponse(                                          \
                ns::TEv##name##Response::EventType,                            \
                std::make_shared<TRecord>(std::move((*x)->Get()->Record))));   \
        break;                                                                 \
    }                                                                          \
// FILESTORE_HANDLE_RDMA_RESPONSE

        switch (ev->GetTypeRewrite()) {
            FILESTORE_SERVICE_REQUESTS(
                FILESTORE_HANDLE_RDMA_RESPONSE,
                TEvService)
            FILESTORE_TABLET_REQUESTS(
                FILESTORE_HANDLE_RDMA_RESPONSE,
                TEvIndexTablet)

            default:
                return false;
        }

        return true;

#undef FILESTORE_HANDLE_RDMA_RESPONSE
    }

    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, HandlePoisonPill);

            HFunc(
                TEvRdmaTabletForwarder::TEvForwardRequest,
                HandleForwardRequest);
            HFunc(TEvents::TEvWakeup, HandleWakeup);

            HFunc(TEvTabletPipe::TEvClientConnected, HandleClientConnected);
            HFunc(TEvTabletPipe::TEvClientDestroyed, HandleClientDestroyed);

            HFunc(
                TEvIndexTabletProxy::TEvFileSystemEvent,
                HandleFileSystemEvent);

            default:
                if (!HandleResponses(ev)) {
                    HandleUnexpectedEvent(
                        ev,
                        TFileStoreComponents::RDMA,
                        __PRETTY_FUNCTION__);
                }
                break;
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<IActor> CreateRdmaTabletForwarderActor(
    TDuration requestTimeout,
    TRdmaForwarderCountersPtr counters)
{
    return std::make_unique<TRdmaTabletForwarderActor>(
        requestTimeout,
        std::move(counters));
}

}   // namespace NCloud::NFileStore::NStorage
