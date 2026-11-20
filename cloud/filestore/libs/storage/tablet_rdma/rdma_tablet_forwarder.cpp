#include "rdma_tablet_forwarder.h"

#include "rdma_private.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/api/service.h>
#include <cloud/filestore/libs/storage/api/tablet.h>
#include <cloud/filestore/libs/storage/api/tablet_proxy.h>

#include <cloud/storage/core/libs/actors/helpers.h>
#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actor_bootstrapped.h>
#include <contrib/ydb/library/actors/core/hfunc.h>
#include <contrib/ydb/library/actors/core/log.h>

#include <google/protobuf/message.h>

#include <util/generic/hash.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 TimeoutSweepsPerRequestTimeout = 4;

////////////////////////////////////////////////////////////////////////////////

class TRdmaTabletForwarderActor final
    : public TActorBootstrapped<TRdmaTabletForwarderActor>
{
    struct TInFlight
    {
        TPromise<TTabletTransportResponse> Promise;
        TActorId TabletActorId;
        TInstant Deadline;
    };

private:
    const TDuration RequestTimeout;

    THashMap<ui64, TInFlight> InFlight;
    bool SweepScheduled = false;

public:
    explicit TRdmaTabletForwarderActor(TDuration requestTimeout)
        : RequestTimeout(requestTimeout)
    {}

    void Bootstrap(const TActorContext& ctx)
    {
        Y_UNUSED(ctx);
        Become(&TThis::StateWork);
    }

private:
    void Complete(
        const TActorContext& ctx,
        ui64 cookie,
        TTabletTransportResponse response)
    {
        auto it = InFlight.find(cookie);
        if (it == InFlight.end()) {
            // late response: the request timed out or its tablet is gone
            return;
        }

        if (HasError(response.Error)) {
            Fail(ctx, it->first, it->second, std::move(response.Error));
        } else {
            it->second.Promise.SetValue(std::move(response));
        }
        InFlight.erase(it);
    }

    void Fail(
        const TActorContext& ctx,
        ui64 cookie,
        TInFlight& inflight,
        NProto::TError error)
    {
        LOG_WARN(ctx, TFileStoreComponents::RDMA,
            "RDMA request %lu to tablet %s failed: %s",
            cookie,
            ToString(inflight.TabletActorId).c_str(),
            FormatError(error).c_str());

        inflight.Promise.SetValue(TTabletTransportResponse(std::move(error)));
    }

    void HandleTrackRequest(
        const TEvRdmaTabletPrivate::TEvTrackRequest::TPtr& ev,
        const TActorContext& ctx)
    {
        auto* msg = ev->Get();

        LOG_TRACE(ctx, TFileStoreComponents::RDMA,
            "Track RDMA request %lu to tablet %s",
            ev->Cookie,
            ToString(msg->TabletActorId).c_str());

        if (InFlight.contains(ev->Cookie)) {
            ReportRdmaDuplicateRequestCookie(
                TStringBuilder()
                << "cookie " << ev->Cookie << ", tablet "
                << ToString(msg->TabletActorId));
            msg->Promise.SetValue(TTabletTransportResponse(
                MakeError(E_REJECTED, "duplicate RDMA request cookie")));
            return;
        }

        InFlight.emplace(
            ev->Cookie,
            TInFlight{
                std::move(msg->Promise),
                msg->TabletActorId,
                RequestTimeout ? ctx.Now() + RequestTimeout : TInstant::Max()});

        ScheduleSweep(ctx);
    }

    void HandleUndelivered(
        const TEvents::TEvUndelivered::TPtr& ev,
        const TActorContext& ctx)
    {
        auto it = InFlight.find(ev->Cookie);
        if (it == InFlight.end()) {
            return;
        }

        Fail(
            ctx,
            it->first,
            it->second,
            MakeError(E_REJECTED, "tablet is gone"));
        InFlight.erase(it);
    }

    void HandleTabletGone(
        const TEvRdmaTabletPrivate::TEvTabletGone::TPtr& ev,
        const TActorContext& ctx)
    {
        const auto& tabletActorId = ev->Get()->TabletActorId;

        LOG_INFO(ctx, TFileStoreComponents::RDMA,
            "Tablet %s left the RDMA tablet registry",
            ToString(tabletActorId).c_str());

        for (auto it = InFlight.begin(); it != InFlight.end();) {
            if (it->second.TabletActorId == tabletActorId) {
                Fail(
                    ctx,
                    it->first,
                    it->second,
                    MakeError(E_REJECTED, "tablet is shutting down"));
                InFlight.erase(it++);
            } else {
                ++it;
            }
        }
    }

    void ScheduleSweep(const TActorContext& ctx)
    {
        if (!RequestTimeout || SweepScheduled || InFlight.empty()) {
            return;
        }
        SweepScheduled = true;
        ctx.Schedule(
            Max(
                RequestTimeout / TimeoutSweepsPerRequestTimeout,
                TDuration::Seconds(1)),
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
                ReportTabletRdmaCritEvent(
                    TStringBuilder()
                    << "RDMA request timed out: cookie " << it->first
                    << ", tablet " << ToString(it->second.TabletActorId));
                Fail(
                    ctx,
                    it->first,
                    it->second,
                    MakeError(E_TIMEOUT, "RDMA tablet request timed out"));
                InFlight.erase(it++);
            } else {
                ++it;
            }
        }

        ScheduleSweep(ctx);
    }

    void HandleFileSystemEvent(
        const TEvIndexTabletProxy::TEvFileSystemEvent::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ctx);

        ReportTabletRdmaCritEvent(
            TStringBuilder()
            << "FileSystemEvent reached the RDMA forwarder: sender "
            << ev->Sender.ToString());
    }

    void HandlePoisonPill(
        const TEvents::TEvPoisonPill::TPtr& ev,
        const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        for (auto& [cookie, inflight]: InFlight) {
            Fail(
                ctx,
                cookie,
                inflight,
                MakeError(E_REJECTED, "rdma forwarder stopped"));
        }
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
                ActorContext(),                                                \
                (*x)->Cookie,                                                  \
                TTabletTransportResponse(MakeError(                            \
                    E_NOT_IMPLEMENTED,                                         \
                    "response payload is not supported over RDMA")));          \
            break;                                                             \
        }                                                                      \
        Complete(                                                              \
            ActorContext(),                                                    \
            (*x)->Cookie,                                                      \
            TTabletTransportResponse(                                          \
                ns::TEv##name##Response::EventType,                            \
                std::make_unique<TRecord>(std::move((*x)->Get()->Record))));   \
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

            HFunc(TEvRdmaTabletPrivate::TEvTrackRequest, HandleTrackRequest);
            HFunc(TEvRdmaTabletPrivate::TEvTabletGone, HandleTabletGone);
            HFunc(TEvents::TEvUndelivered, HandleUndelivered);
            HFunc(TEvents::TEvWakeup, HandleWakeup);

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
    TDuration requestTimeout)
{
    return std::make_unique<TRdmaTabletForwarderActor>(requestTimeout);
}

}   // namespace NCloud::NFileStore::NStorage
