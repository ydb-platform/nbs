#pragma once

#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/request_helpers.h>
#include <cloud/blockstore/libs/storage/api/service.h>
#include <cloud/blockstore/libs/storage/core/probes.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/kikimr/actorsystem.h>

#include <contrib/ydb/library/actors/core/event.h>
#include <contrib/ydb/library/actors/core/events.h>
#include <contrib/ydb/library/actors/core/log.h>
#include <contrib/ydb/library/actors/core/scheduler_cookie.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/spinlock.h>

#include <memory>
#include <utility>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

// Tracks in-flight requests of one method type for a permanent handler actor.
// SendRequest may be called concurrently from arbitrary service threads, while
// responses and timeouts arrive on actor-system threads. Lock protects the
// request map and coordinates dispatch with actor shutdown.
template <typename T>
class TMethodHandler final
{
    using TRequest = typename T::TRequest;
    using TRequestProto = typename T::TRequestProto;

    using TResponse = typename T::TResponse;
    using TResponseProto = typename T::TResponseProto;

    struct TRequestState
    {
        NThreading::TPromise<TResponseProto> Response;
        TCallContextPtr CallContext;
        TString DiskId;
        NActors::TSchedulerCookieHolder TimeoutCookie;

        // Set until SendRequest has finished sending the request and scheduling
        // its timeout. RejectRequests leaves such a request in the map so the
        // dispatching thread can safely finish and reject it in FinishDispatch.
        // Access after publication is protected by Lock.
        bool Dispatching = true;

        TRequestState(
            NThreading::TPromise<TResponseProto> response,
            TCallContextPtr callContext,
            TString diskId)
            : Response(std::move(response))
            , CallContext(std::move(callContext))
            , DiskId(std::move(diskId))
        {}
    };

    using TRequestStatePtr = std::shared_ptr<TRequestState>;

private:
    TAdaptiveLock Lock;
    THashMap<ui64, TRequestStatePtr> Requests;
    bool Closed = false;

public:
    // TEvWakeup reserves zero as its default, untagged value.
    static constexpr ui64 TimeoutTag = static_cast<ui64>(T::Request) + 1;

    TMethodHandler() = default;
    ~TMethodHandler() = default;

    void SendRequest(
        NCloud::IActorSystem& actorSystem,
        TLog& Log,
        TCallContextPtr callContext,
        std::shared_ptr<TRequestProto> request,
        NThreading::TPromise<TResponseProto> response,
        TDuration requestTimeout,
        NActors::TActorId actorId,
        ui64 cookie)
    {
        auto state = std::make_shared<TRequestState>(
            std::move(response),
            std::move(callContext),
            GetDiskId(*request));

        auto event = std::make_unique<NActors::IEventHandle>(
            NStorage::MakeStorageServiceId(),
            actorId,
            std::make_unique<TRequest>(state->CallContext, std::move(*request))
                .release(),
            /*flags=*/0,
            cookie,
            /*forwardOnNondelivery=*/nullptr);

        NActors::IEventHandlePtr timeoutEvent;
        if (requestTimeout && requestTimeout != TDuration::Max()) {
            timeoutEvent = std::make_unique<NActors::IEventHandle>(
                actorId,
                actorId,
                new NActors::TEvents::TEvWakeup(TimeoutTag),
                /*flags=*/0,
                cookie);
        }

        bool accepted = false;
        with_lock (Lock) {
            if (!Closed) {
                accepted = true;
                Requests.emplace(cookie, state);
            }
        }

        if (!accepted) {
            RejectRequest(Log, *state);
            return;
        }

        STORAGE_TRACE(
            TRequestInfo(
                T::Request,
                state->CallContext->RequestId,
                state->DiskId)
            << " sending request");

        GLOBAL_LWTRACK(
            BLOCKSTORE_STORAGE_PROVIDER,
            RequestSent_Proxy,
            state->CallContext->LWOrbit,
            GetBlockStoreRequestName(T::Request),
            state->CallContext->RequestId);

        actorSystem.Send(std::move(event));

        if (timeoutEvent) {
            state->TimeoutCookie.Reset(NActors::ISchedulerCookie::Make2Way());
            actorSystem.Schedule(
                requestTimeout,
                std::move(timeoutEvent),
                state->TimeoutCookie.Get());
        }

        FinishDispatch(Log, cookie, state);
    }

    void HandleResponse(
        const typename TResponse::TPtr& ev,
        const NActors::TActorContext& ctx)
    {
        auto state = ExtractRequest(ev->Cookie);
        if (!state) {
            ReportServiceProxyReceivedUnknownCookie(
                {{"request", GetBlockStoreRequestName(T::Request)},
                 {"cookie", ev->Cookie}});
            return;
        }

        GLOBAL_LWTRACK(
            BLOCKSTORE_STORAGE_PROVIDER,
            ResponseReceived_Proxy,
            state->CallContext->LWOrbit,
            GetBlockStoreRequestName(T::Request),
            state->CallContext->RequestId);

        LOG_TRACE_S(
            ctx,
            TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(
                T::Request,
                state->CallContext->RequestId,
                state->DiskId)
                << " response received");

        CompleteRequest(ctx, *state, std::move(ev->Get()->Record));
    }

    void HandleTimeout(ui64 cookie, const NActors::TActorContext& ctx)
    {
        TRequestStatePtr state;
        if constexpr (IsWriteRequest(T::Request)) {
            state = FindRequest(cookie);
        } else {
            state = ExtractRequest(cookie);
        }

        if (!state) {
            return;
        }

        LOG_WARN_S(
            ctx,
            TBlockStoreComponents::SERVICE_PROXY,
            TRequestInfo(
                T::Request,
                state->CallContext->RequestId,
                state->DiskId)
                << " request wakeup timer hit");

        if constexpr (IsWriteRequest(T::Request)) {
            // Write requests are a special case: do not time them out because
            // TVolumeActor already protects against overlapping requests.
            ReportServiceProxyWakeupTimerHit(
                {{"disk", state->DiskId},
                 {"RequestId", state->CallContext->RequestId}});
            return;
        }

        TResponseProto response;
        auto& error = *response.MutableError();
        error.SetCode(E_TIMEOUT);
        error.SetMessage("Timeout");

        CompleteRequest(ctx, *state, std::move(response));
    }

    void RejectRequests(TLog& Log)
    {
        TVector<TRequestStatePtr> requests;
        with_lock (Lock) {
            Closed = true;
            requests.reserve(Requests.size());
            for (auto it = Requests.begin(); it != Requests.end();) {
                if (!it->second->Dispatching) {
                    requests.push_back(std::move(it->second));
                    Requests.erase(it++);
                } else {
                    ++it;
                }
            }
        }

        for (const auto& request: requests) {
            RejectRequest(Log, *request);
        }
    }

private:
    void FinishDispatch(TLog& Log, ui64 cookie, const TRequestStatePtr& state)
    {
        bool reject = false;
        with_lock (Lock) {
            state->Dispatching = false;
            if (Closed) {
                auto it = Requests.find(cookie);
                if (it != Requests.end() && it->second == state) {
                    Requests.erase(it);
                    reject = true;
                }
            }
        }

        if (reject) {
            RejectRequest(Log, *state);
        }
    }

    TRequestStatePtr FindRequest(ui64 cookie) const
    {
        with_lock (Lock) {
            auto it = Requests.find(cookie);
            return it != Requests.end() ? it->second : nullptr;
        }
    }

    TRequestStatePtr ExtractRequest(ui64 cookie)
    {
        TRequestStatePtr request;
        with_lock (Lock) {
            auto it = Requests.find(cookie);
            if (it != Requests.end()) {
                request = std::move(it->second);
                Requests.erase(it);
            }
        }
        return request;
    }

    static void CompleteRequest(
        const NActors::TActorContext& ctx,
        TRequestState& state,
        TResponseProto&& response)
    {
        try {
            state.Response.SetValue(std::move(response));
        } catch (...) {
            LOG_ERROR_S(
                ctx,
                TBlockStoreComponents::SERVICE_PROXY,
                TRequestInfo(
                    T::Request,
                    state.CallContext->RequestId,
                    state.DiskId)
                    << " failed to complete request: "
                    << CurrentExceptionMessage());
        }
    }

    static void RejectRequest(TLog& Log, TRequestState& state)
    {
        TResponseProto response;
        response.MutableError()->SetCode(E_REJECTED);

        try {
            state.Response.SetValue(std::move(response));
        } catch (...) {
            STORAGE_ERROR(
                TRequestInfo(
                    T::Request,
                    state.CallContext->RequestId,
                    state.DiskId)
                << " failed to reject request: " << CurrentExceptionMessage());
        }
    }
};

}   // namespace NCloud::NBlockStore::NServer
