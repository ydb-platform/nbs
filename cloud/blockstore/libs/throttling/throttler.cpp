#include "throttler.h"

#include "throttler_logger.h"
#include "throttler_metrics.h"
#include "throttler_policy.h"
#include "throttler_tracker.h"

#include <cloud/blockstore/libs/diagnostics/request_stats.h>
#include <cloud/blockstore/libs/diagnostics/volume_stats.h>
#include <cloud/blockstore/libs/service/context.h>
#include <cloud/blockstore/libs/service/service.h>
#include <cloud/storage/core/libs/common/scheduler.h>
#include <cloud/storage/core/libs/common/timer.h>

#include <util/generic/deque.h>
#include <util/system/mutex.h>

namespace NCloud::NBlockStore {

using namespace NThreading;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_METHOD(name, ...)                                   \
    struct T##name##Method                                                     \
    {                                                                          \
        static constexpr EBlockStoreRequest Request = EBlockStoreRequest::name;\
                                                                               \
        using TRequest = NProto::T##name##Request;                             \
        using TResponse = NProto::T##name##Response;                           \
                                                                               \
        template <typename T, typename ...TArgs>                               \
        static TFuture<TResponse> Execute(T& client, TArgs&& ...args)          \
        {                                                                      \
            return client.name(std::forward<TArgs>(args)...);                  \
        }                                                                      \
    };                                                                         \
// BLOCKSTORE_DECLARE_METHOD

BLOCKSTORE_SERVICE(BLOCKSTORE_DECLARE_METHOD)

#undef BLOCKSTORE_DECLARE_METHOD

////////////////////////////////////////////////////////////////////////////////

struct TRequestStateBase
    : public TAtomicRefCount<TRequestStateBase>
{
    EBlockStoreRequest Type;
    ui64 Bytes;
    IVolumeInfoPtr VolumeInfo;

    TRequestStateBase(
            EBlockStoreRequest type,
            ui64 bytes,
            IVolumeInfoPtr&& volumeInfo)
        : Type(type)
        , Bytes(bytes)
        , VolumeInfo(std::move(volumeInfo))
    {
    }

    virtual ~TRequestStateBase() = default;
};

////////////////////////////////////////////////////////////////////////////////

template <typename T>
struct TRequestState
    : public TRequestStateBase
{
    std::weak_ptr<IBlockStore> Client;
    TCallContextPtr CallContext;
    std::shared_ptr<typename T::TRequest> Request;
    TPromise<typename T::TResponse> Response;

    TRequestState(
            std::weak_ptr<IBlockStore> client,
            TCallContextPtr callContext,
            std::shared_ptr<typename T::TRequest> request,
            TPromise<typename T::TResponse> response,
            ui64 bytes,
            IVolumeInfoPtr volumeInfo)
        : TRequestStateBase(T::Request, bytes, std::move(volumeInfo))
        , Client(std::move(client))
        , CallContext(std::move(callContext))
        , Request(std::move(request))
        , Response(std::move(response))
    {}
};

using TRequestStateBasePtr = TIntrusivePtr<TRequestStateBase>;

template <typename TMethod>
using TRequestStatePtr = TIntrusivePtr<TRequestState<TMethod>>;

////////////////////////////////////////////////////////////////////////////////

class TThrottler final
    : public IThrottler
    , public std::enable_shared_from_this<TThrottler>
{
private:
    const IThrottlerLoggerPtr ThrottlerLogger;
    const IThrottlerMetricsPtr ThrottlerMetrics;
    const IThrottlerTrackerPtr ThrottlerTracker;
    const ITimerPtr Timer;
    const ISchedulerPtr Scheduler;
    const IVolumeStatsPtr VolumeStats;

    TMutex ThrottlerLock;
    IThrottlerPolicyPtr ThrottlerPolicy;
    TDeque<TRequestStateBasePtr> PostponedRequests;

    TAtomic ShouldStop = false;

public:
    TThrottler(
            IThrottlerLoggerPtr throttlerLogger,
            IThrottlerMetricsPtr throttlerMetrics,
            IThrottlerPolicyPtr throttlerPolicy,
            IThrottlerTrackerPtr throttlerTracker,
            ITimerPtr timer,
            ISchedulerPtr scheduler,
            IVolumeStatsPtr volumeStats)
        : ThrottlerLogger(std::move(throttlerLogger))
        , ThrottlerMetrics(std::move(throttlerMetrics))
        , ThrottlerTracker(std::move(throttlerTracker))
        , Timer(std::move(timer))
        , Scheduler(std::move(scheduler))
        , VolumeStats(std::move(volumeStats))
        , ThrottlerPolicy(std::move(throttlerPolicy))
    {}

    void Start() override
    {
        UpdateUsedQuotaWithScheduler();
        TrimRegisteredWithScheduler();
    }

    void Stop() override
    {
        AtomicSet(ShouldStop, true);
    }

    void UpdateThrottlerPolicy(IThrottlerPolicyPtr throttlerPolicy) override
    {
        with_lock (ThrottlerLock) {
            ThrottlerPolicy = std::move(throttlerPolicy);
        }
    }

#define BLOCKSTORE_IMPLEMENT_METHOD(name, ...)                                 \
    TFuture<NProto::T##name##Response> name(                                   \
        const IBlockStorePtr& client,                                          \
        TCallContextPtr callContext,                                           \
        std::shared_ptr<NProto::T##name##Request> request) override            \
    {                                                                          \
        return HandleRequest<T##name##Method>(                                 \
            client,                                                            \
            std::move(callContext),                                            \
            std::move(request));                                               \
    }                                                                          \
// BLOCKSTORE_IMPLEMENT_METHOD

    BLOCKSTORE_SERVICE(BLOCKSTORE_IMPLEMENT_METHOD)

#undef BLOCKSTORE_IMPLEMENT_METHOD

private:
    template <typename TMethod>
    bool PreprocessPostponedRequest(
        TRequestState<TMethod>& state,
        TInstant now)
    {
        if (state.VolumeInfo) {
            auto delay = PostponeIfNeeded<TMethod>(
                now,
                state.VolumeInfo->GetInfo().GetStorageMediaKind(),
                state.Bytes);

            if (delay.GetValue()) {
                ThrottlerTracker->TrackPostponedRequest(
                    *state.CallContext,
                    *state.Request);
                return false;
            }
        }

        PostponedRequests.pop_front();
        return true;
    }

    void ProcessPostponed()
    {
        while (true) {
            auto guard = Guard(ThrottlerLock);

            if (!PostponedRequests) {
                return;
            }

            auto queueSize = PostponedRequests.size();
            auto head = PostponedRequests.front();
            const auto now = Timer->Now();
            const auto cycles = GetCycleCount();

#define REQUEST_TYPE_CASE(name)                                                \
    case EBlockStoreRequest::name: {                                           \
        using TMethod = T##name##Method;                                       \
        using TConcreteState = TRequestState<TMethod>;                         \
        auto concreteState = static_cast<TConcreteState*>(head.Get());         \
        if (!PreprocessPostponedRequest<TMethod>(*concreteState, now)) {       \
            return;                                                            \
        }                                                                      \
        guard.Release();                                                       \
        ThrottlerTracker->TrackAdvancedRequest(                                \
            *concreteState->CallContext,                                       \
            *concreteState->Request);                                          \
        ThrottlerLogger->LogAdvancedRequest(                                   \
            cycles,                                                            \
            *concreteState->CallContext,                                       \
            concreteState->VolumeInfo.get(),                                   \
            *concreteState->Request);                                          \
        ExecuteRequest<TMethod>(*concreteState);                               \
                                                                               \
        break;                                                                 \
    }                                                                          \
// REQUEST_TYPE_CASE

            switch (head->Type) {
                REQUEST_TYPE_CASE(ZeroBlocks);
                REQUEST_TYPE_CASE(WriteBlocks);
                REQUEST_TYPE_CASE(ReadBlocks);
                REQUEST_TYPE_CASE(WriteBlocksLocal);
                REQUEST_TYPE_CASE(ReadBlocksLocal);

                default: {
                    Y_DEBUG_ABORT_UNLESS(0);
                }
            }

            // This if avoids enqueueing many ProcessPostponed() callbacks in
            // the following case:
            //
            // 1. Thread 1: ProcessPostponed() is called
            // 2. Thread 1: ProcessPostponed() finishes processing the last
            //    request in the PostponedQueue - queue is empty at this point
            //    of time
            // 3. Thread 2: New request is postponed, ProcessPostponed()
            //    callback is enqueued since PostponedQueue was empty
            // 4. Thread 1: ProcessPostponed() starts another iteration - queue
            //    is not empty, the request is processed, queue is empty again
            // 5. Thread 2: Another request is postponed, ProcessPostponed()
            //    callback is enqueued again
            // ... steps 2-4 can be repeated many times in a row
            //
            // Thus we need to stop the ProcessPostponed() loop after step 2.
            if (queueSize == 1) {
                return;
            }
        }

#undef REQUEST_TYPE_CASE
    }

    template <typename TMethod>
    TDuration PostponeIfNeeded(
        TInstant now,
        NCloud::NProto::EStorageMediaKind mediaKind,
        ui64 bytes)
    {
        auto delay = ThrottlerPolicy->SuggestDelay(
            now,
            mediaKind,
            TMethod::Request,
            bytes);

        if (delay.GetValue()) {
            ScheduleFunction(
                delay,
                &TThrottler::ProcessPostponed);
        }

        return delay;
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> HandleRequest(
        const IBlockStorePtr& client,
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        Y_UNUSED(TMethod::Request);

        auto volumeInfo = VolumeStats->GetVolumeInfo(
            GetDiskId(*request),
            GetClientId(*request));
        ThrottlerTracker->TrackReceivedRequest(
            *callContext,
            volumeInfo.get(),
            *request);

        if constexpr (ShouldBeThrottled<typename TMethod::TRequest>()) {
            return this->HandleThrottledRequest<TMethod>(
                client,
                callContext,
                std::move(request),
                std::move(volumeInfo));
        }
        return TMethod::Execute(
            *client,
            std::move(callContext),
            std::move(request));
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> HandleThrottledRequest(
        const IBlockStorePtr& client,
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request,
        IVolumeInfoPtr volumeInfo)
    {
        TRequestStatePtr<TMethod> state;

        TDuration delay;
        auto response = NewPromise<typename TMethod::TResponse>();
        auto future = response.GetFuture();

        const auto now = Timer->Now();
        const auto cycles = GetCycleCount();
        with_lock (ThrottlerLock) {
            ui64 bytes = 0;

            if (volumeInfo) {
                bytes = CalculateBytesCount(
                    *request,
                    volumeInfo->GetInfo().GetBlockSize());
            }

            if (PostponedRequests) {
                delay = TDuration::Max();
            } else if (volumeInfo) {
                delay = PostponeIfNeeded<TMethod>(
                    now,
                    volumeInfo->GetInfo().GetStorageMediaKind(),
                    bytes
                );
            } else {
                delay = TDuration::Zero();
            }

            if (delay.GetValue()) {
                state = MakeIntrusive<TRequestState<TMethod>>(
                    client,
                    std::move(callContext),
                    std::move(request),
                    std::move(response),
                    bytes,
                    std::move(volumeInfo)
                );
                PostponedRequests.push_back(state);
            }
        }

        if (state) {
            ThrottlerTracker->TrackPostponedRequest(
                *state->CallContext,
                *state->Request);
            ThrottlerLogger->LogPostponedRequest(
                cycles,
                *state->CallContext,
                state->VolumeInfo.get(),
                *state->Request,
                delay);
        } else {
            ExecuteRequest<TMethod>(
                *client,
                std::move(callContext),
                std::move(request),
                std::move(response));
        }

        return future;
    }

    template <>
    TFuture<typename TMountVolumeMethod::TResponse>
        HandleRequest<TMountVolumeMethod>(
            const IBlockStorePtr& client,
            TCallContextPtr callContext,
            std::shared_ptr<typename TMountVolumeMethod::TRequest> request)
    {
        Y_UNUSED(TMountVolumeMethod::Request);

        return HandleMountUnmountRequest<TMountVolumeMethod>(
            client,
            std::move(callContext),
            std::move(request));
    }

    template <>
    TFuture<typename TUnmountVolumeMethod::TResponse>
        HandleRequest<TUnmountVolumeMethod>(
            const IBlockStorePtr& client,
            TCallContextPtr callContext,
            std::shared_ptr<typename TUnmountVolumeMethod::TRequest> request)
    {
        Y_UNUSED(TUnmountVolumeMethod::Request);

        return HandleMountUnmountRequest<TUnmountVolumeMethod>(
            client,
            std::move(callContext),
            std::move(request));
    }

    template <typename TMethod>
    TFuture<typename TMethod::TResponse> HandleMountUnmountRequest(
        const IBlockStorePtr& client,
        TCallContextPtr callContext,
        std::shared_ptr<typename TMethod::TRequest> request)
    {
        auto diskId = GetDiskId(*request);
        auto clientId = GetClientId(*request);
        auto instanceId = request->GetInstanceId();

        auto volumeInfo = VolumeStats->GetVolumeInfo(diskId, clientId);
        ThrottlerTracker->TrackReceivedRequest(
            *callContext,
            volumeInfo.get(),
            *request);

        auto weakPtr = weak_from_this();
        return TMethod::Execute(
            *client,
            std::move(callContext),
            std::move(request)).Subscribe(
                [=, weakPtr = std::move(weakPtr)] (const auto& future) {
                    const auto& response = future.GetValue();
                    auto thisPtr = weakPtr.lock();

                    if (thisPtr && !HasError(response)) {
                        thisPtr->FinalizeRequest(
                            response,
                            diskId,
                            instanceId.empty() ? clientId : instanceId);
                    }
                });
    }

    void FinalizeRequest(
        const TMountVolumeMethod::TResponse& response,
        const TString& diskId,
        const TString& instanceId)
    {
        if (response.HasVolume()) {
            ThrottlerMetrics->Register(diskId, instanceId);
        }
    }

    void FinalizeRequest(
        const TUnmountVolumeMethod::TResponse& response,
        const TString& diskId,
        const TString& instanceId)
    {
        Y_UNUSED(response);

        ThrottlerMetrics->Unregister(diskId, instanceId);
    }

    template <typename T>
    void ExecuteRequest(TRequestState<T>& state)
    {
        auto client = state.Client.lock();
        if (!client) {
            state.Response.SetValue(
                TErrorResponse(E_FAIL, "Client endpoint was closed"));
            return;
        }

        ExecuteRequest<T>(
            *client,
            state.CallContext,
            state.Request,
            state.Response);
    }

    template <typename T>
    void ExecuteRequest(
        IBlockStore& client,
        TCallContextPtr callContext,
        std::shared_ptr<typename T::TRequest> request,
        TPromise<typename T::TResponse> promise)
    {
        T::Execute(client, callContext, request).Subscribe(
            [=, this, promise = std::move(promise)] (auto future) mutable {
                auto response = ExtractResponse(future);

                try {
                    promise.SetValue(std::move(response));
                } catch (...) {
                    ThrottlerLogger->LogError(
                        *request,
                        CurrentExceptionMessage());
                }
            }
        );
    }

    template <typename TFunction>
    void ScheduleFunction(TDuration duration, TFunction function)
    {
        if (AtomicGet(ShouldStop)) {
            return;
        }

        auto weakPtr = weak_from_this();
        Scheduler->Schedule(
            Timer->Now() + duration,
            [func = std::move(function), weakPtr = std::move(weakPtr)] {
                if (auto thisPtr = weakPtr.lock()) {
                    std::invoke(std::move(func), thisPtr.get());
                }
            });
    }

    void UpdateUsedQuotaWithScheduler()
    {
        ui64 quota = 0;
        with_lock (ThrottlerLock) {
            quota = static_cast<ui64>(
                ThrottlerPolicy->CalculateCurrentSpentBudgetShare(
                    Timer->Now()) *
                100.0);
        }
        ThrottlerMetrics->UpdateUsedQuota(quota);
        ThrottlerMetrics->UpdateMaxUsedQuota();

        ScheduleFunction(
            UPDATE_THROTTLER_USED_QUOTA_INTERVAL,
            &TThrottler::UpdateUsedQuotaWithScheduler);
    }

    void TrimRegisteredWithScheduler()
    {
        ThrottlerMetrics->Trim(Timer->Now());

        ScheduleFunction(
            TRIM_THROTTLER_METRICS_INTERVAL,
            &TThrottler::TrimRegisteredWithScheduler);
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

IThrottlerPtr CreateThrottler(
    IThrottlerLoggerPtr throttlerLogger,
    IThrottlerMetricsPtr throttlerMetrics,
    IThrottlerPolicyPtr throttlerPolicy,
    IThrottlerTrackerPtr throttlerTracker,
    ITimerPtr timer,
    ISchedulerPtr scheduler,
    IVolumeStatsPtr volumeStats)
{
    return std::make_shared<TThrottler>(
        std::move(throttlerLogger),
        std::move(throttlerMetrics),
        std::move(throttlerPolicy),
        std::move(throttlerTracker),
        std::move(timer),
        std::move(scheduler),
        std::move(volumeStats));
}

}   // namespace NCloud::NBlockStore
