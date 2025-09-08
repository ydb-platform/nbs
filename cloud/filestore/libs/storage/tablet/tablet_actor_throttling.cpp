#include "tablet_actor.h"

#include <cloud/filestore/libs/diagnostics/metrics/operations.h>
#include <cloud/filestore/libs/storage/api/service.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/throttling/tablet_throttler.h>

#include <contrib/ydb/library/actors/core/event.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

template <typename TProtoRequest>
ui64 CalculateByteCount(const TProtoRequest& request)
{
    return request.GetLength();
}

template <>
ui64 CalculateByteCount<NProto::TWriteDataRequest>(
    const NProto::TWriteDataRequest& request)
{
    return request.GetDataSize();
}

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest>
TThrottlingRequestInfo BuildRequestInfo(
    const TRequest& request,
    ui32 policyVersion);

template <>
TThrottlingRequestInfo BuildRequestInfo(
    const TEvService::TEvReadDataRequest& request,
    ui32 policyVersion)
{
    return {
        CalculateByteCount(request.Record),
        static_cast<ui32>(TThrottlingPolicy::EOpType::Read),
        policyVersion
    };
}

template <>
TThrottlingRequestInfo BuildRequestInfo(
    const TEvService::TEvWriteDataRequest& request,
    ui32 policyVersion)
{
    return {
        CalculateByteCount(request.Record),
        static_cast<ui32>(TThrottlingPolicy::EOpType::Write),
        policyVersion
    };
}

template <>
TThrottlingRequestInfo BuildRequestInfo(
    const TEvIndexTablet::TEvDescribeDataRequest& request,
    ui32 policyVersion)
{
    return {
        CalculateByteCount(request.Record),
        static_cast<ui32>(TThrottlingPolicy::EOpType::Read),
        policyVersion
    };
}

template <>
TThrottlingRequestInfo BuildRequestInfo(
    const TEvIndexTablet::TEvGenerateBlobIdsRequest& request,
    ui32 policyVersion)
{
    return {
        CalculateByteCount(request.Record),
        static_cast<ui32>(TThrottlingPolicy::EOpType::Write),
        policyVersion
    };
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

void TIndexTabletActor::HandleUpdateLeakyBucketCounters(
    const TEvIndexTabletPrivate::TEvUpdateLeakyBucketCounters::TPtr& /*ev*/,
    const NActors::TActorContext& ctx)
{
    const ui64 currentRate = std::ceil(
        GetThrottlingPolicy().CalculateCurrentSpentBudgetShare(ctx.Now()) * 100);

    Metrics.MaxUsedQuota.Record(currentRate);
    NMetrics::Add(Metrics.UsedQuota, currentRate);

    UpdateLeakyBucketCountersScheduled = false;
    ScheduleUpdateCounters(ctx);
}

void TIndexTabletActor::UpdateDelayCounter(
    TThrottlingPolicy::EOpType opType,
    TDuration time)
{
    switch (opType) {
        case TThrottlingPolicy::EOpType::Read: {
            Metrics.ReadDataPostponed.Record(time.MicroSeconds());
            return;
        }
        case TThrottlingPolicy::EOpType::Write: {
            Metrics.WriteDataPostponed.Record(time.MicroSeconds());
            return;
        }
        default:
            Y_DEBUG_ABORT_UNLESS(false);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <typename TMethod>
NProto::TError TIndexTabletActor::Throttle(
    const typename TMethod::TRequest::TPtr& ev,
    const TActorContext& ctx)
{
    static const auto ok = MakeError(S_OK);
    static const auto err = MakeError(E_FS_THROTTLED, "Throttled");

    auto* msg = ev->Get();

    const auto requestInfo = BuildRequestInfo(
        *msg,
        GetThrottlingPolicy().GetVersion()
    );

    const auto status = Throttler->Throttle(
        ctx,
        msg->CallContext,
        requestInfo,
        [&ev]() { return NActors::IEventHandlePtr(ev.Release()); },
        TMethod::Name);

    switch (status) {
        case ETabletThrottlerStatus::POSTPONED: {
            NMetrics::Inc(Metrics.PostponedRequests);
            break;
        }
        case ETabletThrottlerStatus::ADVANCED: {
            break;
        }
        case ETabletThrottlerStatus::REJECTED: {
            NMetrics::Inc(Metrics.RejectedRequests);
            return err;
        }
        default:
            Y_DEBUG_ABORT_UNLESS(false);
    }

    return ok;
}

template <typename TMethod>
bool TIndexTabletActor::ThrottleIfNeeded(
    const typename TMethod::TRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    if (!Config->GetThrottlingEnabled() ||
        !GetPerformanceProfile().GetThrottlingEnabled() ||
        ev->Get()->Record.GetHeaders().GetThrottlingDisabled())
    {
        return false;
    }

    auto error = Throttle<TMethod>(ev, ctx);
    if (HasError(error)) {
        FILESTORE_TRACK(
            ResponseSent_Tablet,
            ev->Get()->CallContext,
            TMethod::Name);

        auto response = std::make_unique<typename TMethod::TResponse>(
            std::move(error)
        );
        NCloud::Reply(ctx, *ev, std::move(response));
        return true;
    } else if (!ev) {
        // Request postponed.
        return true;
    }

    return false;
}

////////////////////////////////////////////////////////////////////////////////

#define GENERATE_IMPL(name, ns)                                                \
template NProto::TError TIndexTabletActor::Throttle<ns::T##name##Method>(      \
    const ns::TEv##name##Request::TPtr& ev,                                    \
    const TActorContext& ctx);                                                 \
                                                                               \
template bool TIndexTabletActor::ThrottleIfNeeded<ns::T##name##Method>(        \
    const ns::TEv##name##Request::TPtr& ev,                                    \
    const TActorContext& ctx);                                                 \
// GENERATE_IMPL

GENERATE_IMPL(ReadData,  TEvService)
GENERATE_IMPL(WriteData, TEvService)
GENERATE_IMPL(DescribeData, TEvIndexTablet)
GENERATE_IMPL(GenerateBlobIds, TEvIndexTablet)

#undef GENERATE_IMPL

}   // namespace NCloud::NFileStore::NStorage
