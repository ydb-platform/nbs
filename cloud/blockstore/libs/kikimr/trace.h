#pragma once

#include "public.h"

#include <cloud/blockstore/libs/common/typeinfo.h>

#include <ydb/core/protos/services.pb.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/wilson/wilson_event.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

inline ui64 GetRequestId(const NWilson::TTraceId& traceId)
{
    return traceId.GetTraceId();
}

////////////////////////////////////////////////////////////////////////////////

template <typename T>
class TEventTraceAdaptor
{
private:
    const char* Action;
    const NActors::IActor& Actor;
    const T& Event;
    const NWilson::TTraceId* MergedTraceId;

public:
    TEventTraceAdaptor(
            const char* action,
            const NActors::IActor& actor,
            const T& event,
            const NWilson::TTraceId* mergedTraceId)
        : Action(action)
        , Actor(actor)
        , Event(event)
        , MergedTraceId(mergedTraceId)
    {}

    void Output(IOutputStream& out) const
    {
        const TString& activityType = NKikimrServices::TActivity::EType_Name(
            static_cast<NKikimrServices::TActivity::EType>(Actor.GetActivityType()));

        out << GetName<T>() << " " << Action
            << " {ActivityType# " << activityType
            << ", ActorId# " << Actor.SelfId();
        Describe<T>(out, Event);
        if (MergedTraceId && *MergedTraceId) {
            out << ", MergedNode# " << *MergedTraceId;
        }
        out << "}";
    }
};

template <typename T>
inline TEventTraceAdaptor<T> EventTrace(
    const char* action,
    const NActors::IActor& actor,
    const T& event,
    const NWilson::TTraceId* mergedTraceId,
    void* /*fake_param*/ = nullptr)
{
    return { action, actor, event, mergedTraceId };
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_TRACE_EVENT(ctx, action, traceId, actor, event, ...)        \
    if (::NWilson::TraceEnabled(ctx)) {                                        \
        ::NWilson::TTraceId* __traceId = traceId;                              \
        if (__traceId && *__traceId) {                                         \
            ::NWilson::TraceEvent(                                             \
                ctx,                                                           \
                __traceId,                                                     \
                NCloud::NBlockStore::EventTrace(                               \
                    action, *actor, *event, __VA_ARGS__),                      \
                ctx.Now());                                                    \
        }                                                                      \
    }                                                                          \
// BLOCKSTORE_TRACE_EVENT

#define BLOCKSTORE_TRACE_SENT(ctx, ...)                                        \
    BLOCKSTORE_TRACE_EVENT(ctx, "Sent", __VA_ARGS__, nullptr)                  \
// BLOCKSTORE_TRACE_SENT

#define BLOCKSTORE_TRACE_RECEIVED(ctx, ...)                                    \
    BLOCKSTORE_TRACE_EVENT(ctx, "Received", __VA_ARGS__, nullptr)              \
// BLOCKSTORE_TRACE_RECEIVED

}   // namespace NCloud::NBlockStore
