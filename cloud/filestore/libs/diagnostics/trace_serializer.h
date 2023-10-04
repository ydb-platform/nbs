#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/context.h>

#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

namespace NCloud::NFileStore {

namespace NImpl {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
concept TTraceable = requires(T v)
{
    {v.GetTrace()};
    {v.MutableTrace()};
    {v.ClearTrace()};
};

}   // namespace NImpl

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void HandleTraceInfo(
    const ITraceSerializerPtr& traceSerializer,
    const TCallContextPtr& ctx,
    T& record)
{
    if constexpr (NImpl::TTraceable<T>) {
        if (!ctx->LWOrbit.HasShuttles()) {
            return;
        }

        traceSerializer->HandleTraceInfo(
            record.GetTrace(),
            ctx->LWOrbit,
            ctx->GetRequestStartedCycles(),
            GetCycleCount());

        record.ClearTrace();
    }
}

template <typename T>
void BuildTraceInfo(
    const ITraceSerializerPtr& traceSerializer,
    const TCallContextPtr& ctx,
    T& record)
{
    if constexpr (NImpl::TTraceable<T>) {
        if (!traceSerializer->IsTraced(ctx->LWOrbit)) {
            return;
        }

        traceSerializer->BuildTraceInfo(
            *record.MutableTrace(),
            ctx->LWOrbit,
            ctx->GetRequestStartedCycles(),
            GetCycleCount());
    }
}

}   // namespace NCloud::NFileStore
