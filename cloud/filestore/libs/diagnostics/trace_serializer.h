#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void HandleTraceInfo(
    const ITraceSerializerPtr& traceSerializer,
    const TCallContextBasePtr& ctx,
    T& record)
{
    if constexpr (HasResponseHeaders<T>()) {
        if (!ctx->LWOrbit.HasShuttles()) {
            return;
        }

        traceSerializer->HandleTraceInfo(
            record.GetHeaders().GetTrace(),
            ctx->LWOrbit,
            ctx->GetRequestStartedCycles(),
            GetCycleCount());

        record.MutableHeaders()->ClearTrace();
    }
}

template <typename T>
void BuildTraceInfo(
    const ITraceSerializerPtr& traceSerializer,
    const TCallContextBasePtr& ctx,
    T& record)
{
    if constexpr (HasResponseHeaders<T>()) {
        if (!traceSerializer->IsTraced(ctx->LWOrbit)) {
            return;
        }

        traceSerializer->BuildTraceInfo(
            *record.MutableHeaders()->MutableTrace(),
            ctx->LWOrbit,
            ctx->GetRequestStartedCycles(),
            GetCycleCount());
    }
}

}   // namespace NCloud::NFileStore
