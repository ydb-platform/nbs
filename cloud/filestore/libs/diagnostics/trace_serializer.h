#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/diagnostics/trace_serializer.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
bool HandleTraceInfo(
    const ITraceSerializerPtr& traceSerializer,
    const TCallContextBasePtr& ctx,
    T& record)
{
    if constexpr (HasResponseHeaders<T>()) {
        if (!ctx->LWOrbit.HasShuttles()) {
            return false;
        }

        auto& headers = *record.MutableHeaders();
        traceSerializer->HandleTraceInfo(
            std::move(*headers.MutableTrace()),
            ctx->LWOrbit,
            ctx->GetRequestStartedCycles(),
            GetCycleCount());

        headers.ClearTrace();
        return true;
    }

    return false;
}

template <typename T>
bool BuildTraceInfo(
    const ITraceSerializerPtr& traceSerializer,
    const TCallContextBasePtr& ctx,
    T& record)
{
    if constexpr (HasResponseHeaders<T>()) {
        if (!traceSerializer->IsTraced(ctx->LWOrbit)) {
            return false;
        }

        traceSerializer->BuildTraceInfo(
            *record.MutableHeaders()->MutableTrace(),
            ctx->LWOrbit,
            ctx->GetRequestStartedCycles(),
            GetCycleCount());
        return true;
    }

    return false;
}

}   // namespace NCloud::NFileStore
