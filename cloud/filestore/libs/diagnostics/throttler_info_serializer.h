#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/context.h>
#include <cloud/storage/core/libs/diagnostics/postpone_time_predictor.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void BuildThrottlerInfo(const TCallContextBase& ctx, T& record)
{
    if constexpr (HasResponseHeaders<T>()) {
        record.MutableHeaders()->MutableThrottler()->SetDelay(
            ctx.Time(EProcessingStage::Postponed).MicroSeconds());
    }
}

template <typename T>
void HandleThrottlerInfo(TCallContextBase& ctx, T& record)
{
    if constexpr (HasResponseHeaders<T>()) {
        const auto delay = TDuration::MicroSeconds(
            record.GetHeaders().GetThrottler().GetDelay());

        ctx.AddTime(EProcessingStage::Postponed, delay);
    }
}

}   // namespace NCloud::NFileStore
