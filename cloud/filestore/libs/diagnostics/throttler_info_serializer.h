#pragma once

#include "public.h"

#include <cloud/filestore/libs/service/request.h>

#include <cloud/storage/core/libs/common/context.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

template <typename T>
void BuildThrottlerInfo(const TCallContextBase& ctx, T& request)
{
    if constexpr (HasResponseHeaders<T>()) {
        request.MutableHeaders()
            ->MutableThrottler()
            ->SetDelay(ctx.Time(EProcessingStage::Postponed).MicroSeconds());
    }
}

}   // namespace NCloud::NFileStore
