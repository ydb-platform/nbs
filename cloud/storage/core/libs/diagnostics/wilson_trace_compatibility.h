#pragma once

#include "library/cpp/lwtrace/shuttle.h"
#include <contrib/ydb/library/actors/wilson/wilson_trace.h>

#include <util/system/types.h>

namespace NCloud {

NWilson::TTraceId GetTraceIdForRequestId(
    NLWTrace::TOrbit& orbit,
    ui64 requestId);

} // namespace NCloud
