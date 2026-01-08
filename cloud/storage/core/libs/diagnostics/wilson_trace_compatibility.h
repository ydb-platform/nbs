#pragma once

#include "library/cpp/lwtrace/shuttle.h"
#include <contrib/ydb/library/actors/wilson/wilson_trace.h>

#include <util/system/types.h>

namespace NCloud {

// Function that extracts the spanId from the orbit and constructs the
// NWilson::TTraceId from it and the requestId, which is used for tracing in
// YDB. Thread-safe.
NWilson::TTraceId GetTraceIdForRequestId(
    NLWTrace::TOrbit& orbit,
    ui64 requestId);

} // namespace NCloud
