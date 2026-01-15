#include "wilson_trace_compatibility.h"

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GenerateSpanId()
{
    for (;;) {
        // SpanId can't be zero
        if (const ui64 res = RandomNumber<ui64>(); res) {
            return res;
        }
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

NWilson::TTraceId GetTraceIdForRequestId(
    NLWTrace::TOrbit& orbit,
    ui64 requestId)
{
    if (requestId == 0 || !orbit.HasShuttles()) {
        return {};
    }

    ui64 spanId = 0;

    orbit.ForEachShuttle([&](const NLWTrace::IShuttle* s)
                         { spanId = s->GetSpanId(); });

    if (!spanId) {
        spanId = GenerateSpanId();
    }

    return {
        {requestId, 0},
        spanId,
        NWilson::TTraceId::MAX_VERBOSITY,
        NWilson::TTraceId::MAX_TIME_TO_LIVE};
}

}   // namespace NCloud
