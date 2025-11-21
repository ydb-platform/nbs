#include "wilson_trace_compatibility.h"

namespace NCloud {

namespace {

////////////////////////////////////////////////////////////////////////////////

ui64 GenerateSpanId()
{
    for (;;) {
        if (const ui64 res = RandomNumber<ui64>(); res)
        {   // SpanId can't be zero
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

    orbit.ForEachShuttle(
        [&](const NLWTrace::IShuttle* s)
        {
            spanId = s->GetSpanId();
            return true;
        });

    if (!spanId) {
        spanId = GenerateSpanId();
    }

    using TTrace = std::array<ui64, 2>;

    TTrace traceId{requestId, 0};
    union {
        struct {
            ui32 Verbosity : 4;
            ui32 TimeToLive : 12;
        };
        ui32 Raw;
    } verbosityAndTtl;
    verbosityAndTtl.TimeToLive = 4095;
    verbosityAndTtl.Verbosity = 15;

    char out[sizeof(TTrace) + sizeof(ui64) + sizeof(ui32)];

    char* p = out;
    memcpy(p, traceId.data(), sizeof(traceId));
    p += sizeof(traceId);
    memcpy(p, &spanId, sizeof(spanId));
    p += sizeof(spanId);
    memcpy(p, &verbosityAndTtl.Raw, sizeof(verbosityAndTtl.Raw));
    p += sizeof(verbosityAndTtl.Raw);
    Y_DEBUG_ABORT_UNLESS(p - out == sizeof(TTrace) + sizeof(ui64) + sizeof(ui32));
    return {out};
}

}   // namespace NCloud
