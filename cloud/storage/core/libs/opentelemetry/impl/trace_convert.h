#pragma once

#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/trace/v1/trace.pb.h>

#include <library/cpp/lwtrace/log.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

struct TTraceInfo
{
    ui64 RequestId = 0;
    TString DiskId;
    TVector<opentelemetry::proto::trace::v1::Span> Spans;
};

TTraceInfo ConvertToOpenTelemetrySpans(const NLWTrace::TTrackLog& tl);

}   // namespace NCloud
