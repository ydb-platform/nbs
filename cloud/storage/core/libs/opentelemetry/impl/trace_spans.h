#pragma once

#include <contrib/proto/opentelemetry/opentelemetry/proto/trace/v1/trace.pb.h>

#include <util/generic/vector.h>

namespace NCloud {

// Add only diagnostic stages whose begin and end probes belong to the same
// request and orbit. Keep the original events, including incomplete stages.
void AddDiagnosticSpans(TVector<opentelemetry::proto::trace::v1::Span>& spans);

}   // namespace NCloud
