

#include <contrib/libs/opentelemetry-proto/opentelemetry/proto/trace/v1/trace.pb.h>

#include <library/cpp/lwtrace/log.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

TVector<opentelemetry::proto::trace::v1::Span> ConvertToOpenTelemetrySpans(
    const NLWTrace::TTrackLog& tl);

}   // namespace NCloud
