#include <cloud/storage/core/libs/diagnostics/trace_reader.h>
#include <cloud/storage/core/libs/opentelemetry/iface/public.h>

namespace NCloud {

ITraceReaderPtr CreateTraceExporter(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName);

} // namespace NCloud
