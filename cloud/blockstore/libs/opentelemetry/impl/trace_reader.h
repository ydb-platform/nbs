#include <cloud/blockstore/libs/opentelemetry/iface/public.h>

#include <cloud/storage/core/libs/diagnostics/trace_reader.h>

namespace NCloud::NBlockStore {

ITraceReaderPtr CreateTraceExporter(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName);

} // namespace NCloud::NBlockStore
