#pragma once

#include <cloud/storage/core/libs/diagnostics/trace_reader.h>
#include <cloud/storage/core/libs/opentelemetry/iface/public.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

ITraceReaderPtr CreateTraceExporter(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TString tag,
    ITraceReaderPtr consumer,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName);

ITraceReaderPtr SetupTraceReaderWithOpentelemetryExport(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    TString tag,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName,
    ELogPriority priority = ELogPriority::TLOG_INFO);

ITraceReaderPtr SetupTraceReaderForSlowRequestsWithOpentelemetryExport(
    TString id,
    ILoggingServicePtr logging,
    TString componentName,
    ITraceServiceClientPtr traceServiceClient,
    TString serviceName,
    TRequestThresholds requestThresholds);

} // namespace NCloud
