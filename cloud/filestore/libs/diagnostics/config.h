#pragma once

#include "public.h"

#include <cloud/filestore/config/diagnostics.pb.h>
#include <cloud/storage/core/libs/diagnostics/trace_processor.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticsConfig
{
private:
    const NProto::TDiagnosticsConfig DiagnosticsConfig;

public:
    TDiagnosticsConfig(NProto::TDiagnosticsConfig diagnosticsConfig = {});

    TString GetBastionNameSuffix() const;

    TString GetSolomonClusterName() const;
    TString GetSolomonUrl() const;
    TString GetSolomonProject() const;

    ui32 GetFilestoreMonPort() const;

    TRequestThresholds GetRequestThresholds() const;
    ui32 GetSamplingRate() const;
    ui32 GetSlowRequestSamplingRate() const;
    TString GetTracesUnifiedAgentEndpoint() const;
    TString GetTracesSyslogIdentifier() const;

    TDuration GetProfileLogTimeThreshold() const;
    ui32 GetLWTraceShuttleCount() const;

    TString GetCpuWaitServiceName() const;

    TDuration GetMetricsUpdateInterval() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NFileStore
