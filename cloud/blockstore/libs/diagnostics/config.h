#pragma once

#include "public.h"

#include <cloud/blockstore/config/diagnostics.pb.h>

#include <cloud/storage/core/libs/diagnostics/trace_processor.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TVolumePerfSettings:
    public TAtomicRefCount<TVolumePerfSettings>
{
    ui32 ReadIops = 0;
    ui32 ReadBandwidth = 0;

    ui32 WriteIops = 0;
    ui32 WriteBandwidth = 0;

    TVolumePerfSettings() = default;
    TVolumePerfSettings(const TVolumePerfSettings& rhs) = default;

    TVolumePerfSettings(
            ui32 readIops,
            ui32 readBandwidth,
            ui32 writeIops,
            ui32 writeBandwidth)
        : ReadIops(readIops)
        , ReadBandwidth(readBandwidth)
        , WriteIops(writeIops)
        , WriteBandwidth(writeBandwidth)
    {}

    TVolumePerfSettings(const NProto::TVolumePerfSettings& settings)
        : ReadIops(settings.GetRead().GetIops())
        , ReadBandwidth(settings.GetRead().GetBandwidth())
        , WriteIops(settings.GetWrite().GetIops())
        , WriteBandwidth(settings.GetWrite().GetBandwidth())
    {}

    bool IsValid() const
    {
        return ReadIops != 0
            && ReadBandwidth != 0
            && WriteIops != 0
            && WriteBandwidth != 0;
    }

    bool operator == (const TVolumePerfSettings& rhs) const
    {
        return ReadIops == rhs.ReadIops
            && ReadBandwidth == rhs.ReadBandwidth
            && WriteIops == rhs.WriteIops
            && WriteBandwidth == rhs.WriteBandwidth;
    }

    bool operator != (const TVolumePerfSettings& rhs) const = default;
};

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticsConfig
{
private:
    const NProto::TDiagnosticsConfig DiagnosticsConfig;

public:
    TDiagnosticsConfig(NProto::TDiagnosticsConfig diagnosticsConfig = {});

    NProto::EHostNameScheme GetHostNameScheme() const;
    TString GetBastionNameSuffix() const;
    TString GetViewerHostName() const;
    TString GetSolomonClusterName() const;
    TString GetSolomonUrl() const;
    TString GetSolomonProject() const;
    ui32 GetKikimrMonPort() const;
    ui32 GetNbsMonPort() const;
    TDuration GetHDDSlowRequestThreshold() const;
    TDuration GetSSDSlowRequestThreshold() const;
    TDuration GetNonReplicatedSSDSlowRequestThreshold() const;
    ui32 GetSamplingRate() const;
    ui32 GetSlowRequestSamplingRate() const;
    TString GetTracesUnifiedAgentEndpoint() const;
    TString GetTracesSyslogIdentifier() const;
    TDuration GetProfileLogTimeThreshold() const;
    bool GetUseAsyncLogger() const;

    bool GetUnsafeLWTrace() const;
    TString GetLWTraceDebugInitializationQuery() const;
    ui32 GetLWTraceShuttleCount() const;

    TVolumePerfSettings GetSsdPerfSettings() const;
    TVolumePerfSettings GetHddPerfSettings() const;
    TVolumePerfSettings GetNonreplPerfSettings() const;
    TVolumePerfSettings GetMirror2PerfSettings() const;
    TVolumePerfSettings GetMirror3PerfSettings() const;
    TVolumePerfSettings GetLocalSSDPerfSettings() const;
    ui32 GetExpectedIoParallelism() const;

    TString GetCpuWaitFilename() const;

    TDuration GetPostponeTimePredictorInterval() const;
    TDuration GetPostponeTimePredictorMaxTime() const;
    double GetPostponeTimePredictorPercentage() const;

    TDuration GetSSDDowntimeThreshold() const;
    TDuration GetHDDDowntimeThreshold() const;
    TDuration GetNonreplicatedSSDDowntimeThreshold() const;
    TDuration GetMirror3SSDDowntimeThreshold() const;
    TDuration GetMirror2SSDDowntimeThreshold() const;
    TDuration GetLocalSSDDowntimeThreshold() const;
    TRequestThresholds GetRequestThresholds() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore
