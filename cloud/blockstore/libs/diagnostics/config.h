#pragma once

#include "public.h"

#include <cloud/blockstore/config/diagnostics.pb.h>

#include "cloud/storage/core/libs/diagnostics/histogram_counter_options.h"
#include <cloud/storage/core/libs/diagnostics/trace_reader.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TVolumePerfSettings:
    public TAtomicRefCount<TVolumePerfSettings>
{
    ui32 ReadIops = 0;
    ui64 ReadBandwidth = 0;

    ui32 WriteIops = 0;
    ui64 WriteBandwidth = 0;

    ui32 CriticalFactor = 0;

    TVolumePerfSettings() = default;
    TVolumePerfSettings(const TVolumePerfSettings& rhs) = default;

    TVolumePerfSettings(
            ui32 readIops,
            ui64 readBandwidth,
            ui32 writeIops,
            ui64 writeBandwidth,
            ui32 criticalFactor)
        : ReadIops(readIops)
        , ReadBandwidth(readBandwidth)
        , WriteIops(writeIops)
        , WriteBandwidth(writeBandwidth)
        , CriticalFactor(criticalFactor)
    {}

    TVolumePerfSettings(const NProto::TVolumePerfSettings& settings)
        : ReadIops(settings.GetRead().GetIops())
        , ReadBandwidth(settings.GetRead().GetBandwidth())
        , WriteIops(settings.GetWrite().GetIops())
        , WriteBandwidth(settings.GetWrite().GetBandwidth())
        , CriticalFactor(settings.GetCriticalFactor())
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
            && WriteBandwidth == rhs.WriteBandwidth
            && CriticalFactor == rhs.CriticalFactor;
    }

    bool operator != (const TVolumePerfSettings& rhs) const = default;
};

////////////////////////////////////////////////////////////////////////////////

struct TMonitoringUrlData: public TAtomicRefCount<TMonitoringUrlData>
{
    TString MonitoringClusterName;
    TString MonitoringUrl;
    TString MonitoringProject;
    TString MonitoringVolumeDashboard;
    TString MonitoringPartitionDashboard;
    TString MonitoringNBSAlertsDashboard;
    TString MonitoringNBSTVDashboard;
    TString MonitoringYDBProject;
    TString MonitoringYDBGroupDashboard;

    TMonitoringUrlData()
        : MonitoringProject("nbs")
    {}
    TMonitoringUrlData(const TMonitoringUrlData& rhs) = default;

    explicit TMonitoringUrlData(const NProto::TMonitoringUrlData& data)
        : MonitoringClusterName(data.GetMonitoringClusterName())
        , MonitoringUrl(data.GetMonitoringUrl())
        , MonitoringProject(data.GetMonitoringProject())
        , MonitoringVolumeDashboard(data.GetMonitoringVolumeDashboard())
        , MonitoringPartitionDashboard(data.GetMonitoringPartitionDashboard())
        , MonitoringNBSAlertsDashboard(data.GetMonitoringNBSAlertsDashboard())
        , MonitoringNBSTVDashboard(data.GetMonitoringNBSTVDashboard())
        , MonitoringYDBProject(data.GetMonitoringYDBProject())
        , MonitoringYDBGroupDashboard(data.GetMonitoringYDBGroupDashboard())
    {}
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
    ui32 GetKikimrMonPort() const;
    ui32 GetNbsMonPort() const;
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
    TVolumePerfSettings GetHddNonreplPerfSettings() const;
    TVolumePerfSettings GetMirror2PerfSettings() const;
    TVolumePerfSettings GetMirror3PerfSettings() const;
    TVolumePerfSettings GetLocalSSDPerfSettings() const;
    TVolumePerfSettings GetLocalHDDPerfSettings() const;
    ui32 GetExpectedIoParallelism() const;
    TVector<TString> GetCloudIdsWithStrictSLA() const;
    TMonitoringUrlData GetMonitoringUrlData() const;

    TString GetCpuWaitFilename() const;

    TDuration GetPostponeTimePredictorInterval() const;
    TDuration GetPostponeTimePredictorMaxTime() const;
    double GetPostponeTimePredictorPercentage() const;

    TDuration GetSSDDowntimeThreshold() const;
    TDuration GetHDDDowntimeThreshold() const;
    TDuration GetNonreplicatedSSDDowntimeThreshold() const;
    TDuration GetNonreplicatedHDDDowntimeThreshold() const;
    TDuration GetMirror3SSDDowntimeThreshold() const;
    TDuration GetMirror2SSDDowntimeThreshold() const;
    TDuration GetLocalSSDDowntimeThreshold() const;
    TDuration GetLocalHDDDowntimeThreshold() const;
    bool GetReportHistogramAsMultipleCounters() const;
    bool GetReportHistogramAsSingleCounter() const;

    TRequestThresholds GetRequestThresholds() const;
    EHistogramCounterOptions GetHistogramCounterOptions() const;

    NCloud::NProto::EStatsFetcherType GetStatsFetcherType() const;

    bool GetSkipReportingZeroBlocksMetricsForYDBBasedDisks() const;

    [[nodiscard]] NCloud::NProto::TOpentelemetryTraceConfig
    GetOpentelemetryTraceConfig() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

// Returns the DowntimeThreshold corresponding to the media kind.
TDuration GetDowntimeThreshold(
    const TDiagnosticsConfig& config,
    NCloud::NProto::EStorageMediaKind kind);

}   // namespace NCloud::NBlockStore
