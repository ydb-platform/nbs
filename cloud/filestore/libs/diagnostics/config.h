#pragma once

#include "public.h"

#include <cloud/filestore/config/diagnostics.pb.h>
#include "cloud/storage/core/libs/diagnostics/histogram_counter_options.h"
#include <cloud/storage/core/libs/diagnostics/trace_reader.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

struct TRequestPerformanceProfile
{
    ui64 RPS = 0;
    ui64 Throughput = 0;

    TRequestPerformanceProfile() = default;

    TRequestPerformanceProfile(ui64 rps, ui64 throughput)
        : RPS(rps)
        , Throughput(throughput)
    {}

    TRequestPerformanceProfile(const TRequestPerformanceProfile& rhs) = default;
    TRequestPerformanceProfile& operator=(
        const TRequestPerformanceProfile& rhs) = default;
};

struct TFileSystemPerformanceProfile
{
    TRequestPerformanceProfile Read;
    TRequestPerformanceProfile Write;
    TRequestPerformanceProfile ListNodes;
    TRequestPerformanceProfile GetNodeAttr;
    TRequestPerformanceProfile CreateHandle;
    TRequestPerformanceProfile DestroyHandle;
    TRequestPerformanceProfile CreateNode;
    TRequestPerformanceProfile RenameNode;
    TRequestPerformanceProfile UnlinkNode;
    TRequestPerformanceProfile StatFileStore;

    TFileSystemPerformanceProfile() = default;

    TFileSystemPerformanceProfile(
            TRequestPerformanceProfile read,
            TRequestPerformanceProfile write,
            TRequestPerformanceProfile listNodes,
            TRequestPerformanceProfile getNodeAttr,
            TRequestPerformanceProfile createHandle,
            TRequestPerformanceProfile destroyHandle,
            TRequestPerformanceProfile createNode,
            TRequestPerformanceProfile renameNode,
            TRequestPerformanceProfile unlinkNode,
            TRequestPerformanceProfile statFileStore)
        : Read(read)
        , Write(write)
        , ListNodes(listNodes)
        , GetNodeAttr(getNodeAttr)
        , CreateHandle(createHandle)
        , DestroyHandle(destroyHandle)
        , CreateNode(createNode)
        , RenameNode(renameNode)
        , UnlinkNode(unlinkNode)
        , StatFileStore(statFileStore)
    {}

    TFileSystemPerformanceProfile(
        const TFileSystemPerformanceProfile& rhs) = default;
    TFileSystemPerformanceProfile& operator=(
        const TFileSystemPerformanceProfile& rhs) = default;
};

////////////////////////////////////////////////////////////////////////////////

struct TMonitoringUrlData: public TAtomicRefCount<TMonitoringUrlData>
{
    TString MonitoringClusterName;
    TString MonitoringUrl;
    TString MonitoringProject;

    TMonitoringUrlData() : MonitoringProject("nfs") {}
    TMonitoringUrlData(const TMonitoringUrlData& rhs) = default;

    TMonitoringUrlData(const NProto::TMonitoringUrlData& data)
        : MonitoringClusterName(data.GetMonitoringClusterName())
        , MonitoringUrl(data.GetMonitoringUrl())
        , MonitoringProject(data.GetMonitoringProject())
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TDiagnosticsConfig
{
private:
    const NProto::TDiagnosticsConfig DiagnosticsConfig;

public:
    TDiagnosticsConfig(NProto::TDiagnosticsConfig diagnosticsConfig = {});

    TString GetBastionNameSuffix() const;

    ui32 GetFilestoreMonPort() const;

    TRequestThresholds GetRequestThresholds() const;
    ui32 GetSamplingRate() const;
    ui32 GetSlowRequestSamplingRate() const;
    TString GetTracesUnifiedAgentEndpoint() const;
    TString GetTracesSyslogIdentifier() const;

    TDuration GetProfileLogTimeThreshold() const;
    ui32 GetLWTraceShuttleCount() const;

    TString GetCpuWaitServiceName() const;
    TString GetCpuWaitFilename() const;

    TDuration GetMetricsUpdateInterval() const;

    TDuration GetSlowExecutionTimeRequestThreshold() const;
    TDuration GetSlowTotalTimeRequestThreshold() const;

    TDuration GetPostponeTimePredictorInterval() const;
    TDuration GetPostponeTimePredictorMaxTime() const;
    double GetPostponeTimePredictorPercentage() const;

    TMonitoringUrlData GetMonitoringUrlData() const;

    bool GetReportHistogramAsMultipleCounters() const;
    bool GetReportHistogramAsSingleCounter() const;
    EHistogramCounterOptions GetHistogramCounterOptions() const;

    TFileSystemPerformanceProfile GetHDDFileSystemPerformanceProfile() const;
    TFileSystemPerformanceProfile GetSSDFileSystemPerformanceProfile() const;

    NCloud::NProto::EStatsFetcherType GetStatsFetcherType() const;

    ui64 GetProfileLogMaxFlushRecords() const;
    ui64 GetProfileLogMaxFrameFlushRecords() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NFileStore
