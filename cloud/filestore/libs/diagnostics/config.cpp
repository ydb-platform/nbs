#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/protos/trace.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DIAGNOSTICS_CONFIG(xxx)                                      \
    xxx(BastionNameSuffix,  TString,    ""    )\
    xxx(FilestoreMonPort,   ui32,       8767                                  )\
                                                                               \
    xxx(SamplingRate,               ui32,       0                             )\
    xxx(SlowRequestSamplingRate,    ui32,       0                             )\
    xxx(TracesUnifiedAgentEndpoint, TString,    ""                            )\
    xxx(TracesSyslogIdentifier,     TString,    ""                            )\
                                                                               \
    xxx(ProfileLogTimeThreshold,    TDuration,  TDuration::Seconds(15)        )\
    xxx(LWTraceShuttleCount,        ui32,       2000                          )\
                                                                               \
    xxx(CpuWaitServiceName,         TString,    ""                            )\
    xxx(CpuWaitFilename,            TString,    ""                            )\
                                                                               \
    xxx(MetricsUpdateInterval,      TDuration,  TDuration::Seconds(5)         )\
                                                                               \
    xxx(SlowExecutionTimeRequestThreshold, TDuration, TDuration::Seconds(10)  )\
    xxx(SlowTotalTimeRequestThreshold,     TDuration, TDuration::Seconds(30)  )\
                                                                               \
    xxx(PostponeTimePredictorInterval,   TDuration, TDuration::Seconds(15)    )\
    xxx(PostponeTimePredictorMaxTime,    TDuration, TDuration::Minutes(1)     )\
    xxx(PostponeTimePredictorPercentage, double,    0.0                       )\
    xxx(MonitoringUrlData,               TMonitoringUrlData,  {}              )\
    xxx(ReportHistogramAsMultipleCounters,  bool,            true             )\
    xxx(ReportHistogramAsSingleCounter,     bool,            false            )\
                                                                               \
    xxx(HDDFileSystemPerformanceProfile,    TFileSystemPerformanceProfile, {} )\
    xxx(SSDFileSystemPerformanceProfile,    TFileSystemPerformanceProfile, {} )\
                                                                               \
    xxx(StatsFetcherType, NCloud::NProto::EStatsFetcherType, NCloud::NProto::EStatsFetcherType::CGROUP )\
                                                                               \
    xxx(ProfileLogMaxFlushRecords,      ui64, 0                               )\
    xxx(ProfileLogMaxFrameFlushRecords, ui64, 0                               )\

// FILESTORE_DIAGNOSTICS_CONFIG

#define FILESTORE_DIAGNOSTICS_DECLARE_CONFIG(name, type, value)                \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// FILESTORE_DIAGOSTICS_DECLARE_CONFIG

FILESTORE_DIAGNOSTICS_CONFIG(FILESTORE_DIAGNOSTICS_DECLARE_CONFIG)

#undef FILESTORE_DIAGNOSTICS_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return static_cast<TTarget>(value);
}

template <>
TDuration ConvertValue<TDuration, ui32>(const ui32& value)
{
    return TDuration::MilliSeconds(value);
}

template <>
TRequestThresholds
ConvertValue<TRequestThresholds, TProtoRequestThresholds>(
    const TProtoRequestThresholds& value)
{
    return ConvertRequestThresholds(value);
}

template <>
TMonitoringUrlData
ConvertValue<TMonitoringUrlData, NProto::TMonitoringUrlData>(
    const NProto::TMonitoringUrlData& value)
{
    return TMonitoringUrlData(value);
}

TRequestPerformanceProfile ConvertValue(
    const NProto::TRequestPerformanceProfile& value)
{
    return {value.GetRPS(), value.GetThroughput()};
}

template <>
TFileSystemPerformanceProfile ConvertValue<
    TFileSystemPerformanceProfile,
    NProto::TFileSystemPerformanceProfile>
(
    const NProto::TFileSystemPerformanceProfile& value)
{
    return {
        ConvertValue(value.GetRead()),
        ConvertValue(value.GetWrite()),
        ConvertValue(value.GetListNodes()),
        ConvertValue(value.GetGetNodeAttr()),
        ConvertValue(value.GetCreateHandle()),
        ConvertValue(value.GetDestroyHandle()),
        ConvertValue(value.GetCreateNode()),
        ConvertValue(value.GetRenameNode()),
        ConvertValue(value.GetUnlinkNode()),
        ConvertValue(value.GetStatFileStore())};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfig::TDiagnosticsConfig(NProto::TDiagnosticsConfig diagnosticsConfig)
    : DiagnosticsConfig(std::move(diagnosticsConfig))
{}

#define FILESTORE_CONFIG_GETTER(name, type, ...)                               \
type TDiagnosticsConfig::Get##name() const                                     \
{                                                                              \
    return NCloud::HasField(DiagnosticsConfig, #name)                          \
        ? ConvertValue<type>(DiagnosticsConfig.Get##name())                    \
        : Default##name;                                                       \
}                                                                              \
// FILESTORE_CONFIG_GETTER

FILESTORE_DIAGNOSTICS_CONFIG(FILESTORE_CONFIG_GETTER);

#undef FILESTORE_CONFIG_GETTER

TRequestThresholds TDiagnosticsConfig::GetRequestThresholds() const
{
    return ConvertValue<TRequestThresholds>(
        DiagnosticsConfig.GetRequestThresholds());
}

EHistogramCounterOptions TDiagnosticsConfig::GetHistogramCounterOptions() const
{
    EHistogramCounterOptions histogramCounterOptions;
    if (GetReportHistogramAsMultipleCounters()) {
        histogramCounterOptions |=
            EHistogramCounterOption::ReportMultipleCounters;
    }
    if (GetReportHistogramAsSingleCounter()) {
        histogramCounterOptions |= EHistogramCounterOption::ReportSingleCounter;
    }
    return histogramCounterOptions;
}

void TDiagnosticsConfig::Dump(IOutputStream& out) const
{
#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    out << #name << ": " << Get##name() << Endl;                               \
// FILESTORE_CONFIG_DUMP

    FILESTORE_DIAGNOSTICS_CONFIG(FILESTORE_CONFIG_DUMP);

#undef FILESTORE_CONFIG_DUMP
}

void TDiagnosticsConfig::DumpHtml(IOutputStream& out) const
{
#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { out << Get##name(); }                                       \
    }                                                                          \
// FILESTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                FILESTORE_DIAGNOSTICS_CONFIG(FILESTORE_CONFIG_DUMP);
            }
        }
    }

#undef FILESTORE_CONFIG_DUMP
}

}   // namespace NCloud::NFileStore

template <>
void Out<NCloud::TRequestThresholds>(
    IOutputStream& out,
    const NCloud::TRequestThresholds& value)
{
    OutRequestThresholds(out, value);
}

template <>
void Out<NCloud::NFileStore::TMonitoringUrlData>(
    IOutputStream& out,
    const NCloud::NFileStore::TMonitoringUrlData& value)
{
    NCloud::NFileStore::NProto::TMonitoringUrlData v;
    v.SetMonitoringClusterName(value.MonitoringClusterName);
    v.SetMonitoringUrl(value.MonitoringUrl);
    v.SetMonitoringProject(value.MonitoringProject);
    SerializeToTextFormat(v, out);
}

void ConvertFromValue(
    const NCloud::NFileStore::TRequestPerformanceProfile& v,
    NCloud::NFileStore::NProto::TRequestPerformanceProfile* p)
{
    p->SetRPS(v.RPS);
    p->SetThroughput(v.Throughput);
}

template <>
void Out<NCloud::NFileStore::TFileSystemPerformanceProfile>(
    IOutputStream& out,
    const NCloud::NFileStore::TFileSystemPerformanceProfile& value)
{
    NCloud::NFileStore::NProto::TFileSystemPerformanceProfile v;
    ConvertFromValue(value.Read, v.MutableRead());
    ConvertFromValue(value.Write, v.MutableWrite());
    ConvertFromValue(value.ListNodes, v.MutableListNodes());
    ConvertFromValue(value.GetNodeAttr, v.MutableGetNodeAttr());
    ConvertFromValue(value.CreateHandle, v.MutableCreateHandle());
    ConvertFromValue(value.DestroyHandle, v.MutableDestroyHandle());
    ConvertFromValue(value.CreateNode, v.MutableCreateNode());
    ConvertFromValue(value.RenameNode, v.MutableRenameNode());
    ConvertFromValue(value.UnlinkNode, v.MutableUnlinkNode());
    ConvertFromValue(value.StatFileStore, v.MutableStatFileStore());

    SerializeToTextFormat(v, out);
}

template <>
void Out<NCloud::NProto::EStatsFetcherType>(
    IOutputStream& out,
    NCloud::NProto::EStatsFetcherType statsFetcherType)
{
    out << NCloud::NProto::EStatsFetcherType_Name(
        statsFetcherType);
}
