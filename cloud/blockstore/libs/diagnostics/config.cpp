#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>
#include <cloud/storage/core/protos/trace.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <chrono>

namespace NCloud::NBlockStore {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DIAGNOSTICS_CONFIG(xxx)                                     \
    xxx(HostNameScheme,     NProto::EHostNameScheme, NProto::HOSTNAME_RAW     )\
    xxx(BastionNameSuffix,               TString,         ""                  )\
    xxx(ViewerHostName,                  TString,         ""                  )\
    xxx(KikimrMonPort,                   ui32,            8765                )\
    xxx(NbsMonPort,                      ui32,            8766                )\
                                                                               \
    xxx(SamplingRate,                    ui32,            0                   )\
    xxx(SlowRequestSamplingRate,         ui32,            0                   )\
    xxx(TracesUnifiedAgentEndpoint,      TString,         ""                  )\
    xxx(TracesSyslogIdentifier,          TString,         ""                  )\
                                                                               \
    xxx(ProfileLogTimeThreshold,         TDuration,       15s                 )\
    xxx(UseAsyncLogger,                  bool,            false               )\
    xxx(UnsafeLWTrace,                   bool,            false               )\
    xxx(LWTraceDebugInitializationQuery, TString,         ""                  )\
    xxx(SsdPerfSettings,                TVolumePerfSettings,  {}              )\
    xxx(HddPerfSettings,                TVolumePerfSettings,  {}              )\
    xxx(NonreplPerfSettings,            TVolumePerfSettings,  {}              )\
    xxx(HddNonreplPerfSettings,         TVolumePerfSettings,  {}              )\
    xxx(Mirror2PerfSettings,            TVolumePerfSettings,  {}              )\
    xxx(Mirror3PerfSettings,            TVolumePerfSettings,  {}              )\
    xxx(LocalSSDPerfSettings,           TVolumePerfSettings,  {}              )\
    xxx(LocalHDDPerfSettings,           TVolumePerfSettings,  {}              )\
    xxx(ExpectedIoParallelism,          ui32,                 32              )\
    xxx(CloudIdsWithStrictSLA,          TVector<TString>,     {}              )\
    xxx(LWTraceShuttleCount,            ui32,                 2000            )\
    xxx(MonitoringUrlData,              TMonitoringUrlData,   {}              )\
                                                                               \
    xxx(CpuWaitFilename, TString, "/sys/fs/cgroup/cpu/system.slice/nbs.service/cpuacct.wait" )\
                                                                               \
    xxx(PostponeTimePredictorInterval,       TDuration,       5s              )\
    xxx(PostponeTimePredictorMaxTime,        TDuration,       20s             )\
    xxx(PostponeTimePredictorPercentage,     double,          0.5             )\
    xxx(SSDDowntimeThreshold,                TDuration,       5s              )\
    xxx(HDDDowntimeThreshold,                TDuration,       15s             )\
    xxx(NonreplicatedSSDDowntimeThreshold,   TDuration,       5s              )\
    xxx(NonreplicatedHDDDowntimeThreshold,   TDuration,       15s             )\
    xxx(Mirror2SSDDowntimeThreshold,         TDuration,       5s              )\
    xxx(Mirror3SSDDowntimeThreshold,         TDuration,       5s              )\
    xxx(LocalSSDDowntimeThreshold,           TDuration,       5s              )\
    xxx(LocalHDDDowntimeThreshold,           TDuration,       15s             )\
    xxx(ReportHistogramAsMultipleCounters,   bool,            true            )\
    xxx(ReportHistogramAsSingleCounter,      bool,            false           )\
    xxx(StatsFetcherType, NCloud::NProto::EStatsFetcherType, NCloud::NProto::CGROUP  )\
                                                                               \
    xxx(SkipReportingZeroBlocksMetricsForYDBBasedDisks, bool, false           )\
// BLOCKSTORE_DIAGNOSTICS_CONFIG

#define BLOCKSTORE_DIAGNOSTICS_DECLARE_CONFIG(name, type, value)               \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_DIAGOSTICS_DECLARE_CONFIG

BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_DIAGNOSTICS_DECLARE_CONFIG)

#undef BLOCKSTORE_DIAGNOSTICS_DECLARE_CONFIG

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
TVolumePerfSettings
ConvertValue<TVolumePerfSettings, NProto::TVolumePerfSettings>(
    const NProto::TVolumePerfSettings& value)
{
    return TVolumePerfSettings(value);
}

template <>
TMonitoringUrlData
ConvertValue<TMonitoringUrlData, NProto::TMonitoringUrlData>(
    const NProto::TMonitoringUrlData& value)
{
    return TMonitoringUrlData(value);
}

template <>
TRequestThresholds
ConvertValue<TRequestThresholds, TProtoRequestThresholds>(
    const TProtoRequestThresholds& value)
{
    return ConvertRequestThresholds(value);
}

template <>
TVector<TString> ConvertValue(
    const google::protobuf::RepeatedPtrField<TString>& value)
{
    TVector<TString> v;
    for (const auto& x : value) {
        v.push_back(x);
    }
    return v;
}

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

template <>
void DumpImpl(const TVector<TString>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os << value[i];
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfig::TDiagnosticsConfig(NProto::TDiagnosticsConfig diagnosticsConfig)
    : DiagnosticsConfig(std::move(diagnosticsConfig))
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TDiagnosticsConfig::Get##name() const                                     \
{                                                                              \
    return NCloud::HasField(DiagnosticsConfig, #name)                          \
        ? ConvertValue<type>(DiagnosticsConfig.Get##name())                    \
        : Default##name;                                                       \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_CONFIG_GETTER);

#undef BLOCKSTORE_CONFIG_GETTER

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
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TDiagnosticsConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// BLOCKSTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

TDuration GetDowntimeThreshold(
    const TDiagnosticsConfig& config,
    NCloud::NProto::EStorageMediaKind kind)
{
    switch (kind) {
        case NCloud::NProto::STORAGE_MEDIA_SSD: {
            return config.GetSSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED: {
            return config.GetNonreplicatedSSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED: {
            return config.GetNonreplicatedHDDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3: {
            return config.GetMirror3SSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2: {
            return config.GetMirror2SSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL: {
            return config.GetLocalSSDDowntimeThreshold();
        }
        case NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL: {
            return config.GetLocalHDDDowntimeThreshold();
        }
        default: {
            return config.GetHDDDowntimeThreshold();
        }
    }
}

}   // namespace NCloud::NBlockStore

////////////////////////////////////////////////////////////////////////////////

template <>
void Out<NCloud::NBlockStore::NProto::EHostNameScheme>(
    IOutputStream& out,
    NCloud::NBlockStore::NProto::EHostNameScheme scheme)
{
    out << NCloud::NBlockStore::NProto::EHostNameScheme_Name(scheme);
}

template <>
void Out<NCloud::NBlockStore::TVolumePerfSettings>(
    IOutputStream& out,
    const NCloud::NBlockStore::TVolumePerfSettings& value)
{
    NCloud::NBlockStore::NProto::TVolumePerfSettings v;
    v.MutableRead()->SetIops(value.ReadIops);
    v.MutableRead()->SetBandwidth(value.ReadBandwidth);
    v.MutableWrite()->SetIops(value.WriteIops);
    v.MutableWrite()->SetBandwidth(value.WriteBandwidth);
    v.SetCriticalFactor(value.CriticalFactor);

    SerializeToTextFormat(v, out);
}

template <>
void Out<NCloud::NBlockStore::TMonitoringUrlData>(
    IOutputStream& out,
    const NCloud::NBlockStore::TMonitoringUrlData& value)
{
    NCloud::NBlockStore::NProto::TMonitoringUrlData v;
    v.SetMonitoringClusterName(value.MonitoringClusterName);
    v.SetMonitoringUrl(value.MonitoringUrl);
    v.SetMonitoringProject(value.MonitoringProject);
    v.SetMonitoringVolumeDashboard(value.MonitoringVolumeDashboard);
    v.SetMonitoringPartitionDashboard(value.MonitoringPartitionDashboard);
    v.SetMonitoringNBSAlertsDashboard(value.MonitoringNBSAlertsDashboard);
    v.SetMonitoringNBSTVDashboard(value.MonitoringNBSTVDashboard);
    SerializeToTextFormat(v, out);
}

template <>
void Out<NCloud::TRequestThresholds>(
    IOutputStream& out,
    const NCloud::TRequestThresholds& value)
{
    OutRequestThresholds(out, value);
}

template <>
void Out<NCloud::NProto::EStatsFetcherType>(
    IOutputStream& out,
    NCloud::NProto::EStatsFetcherType statsFetcherType)
{
    out << NCloud::NProto::EStatsFetcherType_Name(
        statsFetcherType);
}
