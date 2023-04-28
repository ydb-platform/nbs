#include "config.h"

#include <cloud/storage/core/protos/trace.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DIAGNOSTICS_CONFIG(xxx)                                                               \
    xxx(HostNameScheme,                  NProto::EHostNameScheme, NProto::EHostNameScheme::HOSTNAME_RAW )\
    xxx(BastionNameSuffix,               TString,         ""                                            )\
    xxx(ViewerHostName,                  TString,         ""                                            )\
    xxx(SolomonClusterName,              TString,         ""                                            )\
    xxx(KikimrMonPort,                   ui32,            8765                                          )\
    xxx(NbsMonPort,                      ui32,            8766                                          )\
                                                                                                         \
    xxx(HDDSlowRequestThreshold,                TDuration,       TDuration::Seconds(0)                  )\
    xxx(SSDSlowRequestThreshold,                TDuration,       TDuration::Seconds(0)                  )\
    xxx(NonReplicatedSSDSlowRequestThreshold,   TDuration,       TDuration::MilliSeconds(10)            )\
    xxx(SamplingRate,                    ui32,            0                                             )\
    xxx(SlowRequestSamplingRate,         ui32,            0                                             )\
    xxx(TracesUnifiedAgentEndpoint,      TString,         ""                                            )\
    xxx(TracesSyslogIdentifier,          TString,         ""                                            )\
                                                                                                         \
    xxx(ProfileLogTimeThreshold,         TDuration,       TDuration::Seconds(15)                        )\
    xxx(UseAsyncLogger,                  bool,            false                                         )\
    xxx(SolomonUrl,                      TString,         ""                                            )\
    xxx(SolomonProject,                  TString,         "nbs"                                         )\
    xxx(UnsafeLWTrace,                   bool,            false                                         )\
    xxx(LWTraceDebugInitializationQuery, TString,         ""                                            )\
    xxx(SsdPerfSettings,                TVolumePerfSettings,  {}                                        )\
    xxx(HddPerfSettings,                TVolumePerfSettings,  {}                                        )\
    xxx(NonreplPerfSettings,            TVolumePerfSettings,  {}                                        )\
    xxx(Mirror2PerfSettings,            TVolumePerfSettings,  {}                                        )\
    xxx(Mirror3PerfSettings,            TVolumePerfSettings,  {}                                        )\
    xxx(LocalSSDPerfSettings,           TVolumePerfSettings,  {}                                        )\
    xxx(ExpectedIoParallelism,          ui32,                 32                                        )\
    xxx(LWTraceShuttleCount,            ui32,                 2000                                      )\
                                                                                                         \
    xxx(CpuWaitFilename,            TString, "/sys/fs/cgroup/cpu/system.slice/nbs.service/cpuacct.wait" )\
                                                                                                         \
    xxx(PostponeTimePredictorInterval,       TDuration,       TDuration::Seconds(5)                     )\
    xxx(PostponeTimePredictorMaxTime,        TDuration,       TDuration::Seconds(20)                    )\
    xxx(PostponeTimePredictorPercentage,     double,          0.5                                       )\
    xxx(SSDDowntimeThreshold,                TDuration,       TDuration::Seconds(5)                     )\
    xxx(HDDDowntimeThreshold,                TDuration,       TDuration::Seconds(15)                    )\
    xxx(NonreplicatedSSDDowntimeThreshold,   TDuration,       TDuration::Seconds(5)                     )\
    xxx(Mirror2SSDDowntimeThreshold,         TDuration,       TDuration::Seconds(5)                     )\
    xxx(Mirror3SSDDowntimeThreshold,         TDuration,       TDuration::Seconds(5)                     )\
    xxx(LocalSSDDowntimeThreshold,           TDuration,       TDuration::Seconds(5)                     )\
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
TRequestThresholds
ConvertValue<TRequestThresholds, TProtoRequestThresholds>(
    const TProtoRequestThresholds& value)
{
    return ConvertRequestThresholds(value);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfig::TDiagnosticsConfig(NProto::TDiagnosticsConfig diagnosticsConfig)
    : DiagnosticsConfig(std::move(diagnosticsConfig))
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                                    \
type TDiagnosticsConfig::Get##name() const                                           \
{                                                                                    \
    auto has = DiagnosticsConfig.Has##name();                                        \
    return has ? ConvertValue<type>(DiagnosticsConfig.Get##name()) : Default##name;  \
}                                                                                    \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_CONFIG_GETTER);

#undef BLOCKSTORE_CONFIG_GETTER

TRequestThresholds TDiagnosticsConfig::GetRequestThresholds() const
{
    return ConvertValue<TRequestThresholds>(
        DiagnosticsConfig.GetRequestThresholds());
}

void TDiagnosticsConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_DIAGNOSTICS_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TDiagnosticsConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { out << Get##name(); }                                       \
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

    SerializeToTextFormat(v, out);
}

template <>
void Out<NCloud::TRequestThresholds>(
    IOutputStream& out,
    const NCloud::TRequestThresholds& value)
{
    OutRequestThresholds(out, value);
}
