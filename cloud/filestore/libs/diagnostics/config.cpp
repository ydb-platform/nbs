#include "config.h"

#include <cloud/storage/core/protos/trace.pb.h>

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_DIAGNOSTICS_CONFIG(xxx)                                      \
    xxx(BastionNameSuffix,  TString,    "ydb.bastion.cloud.yandex-team.ru"    )\
    xxx(SolomonClusterName, TString,    ""                                    )\
    xxx(SolomonUrl,         TString,    "https://solomon.yandex-team.ru"      )\
    xxx(SolomonProject,     TString,    "nfs"                                 )\
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
                                                                               \
    xxx(MetricsUpdateInterval,      TDuration,  TDuration::Seconds(5)         )\
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDiagnosticsConfig::TDiagnosticsConfig(NProto::TDiagnosticsConfig diagnosticsConfig)
    : DiagnosticsConfig(std::move(diagnosticsConfig))
{}

#define FILESTORE_CONFIG_GETTER(name, type, ...)                               \
type TDiagnosticsConfig::Get##name() const                                     \
{                                                                              \
    auto has = DiagnosticsConfig.Has##name();                                  \
    return has ?                                                               \
        ConvertValue<type>(DiagnosticsConfig.Get##name()) :                    \
        Default##name;                                                         \
}                                                                              \
// FILESTORE_CONFIG_GETTER

FILESTORE_DIAGNOSTICS_CONFIG(FILESTORE_CONFIG_GETTER);

#undef FILESTORE_CONFIG_GETTER

TRequestThresholds TDiagnosticsConfig::GetRequestThresholds() const
{
    return ConvertValue<TRequestThresholds>(
        DiagnosticsConfig.GetRequestThresholds());
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
