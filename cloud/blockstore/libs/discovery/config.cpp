#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NDiscovery {

namespace {

////////////////////////////////////////////////////////////////////////////////

TDuration Secs(ui32 x)
{
    return TDuration::Seconds(x);
}

TDuration Mins(ui32 x)
{
    return TDuration::Minutes(x);
}

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISCOVERY_CONFIG(xxx)                                       \
    xxx(ConductorApiUrl,                TString,          ""                  )\
    xxx(InstanceListFile,               TString,          ""                  )\
    xxx(BannedInstanceListFile,         TString,          ""                  )\
    xxx(ConductorRequestInterval,       TDuration,        Mins(5)             )\
    xxx(LocalFilesReloadInterval,       TDuration,        Secs(1)             )\
    xxx(HealthCheckInterval,            TDuration,        Secs(1)             )\
    xxx(ConductorGroups,                TVector<TString>, {}                  )\
    xxx(ConductorInstancePort,          ui32,             9766                )\
    xxx(ConductorSecureInstancePort,    ui32,             0                   )\
    xxx(ConductorRequestTimeout,        TDuration,        Secs(15)            )\
    xxx(PingRequestTimeout,             TDuration,        Secs(15)            )\
    xxx(MaxPingRequestsPerHealthCheck,  ui32,        20                       )\
// BLOCKSTORE_DISCOVERY_CONFIG

#define BLOCKSTORE_DISCOVERY_DECLARE_CONFIG(name, type, value)                 \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_DISCOVERY_DECLARE_CONFIG

BLOCKSTORE_DISCOVERY_CONFIG(BLOCKSTORE_DISCOVERY_DECLARE_CONFIG)

#undef BLOCKSTORE_DISCOVERY_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return TTarget(value);
}

template <>
TDuration ConvertValue<TDuration, ui32>(const ui32& value)
{
    return TDuration::MilliSeconds(value);
}

template <>
TVector<TString> ConvertValue(
    const google::protobuf::RepeatedPtrField<TString>& value)
{
    TVector<TString> v;
    for (const auto& x: value) {
        v.push_back(x);
    }
    return v;
}

template <typename T>
bool IsEmpty(const T& value)
{
    return !value;
}

template <>
bool IsEmpty(const google::protobuf::RepeatedPtrField<TString>& value)
{
    return value.empty();
}

template <typename T>
void DumpImpl(const T& value, IOutputStream& os)
{
    os << value;
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

TDiscoveryConfig::TDiscoveryConfig(NProto::TDiscoveryServiceConfig config)
    : Config(std::move(config))
{
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TDiscoveryConfig::Get##name() const                                       \
{                                                                              \
    const auto value = Config.Get##name();                                     \
    return IsEmpty(value) ? Default##name : ConvertValue<type>(value);         \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_DISCOVERY_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TDiscoveryConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_DISCOVERY_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TDiscoveryConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_DISCOVERY_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore::NDiscovery
