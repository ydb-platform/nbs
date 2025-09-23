#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/vector.h>

#include <type_traits>
#include <utility>

namespace NCloud::NFileStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration Seconds(int s)
{
    return TDuration::Seconds(s);
}

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SERVER_CONFIG(xxx)                                           \
    xxx(Host,                        TString,                   "localhost"   )\
    xxx(Port,                        ui32,                      9021          )\
    xxx(MaxMessageSize,              ui32,                      64*1024*1024  )\
    xxx(MemoryQuotaBytes,            ui32,                      0             )\
    xxx(PreparedRequestsCount,       ui32,                      10            )\
    xxx(ThreadsCount,                ui32,                      1             )\
    xxx(GrpcThreadsLimit,            ui32,                      4             )\
    xxx(KeepAliveEnabled,            bool,                      false         )\
    xxx(KeepAliveIdleTimeout,        TDuration,                 {}            )\
    xxx(KeepAliveProbeTimeout,       TDuration,                 {}            )\
    xxx(KeepAliveProbesCount,        ui32,                      0             )\
    xxx(ShutdownTimeout,             TDuration,                 Seconds(30)   )\
    xxx(SecureHost,                  TString,                   {}            )\
    xxx(SecurePort,                  ui32,                      0             )\
    xxx(RootCertsFile,               TString,                   {}            )\
    xxx(Certs,                       TVector<TCertificate>,     {}            )\
    xxx(UnixSocketPath,              TString,                   {}            )\
    xxx(UnixSocketBacklog,           ui32,                      16            )\
                                                                               \
    xxx(ActionsNoAuth,               TVector<TString>,          {}            )\
// FILESTORE_SERVER_CONFIG

#define FILESTORE_SERVER_DECLARE_CONFIG(name, type, value)                     \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// FILESTORE_SERVER_DECLARE_CONFIG

FILESTORE_SERVER_CONFIG(FILESTORE_SERVER_DECLARE_CONFIG)

#undef FILESTORE_SERVER_DECLARE_CONFIG

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
TVector<TCertificate> ConvertValue(
    const google::protobuf::RepeatedPtrField<NCloud::NProto::TCertificate>& value)
{
    TVector<TCertificate> v;
    for (const auto& x : value) {
        v.push_back({x.GetCertFile(), x.GetCertPrivateKeyFile()});
    }
    return v;
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
bool IsEmpty(const google::protobuf::RepeatedPtrField<T>& value)
{
    return value.empty();
}

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

template <>
void DumpImpl(const TVector<TCertificate>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os
          << "{ "
          << value[i].CertFile
          << ", "
          << value[i].CertPrivateKeyFile
          << " }";
    }
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

////////////////////////////////////////////////////////////////////////////////

#define DECLARE_FIELD_CHECKER(name, type, ...)                               \
    template <typename TProtoConfig, typename = void>                        \
    struct Has##name##Method: std::false_type                                \
    {                                                                        \
    };                                                                       \
                                                                             \
    template <typename TProtoConfig>                                         \
    struct Has##name##Method<                                                \
        TProtoConfig,                                                        \
        std::void_t<decltype(std::declval<TProtoConfig>().Has##name())>>     \
        : std::true_type                                                     \
    {                                                                        \
    };                                                                       \
                                                                             \
    template <typename TProtoConfig, typename TProtoValue>                   \
    bool IsEmpty##name(const TProtoConfig& config, const TProtoValue& value) \
    {                                                                        \
        if constexpr (Has##name##Method<TProtoConfig>::value) {              \
            return !config.Has##name();                                      \
        } else {                                                             \
            return IsEmpty(value);                                           \
        }                                                                    \
    }

FILESTORE_SERVER_CONFIG(DECLARE_FIELD_CHECKER)

#undef DECLARE_FIELD_CHECKER

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_GETTER(name, type, ...)                              \
    type TServerConfig::Get##name() const                                     \
    {                                                                         \
        const auto& value = ProtoConfig.Get##name();                          \
        return IsEmpty##name(ProtoConfig, value) ? Default##name              \
                                                 : ConvertValue<type>(value); \
    }                                                                         \
// FILESTORE_CONFIG_GETTER

FILESTORE_SERVER_CONFIG(FILESTORE_CONFIG_GETTER)

#undef FILESTORE_CONFIG_GETTER

void TServerConfig::Dump(IOutputStream& out) const
{
#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// FILESTORE_CONFIG_DUMP

    FILESTORE_SERVER_CONFIG(FILESTORE_CONFIG_DUMP);

#undef FILESTORE_CONFIG_DUMP
}

void TServerConfig::DumpHtml(IOutputStream& out) const
{
#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// FILESTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                FILESTORE_SERVER_CONFIG(FILESTORE_CONFIG_DUMP);
            }
        }
    }

#undef FILESTORE_CONFIG_DUMP
}

}   // namespace NCloud::NFileStore::NServer
