#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/vector.h>

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
    xxx(NodeRegistrationToken,       TString,                   "root@builtin")\
    xxx(NodeType,                    TString,                   {}            )\
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

template <typename T>
bool IsEmpty(const T& t)
{
    return !t;
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_GETTER(name, type, ...)                               \
type TServerConfig::Get##name() const                                          \
{                                                                              \
    const auto& value = ProtoConfig.Get##name();                               \
    return !IsEmpty(value) ? ConvertValue<type>(value) : Default##name;        \
}                                                                              \
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

////////////////////////////////////////////////////////////////////////////////

TString GetCertFileFromConfig(const TServerConfigPtr& serverConfig)
{
    const auto& certs = serverConfig->GetCerts();
    if (certs.empty()) {
        return {};
    }
    return certs.front().CertFile;
}

TString GetCertPrivateKeyFileFromConfig(const TServerConfigPtr& serverConfig)
{
    const auto& certs = serverConfig->GetCerts();
    if (certs.empty()) {
        return {};
    }
    return certs.front().CertPrivateKeyFile;
}

}   // namespace NCloud::NFileStore::NServer
