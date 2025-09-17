#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/system/sysstat.h>

#include <type_traits>
#include <utility>

namespace NCloud::NFileStore::NVhost {

namespace {

////////////////////////////////////////////////////////////////////////////////

static constexpr int MODE0660 = S_IRGRP | S_IWGRP | S_IRUSR | S_IWUSR;

////////////////////////////////////////////////////////////////////////////////

#define VHOST_SERVICE_CONFIG(xxx)                                              \
    xxx(ServiceEndpoints,           TVector<NProto::TServiceEndpoint>,  {}    )\
    xxx(RootKeyringName,            TString,                "nfs"             )\
    xxx(EndpointsKeyringName,       TString,                "nfs-endpoints"   )\
    xxx(EndpointStorageType,                                                   \
        NCloud::NProto::EEndpointStorageType,                                  \
        NCloud::NProto::ENDPOINT_STORAGE_KEYRING                              )\
    xxx(EndpointStorageDir,         TString,                {}                )\
    xxx(SocketAccessMode,           ui32,                   MODE0660          )\
    xxx(EndpointStorageNotImplementedErrorIsFatal,  bool,   false             )\
                                                                               \
    xxx(HandleOpsQueuePath,                         TString,    ""            )\
    xxx(HandleOpsQueueSize,                         ui32,       1_GB          )\
    xxx(WriteBackCachePath,                         TString,    ""            )\
    xxx(WriteBackCacheCapacity,                     ui64,       1_GB          )\
    xxx(WriteBackCacheAutomaticFlushPeriod,                                    \
        TDuration,                                                             \
        TDuration::MilliSeconds(100)                                          )\
    xxx(WriteBackCacheFlushRetryPeriod,                                        \
        TDuration,                                                             \
        TDuration::MilliSeconds(100)                                          )\
    xxx(WriteBackCacheFlushMaxWriteRequestSize,     ui32,       1_MB          )\
    xxx(WriteBackCacheFlushMaxWriteRequestsCount,   ui32,       64            )\
    xxx(WriteBackCacheFlushMaxSumWriteRequestsSize, ui32,       32_MB         )\
    xxx(DirectoryHandlesStoragePath,                TString,    ""            )\
    xxx(DirectoryHandlesTableSize,                  ui64,       1000          )\
    xxx(DirectoryHandlesInitialDataSize,            ui64,       1_GB          )\
// VHOST_SERVICE_CONFIG

#define VHOST_SERVICE_DECLARE_CONFIG(name, type, value)                        \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// VHOST_SERVICE_DECLARE_CONFIG

VHOST_SERVICE_CONFIG(VHOST_SERVICE_DECLARE_CONFIG)

#undef VHOST_SERVICE_DECLARE_CONFIG

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

template <class TTarget, typename TSource>
TTarget ConvertValue(
    const google::protobuf::RepeatedPtrField<TSource>& value)
{
    TTarget v(Reserve(value.size()));
    for (const auto& x: value) {
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

template <typename T>
void DumpImpl(const TVector<T>& t, IOutputStream& os)
{
    for (const auto& v: t) {
        os << v;
    }
}

template <>
void DumpImpl(
    const NCloud::NProto::EEndpointStorageType& value,
    IOutputStream& os)
{
    switch (value) {
        case NCloud::NProto::ENDPOINT_STORAGE_DEFAULT:
            os << "ENDPOINT_STORAGE_DEFAULT";
            break;
        case NCloud::NProto::ENDPOINT_STORAGE_KEYRING:
            os << "ENDPOINT_STORAGE_KEYRING";
            break;
        case NCloud::NProto::ENDPOINT_STORAGE_FILE:
            os << "ENDPOINT_STORAGE_FILE";
            break;
        default:
            os << "(Unknown EEndpointStorageType value "
                << static_cast<int>(value)
                << ")";
            break;
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

VHOST_SERVICE_CONFIG(DECLARE_FIELD_CHECKER)

#undef DECLARE_FIELD_CHECKER

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define VHOST_CONFIG_GETTER(name, type, ...)                                  \
    type TVhostServiceConfig::Get##name() const                               \
    {                                                                         \
        const auto& value = ProtoConfig.Get##name();                          \
        return IsEmpty##name(ProtoConfig, value) ? Default##name              \
                                                 : ConvertValue<type>(value); \
    }                                                                         \
// VHOST_CONFIG_GETTER

VHOST_SERVICE_CONFIG(VHOST_CONFIG_GETTER)

#undef VHOST_CONFIG_GETTER

void TVhostServiceConfig::Dump(IOutputStream& out) const
{
#define VHOST_CONFIG_DUMP(name, ...)                                           \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// VHOST_CONFIG_DUMP

    VHOST_SERVICE_CONFIG(VHOST_CONFIG_DUMP);

#undef VHOST_CONFIG_DUMP
}

void TVhostServiceConfig::DumpHtml(IOutputStream& out) const
{
#define VHOST_CONFIG_DUMP(name, ...)                                           \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// VHOST_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                VHOST_SERVICE_CONFIG(VHOST_CONFIG_DUMP);
            }
        }
    }

#undef VHOST_CONFIG_DUMP
}

const NProto::TLocalServiceConfig* TVhostServiceConfig::GetLocalServiceConfig() const
{
    return ProtoConfig.HasLocalServiceConfig()
        ? &ProtoConfig.GetLocalServiceConfig()
        : nullptr;
}

#undef VHOST_SERVICE_CONFIG

}   // namespace NCloud::NFileStore::NVhost
