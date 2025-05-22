#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/stream/str.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration AsyncHandleOpsPeriod = TDuration::MilliSeconds(50);

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SERVICE_CONFIG(xxx)                                          \
    xxx(RootPath,                    TString,       "./"                      )\
    xxx(PathPrefix,                  TString,       "nfs_"                    )\
    xxx(DefaultPermissions,          ui32,          0775                      )\
    xxx(IdleSessionTimeout,          TDuration,     TDuration::Seconds(30)    )\
    xxx(NumThreads,                  ui32,          8                         )\
    xxx(StatePath,                   TString,       "./"                      )\
    xxx(MaxNodeCount,                ui32,          1000000                   )\
    xxx(MaxHandlePerSessionCount,    ui32,          10000                     )\
    xxx(DirectIoEnabled,             bool,          false                     )\
    xxx(DirectIoAlign,               ui32,          4_KB                      )\
    xxx(GuestWriteBackCacheEnabled,  bool,          false                     )\
    xxx(AsyncDestroyHandleEnabled,   bool,          false                     )\
    xxx(AsyncHandleOperationPeriod,  TDuration,     AsyncHandleOpsPeriod      )\
    xxx(OpenNodeByHandleEnabled,     bool,          false                     )\
    xxx(NodeCleanupBatchSize,        ui32,          1000                      )\
    xxx(ZeroCopyEnabled,             bool,          false                     )\
    xxx(GuestPageCacheDisabled,      bool,          false                     )\
    xxx(ExtendedAttributesDisabled,  bool,          false                     )\
    xxx(ServerWriteBackCacheEnabled, bool,          false                     )\
    xxx(DontPopulateNodeCacheWhenListingNodes, bool, false                    )\
// FILESTORE_SERVICE_CONFIG

#define FILESTORE_SERVICE_DECLARE_CONFIG(name, type, value)                    \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// FILESTORE_SERVICE_DECLARE_CONFIG

FILESTORE_SERVICE_CONFIG(FILESTORE_SERVICE_DECLARE_CONFIG)

#undef FILESTORE_SERVICE_DECLARE_CONFIG

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

template <typename T>
bool IsEmpty(const T& t)
{
    return !t;
}

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_GETTER(name, type, ...)                               \
type TLocalFileStoreConfig::Get##name() const                                  \
{                                                                              \
    const auto value = ProtoConfig.Get##name();                                \
    return !IsEmpty(value) ? ConvertValue<type>(value) : Default##name;        \
}                                                                              \
// FILESTORE_CONFIG_GETTER

FILESTORE_SERVICE_CONFIG(FILESTORE_CONFIG_GETTER)

#undef FILESTORE_CONFIG_GETTER

void TLocalFileStoreConfig::Dump(IOutputStream& out) const
{
#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// FILESTORE_CONFIG_DUMP

    FILESTORE_SERVICE_CONFIG(FILESTORE_CONFIG_DUMP);

#undef FILESTORE_CONFIG_DUMP
}

TString TLocalFileStoreConfig::DumpStr() const
{
    TStringStream ss;
#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    ss << #name << ": ";                                                       \
    DumpImpl(Get##name(), ss);                                                 \
    ss << ", ";                                                                \
// FILESTORE_CONFIG_DUMP

    FILESTORE_SERVICE_CONFIG(FILESTORE_CONFIG_DUMP);

#undef FILESTORE_CONFIG_DUMP
    ss.TStringOutput::Undo(2);
    return ss.Str();
}

void TLocalFileStoreConfig::DumpHtml(IOutputStream& out) const
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
                FILESTORE_SERVICE_CONFIG(FILESTORE_CONFIG_DUMP);
            }
        }
    }

#undef FILESTORE_CONFIG_DUMP
}

}   // namespace NCloud::NFileStore
