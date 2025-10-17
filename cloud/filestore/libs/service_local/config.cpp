#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/stream/str.h>

#include <chrono>

namespace NCloud::NFileStore {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_SERVICE_CONFIG(xxx)                                          \
    xxx(RootPath,                    TString,       "./"                      )\
    xxx(PathPrefix,                  TString,       "nfs_"                    )\
    xxx(DefaultPermissions,          ui32,          0775                      )\
    xxx(IdleSessionTimeout,          TDuration,     30s                       )\
    xxx(NumThreads,                  ui32,          8                         )\
    xxx(StatePath,                   TString,       "./"                      )\
    xxx(MaxNodeCount,                ui32,          1000000                   )\
    xxx(MaxHandlePerSessionCount,    ui32,          10000                     )\
    xxx(DirectIoEnabled,             bool,          false                     )\
    xxx(DirectIoAlign,               ui32,          4_KB                      )\
    xxx(GuestWriteBackCacheEnabled,  bool,          false                     )\
    xxx(AsyncDestroyHandleEnabled,   bool,          false                     )\
    xxx(AsyncHandleOperationPeriod,  TDuration,     50ms                      )\
    xxx(OpenNodeByHandleEnabled,     bool,          false                     )\
    xxx(NodeCleanupBatchSize,        ui32,          1000                      )\
    xxx(ZeroCopyEnabled,             bool,          false                     )\
    xxx(GuestPageCacheDisabled,      bool,          false                     )\
    xxx(ExtendedAttributesDisabled,  bool,          false                     )\
    xxx(ServerWriteBackCacheEnabled, bool,          false                     )\
    xxx(DontPopulateNodeCacheWhenListingNodes, bool, false                    )\
    xxx(GuestOnlyPermissionsCheckEnabled,      bool, false                    )\
    xxx(MaxResponseEntries,          ui32,          10000                     )\
    xxx(MaxBackground,               ui32,          0                         )\
    xxx(MaxFuseLoopThreads,          ui32,          1                         )\
    xxx(ZeroCopyWriteEnabled,        bool,          false                     )\
// FILESTORE_SERVICE_CONFIG

#define FILESTORE_SERVICE_NULL_FILE_IO_CONFIG(xxx)                             \
// FILESTORE_SERVICE_NULL_FILE_IO_CONFIG

#define FILESTORE_SERVICE_AIO_CONFIG(xxx)                                      \
    xxx(Entries,                     ui32,          1024                      )\
// FILESTORE_SERVICE_AIO_CONFIG

#define FILESTORE_SERVICE_IO_URING_CONFIG(xxx)                                 \
    xxx(Entries,                     ui32,          1024                      )\
    xxx(ShareKernelWorkers,          bool,          false                     )\
    xxx(MaxKernelWorkersCount,       ui32,          0                         )\
    xxx(ForceAsyncIO,                bool,          false                     )\
    xxx(SQKernelPollingEnabled,      bool,          false                     )\
// FILESTORE_SERVICE_IO_URING_CONFIG

#define FILESTORE_SERVICE_DECLARE_CONFIG(name, type, value)                    \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// FILESTORE_SERVICE_DECLARE_CONFIG

FILESTORE_SERVICE_CONFIG(FILESTORE_SERVICE_DECLARE_CONFIG)

#undef FILESTORE_SERVICE_DECLARE_CONFIG

#define FILESTORE_SERVICE_DECLARE_NULL_FILE_IO_CONFIG(name, type, value)       \
    Y_DECLARE_UNUSED static const type DefaultNullFileIO##name = value;        \
// FILESTORE_SERVICE_DECLARE_NULL_FILE_IO_CONFIG

FILESTORE_SERVICE_NULL_FILE_IO_CONFIG(FILESTORE_SERVICE_DECLARE_NULL_FILE_IO_CONFIG)

#define FILESTORE_SERVICE_DECLARE_AIO_CONFIG(name, type, value)                \
    Y_DECLARE_UNUSED static const type DefaultAio##name = value;               \
// FILESTORE_SERVICE_DECLARE_AIO_CONFIG

FILESTORE_SERVICE_AIO_CONFIG(FILESTORE_SERVICE_DECLARE_AIO_CONFIG)

#define FILESTORE_SERVICE_DECLARE_IO_URING_CONFIG(name, type, value)           \
    Y_DECLARE_UNUSED static const type DefaultIoUring##name = value;           \
// FILESTORE_SERVICE_DECLARE_IO_URING_CONFIG

FILESTORE_SERVICE_IO_URING_CONFIG(FILESTORE_SERVICE_DECLARE_IO_URING_CONFIG)

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
    if (ProtoConfig.Has##name()) {                                             \
        return ConvertValue<type>(ProtoConfig.Get##name());                    \
    }                                                                          \
    return Default##name;                                                      \
}                                                                              \
// FILESTORE_CONFIG_GETTER

FILESTORE_SERVICE_CONFIG(FILESTORE_CONFIG_GETTER)

#undef FILESTORE_CONFIG_GETTER

TFileIOConfig TLocalFileStoreConfig::GetFileIOConfig() const
{
    using EFileIOConfigCase = NProto::TLocalServiceConfig::FileIOConfigCase;

    switch (ProtoConfig.GetFileIOConfigCase()) {
        case EFileIOConfigCase::kAioConfig:
            return TAioConfig{ProtoConfig.GetAioConfig()};
        case EFileIOConfigCase::kIoUringConfig:
            return TIoUringConfig{ProtoConfig.GetIoUringConfig()};
        case EFileIOConfigCase::kNullFileIOConfig:
            return TNullFileIOConfig{ProtoConfig.GetNullFileIOConfig()};
        default:
            return TAioConfig{};
    }
}

////////////////////////////////////////////////////////////////////////////////

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

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_NULL_FILE_IO_GETTER(name, type, ...)                  \
type TNullFileIOConfig::Get##name() const                                      \
{                                                                              \
    if (Proto.Has##name()) {                                                   \
        return ConvertValue<type>(Proto.Get##name());                          \
    }                                                                          \
    return DefaultNullFileIO##name;                                            \
}                                                                              \
// FILESTORE_CONFIG_AIO_GETTER

FILESTORE_SERVICE_NULL_FILE_IO_CONFIG(FILESTORE_CONFIG_NULL_FILE_IO_GETTER)

#undef FILESTORE_CONFIG_NULL_FILE_IO_GETTER

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_AIO_GETTER(name, type, ...)                           \
type TAioConfig::Get##name() const                                             \
{                                                                              \
    if (Proto.Has##name()) {                                                   \
        return ConvertValue<type>(Proto.Get##name());                          \
    }                                                                          \
    return DefaultAio##name;                                                   \
}                                                                              \
// FILESTORE_CONFIG_AIO_GETTER

FILESTORE_SERVICE_AIO_CONFIG(FILESTORE_CONFIG_AIO_GETTER)

#undef FILESTORE_CONFIG_AIO_GETTER

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_IO_URING_GETTER(name, type, ...)                      \
type TIoUringConfig::Get##name() const                                         \
{                                                                              \
    if (Proto.Has##name()) {                                                   \
        return ConvertValue<type>(Proto.Get##name());                          \
    }                                                                          \
    return DefaultIoUring##name;                                               \
}                                                                              \
// FILESTORE_CONFIG_IO_URING_GETTER

FILESTORE_SERVICE_IO_URING_CONFIG(FILESTORE_CONFIG_IO_URING_GETTER)

#undef FILESTORE_CONFIG_IO_URING_GETTER

}   // namespace NCloud::NFileStore
