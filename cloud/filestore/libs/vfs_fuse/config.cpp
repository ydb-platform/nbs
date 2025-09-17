#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/system/compiler.h>

namespace NCloud::NFileStore::NFuse {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_FILESYSTEM_CONFIG(xxx)                                       \
    xxx(FileSystemId,           TString,        ""                            )\
    xxx(BlockSize,              ui32,           4_KB                          )\
                                                                               \
    xxx(LockRetryTimeout,       TDuration,      TDuration::Seconds(1)         )\
    xxx(EntryTimeout,           TDuration,      TDuration::Seconds(15)        )\
    xxx(NegativeEntryTimeout,   TDuration,      TDuration::Zero()             )\
    xxx(AttrTimeout,            TDuration,      TDuration::Seconds(15)        )\
                                                                               \
    xxx(XAttrCacheLimit,        ui32,           512                           )\
    xxx(XAttrCacheTimeout,      TDuration,      TDuration::Seconds(15)        )\
                                                                               \
    xxx(MaxBufferSize,          ui32,           4_MB                          )\
                                                                               \
    xxx(PreferredBlockSize,     ui32,           0                             )\
                                                                               \
    xxx(AsyncDestroyHandleEnabled,  bool,       false                         )\
    xxx(AsyncHandleOperationPeriod, TDuration,  TDuration::MilliSeconds(50)   )\
                                                                               \
    xxx(DirectIoEnabled,            bool,       false                         )\
    xxx(DirectIoAlign,              ui32,       4_KB                          )\
                                                                               \
    xxx(GuestWriteBackCacheEnabled, bool,       false                         )\
                                                                               \
    xxx(ZeroCopyEnabled,            bool,       false                         )\
    xxx(GuestPageCacheDisabled,     bool,       false                         )\
    xxx(ExtendedAttributesDisabled, bool,       false                         )\
                                                                               \
    xxx(ServerWriteBackCacheEnabled,  bool,     false                         )\
                                                                               \
    xxx(DirectoryHandlesStorageEnabled, bool,   false                         )\
                                                                               \
    xxx(GuestKeepCacheAllowed,        bool,     false                         )\
    xxx(MaxBackground,                ui32,     0                             )\
    xxx(MaxFuseLoopThreads,           ui32,     1                             )\
    xxx(ZeroCopyWriteEnabled,         bool,     false                         )\
    xxx(FSyncQueueDisabled,           bool,     false                         )\
// FILESTORE_FUSE_CONFIG

#define FILESTORE_FILESYSTEM_DECLARE_CONFIG(name, type, value)                 \
    Y_DECLARE_UNUSED static const type TFileSystemConfigDefault##name = value; \
// FILESTORE_FILESYSTEM_DECLARE_CONFIG

FILESTORE_FILESYSTEM_CONFIG(FILESTORE_FILESYSTEM_DECLARE_CONFIG)

#undef FILESTORE_FUSE_DECLARE_CONFIG

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
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_GETTER(class, name, type, ...)                        \
type class::Get##name() const                                                  \
{                                                                              \
    if (ProtoConfig.Has##name()) {                                             \
        return ConvertValue<type>(ProtoConfig.Get##name());                    \
    }                                                                          \
    return class##Default##name;                                               \
}                                                                              \
// FILESTORE_CONFIG_GETTER

#define FILESTORE_FS_GETTER(name, type, ...)                                   \
    FILESTORE_CONFIG_GETTER(TFileSystemConfig, name, type, ...)                \
// FILESTORE_FS_GETTER

FILESTORE_FILESYSTEM_CONFIG(FILESTORE_FS_GETTER)

#undef FILESTORE_CONFIG_GETTER
#undef FILESTORE_FS_GETTER

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// FILESTORE_CONFIG_DUMP

void TFileSystemConfig::Dump(IOutputStream& out) const
{
    FILESTORE_FILESYSTEM_CONFIG(FILESTORE_CONFIG_DUMP);
}

#undef FILESTORE_CONFIG_DUMP

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// FILESTORE_CONFIG_DUMP

void TFileSystemConfig::DumpHtml(IOutputStream& out) const
{
    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                FILESTORE_FILESYSTEM_CONFIG(FILESTORE_CONFIG_DUMP);
            }
        }
    }
}

#undef FILESTORE_CONFIG_DUMP

}   // namespace NCloud::NFileStore::NFuse
