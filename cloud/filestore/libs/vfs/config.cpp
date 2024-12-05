#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/system/compiler.h>

namespace NCloud::NFileStore::NVFS {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_VFS_CONFIG(xxx)                                              \
    xxx(FileSystemId,           TString,        ""                            )\
    xxx(ClientId,               TString,        ""                            )\
                                                                               \
    xxx(SocketPath,             TString,        ""                            )\
    xxx(MountPath,              TString,        ""                            )\
    xxx(ReadOnly,               bool,           false                         )\
    xxx(Debug,                  bool,           false                         )\
                                                                               \
    xxx(MaxWritePages,          ui32,           256                           )\
    xxx(MaxBackground,          ui32,           128                           )\
    xxx(MountSeqNumber,         ui64,           0                             )\
    xxx(VhostQueuesCount,       ui32,           0                             )\
                                                                               \
    xxx(HandleOpsQueuePath,     TString,        ""                            )\
    xxx(HandleOpsQueueSize,     ui32,           1_GB                          )\
// FILESTORE_VFS_CONFIG

#define FILESTORE_VFS_DECLARE_CONFIG(name, type, value)                        \
    Y_DECLARE_UNUSED static const type TVFSConfigDefault##name = value;        \
// FILESTORE_VFS_DECLARE_CONFIG

FILESTORE_VFS_CONFIG(FILESTORE_VFS_DECLARE_CONFIG)

#undef FILESTORE_VFS_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return static_cast<TTarget>(value);
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

#define FILESTORE_CONFIG_GETTER(class, name, type, ...)                        \
type class::Get##name() const                                                  \
{                                                                              \
    const auto value = ProtoConfig.Get##name();                                \
    return !IsEmpty(value) ? ConvertValue<type>(value) : class##Default##name; \
}                                                                              \
// FILESTORE_CONFIG_GETTER

#define FILESTORE_VFS_GETTER(name, type, ...)                                  \
    FILESTORE_CONFIG_GETTER(TVFSConfig, name, type, ...)                       \
// FILESTORE_VFS_GETTER

FILESTORE_VFS_CONFIG(FILESTORE_VFS_GETTER)

#undef FILESTORE_CONFIG_GETTER
#undef FILESTORE_VFS_GETTER

////////////////////////////////////////////////////////////////////////////////

#define FILESTORE_CONFIG_DUMP(name, ...)                                       \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// FILESTORE_CONFIG_DUMP

void TVFSConfig::Dump(IOutputStream& out) const
{
    FILESTORE_VFS_CONFIG(FILESTORE_CONFIG_DUMP);
}

#undef FILESTORE_CONFIG_DUMP

}   // namespace NCloud::NFileStore::NVFS
