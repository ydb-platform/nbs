#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/protobuf/util/pb_io.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NServer {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_LOCAL_STORAGE_CONFIG(xxx)                                   \
    xxx(Enabled,                              bool,          false            )\
    xxx(DirectIoDisabled,                     bool,          false            )\
    xxx(SingleQueue,                          bool,          false            )\
    xxx(Backend,                                                               \
        NProto::ELocalStorageBackend,                                          \
        NProto::LOCAL_STORAGE_BACKEND_AIO                                     )\
    xxx(SubmissionQueueThreadCount,           i32,             1              )\
    xxx(CompletionQueueThreadCount,           i32,             2              )\
// BLOCKSTORE_LOCAL_STORAGE_CONFIG

#define BLOCKSTORE_DECLARE_CONFIG(name, type, value)                           \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_DECLARE_CONFIG

BLOCKSTORE_LOCAL_STORAGE_CONFIG(BLOCKSTORE_DECLARE_CONFIG)

#undef BLOCKSTORE_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::ELocalStorageBackend pt)
{
    switch (pt) {
        case NProto::LOCAL_STORAGE_BACKEND_AIO:
            return out << "LOCAL_STORAGE_BACKEND_AIO";
        case NProto::LOCAL_STORAGE_BACKEND_NULL:
            return out << "LOCAL_STORAGE_BACKEND_NULL";
        default:
            return out
                << "(Unknown ELocalStorageBackend value "
                << static_cast<int>(pt)
                << ")";
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TLocalStorageConfig::Get##name() const                                       \
{                                                                              \
    if (Config.Has##name()) {                                                  \
        return ConvertValue<type>(Config.Get##name());                         \
    }                                                                          \
    return Default##name;                                                      \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_LOCAL_STORAGE_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TLocalStorageConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_LOCAL_STORAGE_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TLocalStorageConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_LOCAL_STORAGE_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

}   //  namespace NCloud::NBlockStore::NServer
