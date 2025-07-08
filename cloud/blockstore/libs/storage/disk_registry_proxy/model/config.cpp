#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <chrono>

namespace NCloud::NBlockStore::NStorage {

using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DISK_REGISTRY_PROXY_CONFIG(xxx)                             \
    xxx(Owner,                  ui64,                 0                       )\
    xxx(OwnerIdx,               ui64,                 0                       )\
    xxx(LookupTimeout,          TDuration,            1min                    )\
    xxx(RetryLookupTimeout,     TDuration,            3s                      )\
    xxx(DiskRegistryTabletId,   ui64,                 0                       )\
// BLOCKSTORE_DISK_REGISTRY_PROXY_CONFIG

#define BLOCKSTORE_DECLARE_CONFIG(name, type, value)                           \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_DECLARE_CONFIG

BLOCKSTORE_DISK_REGISTRY_PROXY_CONFIG(BLOCKSTORE_DECLARE_CONFIG)

#undef BLOCKSTORE_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

template <>
TDuration ConvertValue<TDuration, ui32>(ui32 value)
{
    return TDuration::MilliSeconds(value);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TDiskRegistryProxyConfig::TDiskRegistryProxyConfig(
        NProto::TDiskRegistryProxyConfig config)
    : Config(std::move(config))
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TDiskRegistryProxyConfig::Get##name() const                               \
{                                                                              \
    const auto value = Config.Get##name();                                     \
    return value ? ConvertValue<type>(value) : Default##name;                  \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER
BLOCKSTORE_DISK_REGISTRY_PROXY_CONFIG(BLOCKSTORE_CONFIG_GETTER)
#undef BLOCKSTORE_CONFIG_GETTER
void TDiskRegistryProxyConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP
    BLOCKSTORE_DISK_REGISTRY_PROXY_CONFIG(BLOCKSTORE_CONFIG_DUMP);
#undef BLOCKSTORE_CONFIG_DUMP
}
void TDiskRegistryProxyConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_DISK_REGISTRY_PROXY_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }
#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore::NStorage
