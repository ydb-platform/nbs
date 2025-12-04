#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_LOCAL_NVME_CONFIG(xxx)                                      \
    xxx(NVMeDevicesCacheFile,               TString,                        "")\

// BLOCKSTORE_LOCAL_NVME_CONFIG

#define BLOCKSTORE_DECLARE_CONFIG(name, type, value)                           \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_DECLARE_CONFIG

BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_DECLARE_CONFIG)

#undef BLOCKSTORE_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TLocalNVMeConfig::TLocalNVMeConfig(
        NProto::TLocalNVMeConfig config)
    : Config(std::move(config))
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TLocalNVMeConfig::Get##name() const                                \
{                                                                              \
    if (Config.Has##name()) {                                                  \
        const auto value = Config.Get##name();                                 \
        return ConvertValue<type>(value);                                      \
    }                                                                          \
    return Default##name;                                                      \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TLocalNVMeConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP
    BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_DUMP);
#undef BLOCKSTORE_CONFIG_DUMP
}

void TLocalNVMeConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_LOCAL_NVME_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }
#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore
