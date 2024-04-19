#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NNotify {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_NOTIFY_CONFIG(xxx)                                          \
    xxx(Endpoint,       TString,            "")                                \
    xxx(CaCertFilename, TString,            "")                                \
    xxx(Version,        ui32,               1 )                                \

// BLOCKSTORE_NOTIFY_CONFIG

#define BLOCKSTORE_DECLARE_CONFIG(name, type, value)                           \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_DECLARE_CONFIG

BLOCKSTORE_NOTIFY_CONFIG(BLOCKSTORE_DECLARE_CONFIG)

#undef BLOCKSTORE_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TNotifyConfig::TNotifyConfig(
        NProto::TNotifyConfig config)
    : Config(std::move(config))
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TNotifyConfig::Get##name() const                                       \
{                                                                              \
    if (Config.Has##name()) {                                                  \
        const auto value = Config.Get##name();                                 \
        return ConvertValue<type>(value);                                      \
    }                                                                          \
    return Default##name;                                                      \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_NOTIFY_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TNotifyConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP
    BLOCKSTORE_NOTIFY_CONFIG(BLOCKSTORE_CONFIG_DUMP);
#undef BLOCKSTORE_CONFIG_DUMP
}

void TNotifyConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_NOTIFY_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }
#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore::NNotify
