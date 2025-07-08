#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NLogbroker {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_LOGBROKER_CONFIG(xxx)                                       \
    xxx(Address,                  TString,      ""                            )\
    xxx(Port,                     ui32,         2135                          )\
    xxx(Database,                 TString,      ""                            )\
    xxx(UseLogbrokerCDS,          bool,         false                         )\
    xxx(CaCertFilename,           TString,      ""                            )\
    xxx(Topic,                    TString,      ""                            )\
    xxx(SourceId,                 TString,      ""                            )\
    xxx(MetadataServerAddress,    TString,      ""                            )\
    xxx(Protocol,                                                              \
        NProto::TLogbrokerConfig::EProtocol,                                   \
        NProto::TLogbrokerConfig::PROTOCOL_UNSPECIFIED                        )\
// BLOCKSTORE_LOGBROKER_CONFIG

#define BLOCKSTORE_DECLARE_CONFIG(name, type, value)                           \
    Y_DECLARE_UNUSED static const type Default##name = value;                  \
// BLOCKSTORE_DECLARE_CONFIG

BLOCKSTORE_LOGBROKER_CONFIG(BLOCKSTORE_DECLARE_CONFIG)

#undef BLOCKSTORE_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(TSource value)
{
    return static_cast<TTarget>(std::move(value));
}

IOutputStream& operator <<(
    IOutputStream& out,
    NProto::TLogbrokerConfig::EProtocol pt)
{
    const auto& s = NProto::TLogbrokerConfig::EProtocol_Name(pt);
    if (s.empty()) {
        return out << "(Unknown TLogbrokerConfig::EProtocol value "
                   << static_cast<int>(pt) << ")";
    }
    return out << s;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TLogbrokerConfig::TLogbrokerConfig(
        NProto::TLogbrokerConfig config)
    : Config(std::move(config))
{}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TLogbrokerConfig::Get##name() const                                       \
{                                                                              \
    if (Config.Has##name()) {                                                  \
        const auto value = Config.Get##name();                                 \
        return ConvertValue<type>(value);                                      \
    }                                                                          \
    return Default##name;                                                      \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_LOGBROKER_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TLogbrokerConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP
    BLOCKSTORE_LOGBROKER_CONFIG(BLOCKSTORE_CONFIG_DUMP);
#undef BLOCKSTORE_CONFIG_DUMP
}

void TLogbrokerConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_LOGBROKER_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }
#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore::NLogbroker
