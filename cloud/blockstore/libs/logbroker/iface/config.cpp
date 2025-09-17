#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

namespace NCloud::NBlockStore::NLogbroker {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_LOGBROKER_CONFIG(xxx)                                       \
    xxx(LogbrokerConfig, Address,                  TString,      ""           )\
    xxx(LogbrokerConfig, Port,                     ui32,         2135         )\
    xxx(LogbrokerConfig, Database,                 TString,      ""           )\
    xxx(LogbrokerConfig, UseLogbrokerCDS,          bool,         false        )\
    xxx(LogbrokerConfig, CaCertFilename,           TString,      ""           )\
    xxx(LogbrokerConfig, Topic,                    TString,      ""           )\
    xxx(LogbrokerConfig, SourceId,                 TString,      ""           )\
    xxx(LogbrokerConfig, MetadataServerAddress,    TString,      ""           )\
    xxx(LogbrokerConfig, Protocol,                                             \
        NProto::TLogbrokerConfig::EProtocol,                                   \
        NProto::TLogbrokerConfig::PROTOCOL_UNSPECIFIED                        )\
// BLOCKSTORE_LOGBROKER_CONFIG

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG(xxx)                          \
    xxx(IamJwtFile, IamEndpoint,                     TString,          ""     )\
    xxx(IamJwtFile, JwtFilename,                     TString,          ""     )\
// BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_LOGBROKER_IAM_METADATA_SERVER_CONFIG(xxx)                   \
    xxx(IamMetadataServer, Endpoint,                 TString,          ""     )\
// BLOCKSTORE_LOGBROKER_IAM_METADATA_SERVER_CONFIG

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_DECLARE_CONFIG(ns, name, type, value)                       \
    Y_DECLARE_UNUSED static const type Default##ns##name = value;              \
// BLOCKSTORE_DECLARE_CONFIG

BLOCKSTORE_LOGBROKER_CONFIG(BLOCKSTORE_DECLARE_CONFIG)
BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG(BLOCKSTORE_DECLARE_CONFIG)
BLOCKSTORE_LOGBROKER_IAM_METADATA_SERVER_CONFIG(BLOCKSTORE_DECLARE_CONFIG)

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
    const TString& s = NProto::TLogbrokerConfig_EProtocol_Name(pt);
    if (s) {
        return out << s;
    }
    return out << "(Unknown TLogbrokerConfig::EProtocol value "
               << static_cast<int>(pt) << ")";
}

////////////////////////////////////////////////////////////////////////////////

#define CONFIG_ITEM_IS_SET_CHECKER(ns, name, ...)                              \
    template <typename TProto>                                                 \
    [[nodiscard]] bool Is##ns##name##Set(const TProto& proto)                  \
    {                                                                          \
        if constexpr (requires() { proto.name##Size(); }) {                    \
            return proto.name##Size() > 0;                                     \
        } else {                                                               \
            return proto.Has##name();                                          \
        }                                                                      \
    }

BLOCKSTORE_LOGBROKER_CONFIG(CONFIG_ITEM_IS_SET_CHECKER);
BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG(CONFIG_ITEM_IS_SET_CHECKER);
BLOCKSTORE_LOGBROKER_IAM_METADATA_SERVER_CONFIG(CONFIG_ITEM_IS_SET_CHECKER);

#undef CONFIG_ITEM_IS_SET_CHECKER

#define BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(ns, config, name, type, value)      \
    (Is##ns##name##Set(config) ? ConvertValue<type>(config.Get##name()) : value)

////////////////////////////////////////////////////////////////////////////////

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TLogbrokerConfig::TLogbrokerConfig(
        NProto::TLogbrokerConfig config)
    : Config(std::move(config))
{}

TAuthConfig TLogbrokerConfig::GetAuthConfig() const
{
    if (GetMetadataServerAddress()) {
        NProto::TLogbrokerConfig::TIamMetadataServer config;
        config.SetEndpoint(GetMetadataServerAddress());
        return TIamMetadataServer{std::move(config)};
    }

    using EAuthConfigCase = NProto::TLogbrokerConfig::AuthConfigCase;

    switch (Config.GetAuthConfigCase()) {
        case EAuthConfigCase::kIamJwtFile:
            return TIamJwtFile{Config.GetIamJwtFile()};
        case EAuthConfigCase::kIamMetadataServer:
            return TIamMetadataServer{Config.GetIamMetadataServer()};
        default:
            return std::monostate();
    }
}

////////////////////////////////////////////////////////////////////////////////

void TLogbrokerConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(ns, name, ...)                                  \
    out << #name << ": " << Get##name() << Endl;                               \
// BLOCKSTORE_CONFIG_DUMP
    BLOCKSTORE_LOGBROKER_CONFIG(BLOCKSTORE_CONFIG_DUMP);
#undef BLOCKSTORE_CONFIG_DUMP
}

void TLogbrokerConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(ns, name, ...)                                  \
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

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CONFIG_GETTER(ns, name, type, ...)                          \
type T##ns::Get##name() const                                                  \
{                                                                              \
    return BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(                                 \
        ns,                                                                    \
        Config,                                                                \
        name,                                                                  \
        type,                                                                  \
        Default##ns##name);                                                    \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_LOGBROKER_CONFIG(BLOCKSTORE_CONFIG_GETTER)
BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG(BLOCKSTORE_CONFIG_GETTER)
BLOCKSTORE_LOGBROKER_IAM_METADATA_SERVER_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

}   // namespace NCloud::NBlockStore::NLogbroker
