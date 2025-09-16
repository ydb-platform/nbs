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

#define BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG(xxx)                          \
    xxx(IamEndpoint,                     TString,          ""                 )\
    xxx(JwtFilename,                     TString,          ""                 )\
// BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG

#define  BLOCKSTORE_DECLARE_CONFIG(name, type, value)                          \
    Y_DECLARE_UNUSED static const type DefaultIamJwtFile##name = value;        \
// BLOCKSTORE_DECLARE_CONFIG

BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG(BLOCKSTORE_DECLARE_CONFIG)

#undef BLOCKSTORE_DECLARE_CONFIG

#define BLOCKSTORE_LOGBROKER_IAM_METADATA_SERVER_CONFIG(xxx)                   \
    xxx(Endpoint,                        TString,          ""                 )\
// BLOCKSTORE_LOGBROKER_IAM_METADATA_SERVER_CONFIG

#define  BLOCKSTORE_DECLARE_CONFIG(name, type, value)                          \
    Y_DECLARE_UNUSED static const type DefaultIamMetadataServer##name = value; \
// BLOCKSTORE_DECLARE_CONFIG

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
    switch (pt) {
        case NProto::TLogbrokerConfig::PROTOCOL_UNSPECIFIED:
            return out << "PROTOCOL_UNSPECIFIED";
        case NProto::TLogbrokerConfig::PROTOCOL_PQ0:
            return out << "PROTOCOL_PQ0";
        case NProto::TLogbrokerConfig::PROTOCOL_TOPIC_API:
            return out << "PROTOCOL_TOPIC_API";
        default:
            return out
                << "(Unknown TLogbrokerConfig::EProtocol value "
                << static_cast<int>(pt)
                << ")";
    }
}

////////////////////////////////////////////////////////////////////////////////

#define CONFIG_ITEM_IS_SET_CHECKER(name, ...)                                  \
    template <typename TProto>                                                 \
    [[nodiscard]] bool Is##name##Set(const TProto& proto)                      \
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

#define BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(config, name, type, value) \
    (Is##name##Set(config) ? ConvertValue<type>(config.Get##name()) : value)

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

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TLogbrokerConfig::Get##name() const                                       \
{                                                                              \
    return BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(                                 \
        Config,                                                                \
        name,                                                                  \
        type,                                                                  \
        Default##name);                                                        \
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

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TIamJwtFile::Get##name() const                                            \
{                                                                              \
    return BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(                                 \
        Config,                                                                \
        name,                                                                  \
        type,                                                                  \
        DefaultIamJwtFile##name);                                              \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_LOGBROKER_IAM_JWT_FILE_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TIamMetadataServer::Get##name() const                                     \
{                                                                              \
    return BLOCKSTORE_CONFIG_GET_CONFIG_VALUE(                                 \
        Config,                                                                \
        name,                                                                  \
        type,                                                                  \
        DefaultIamMetadataServer##name);                                       \
}                                                                              \
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_LOGBROKER_IAM_METADATA_SERVER_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

}   // namespace NCloud::NBlockStore::NLogbroker
