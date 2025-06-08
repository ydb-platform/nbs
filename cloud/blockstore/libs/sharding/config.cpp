#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NSharding {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SHARD_DEFAULT_CONFIG(xxx)                                   \
    xxx(ShardId,                     TString,                {}               )\
    xxx(GrpcPort,                    ui32,                   0                )\
    xxx(NbdPort,                     ui32,                   {}               )\
    xxx(RdmaPort,                    ui32,                   0                )\
    xxx(SecureGrpcPort,              ui32,                   {}               )\
    xxx(ShardDescribeHostCnt,        ui32,                   1                )\
    xxx(MinShardConnections,         ui32,                   1                )\
    xxx(Transport,             NProto::EShardDataTransport,  NProto::GRPC     )\
    xxx(FixedHost,             TString,                      {}               )\
// BLOCKSTORE_SHARD_DEFAULT_CONFIG

#define BLOCKSTORE_SHARD_DECLARE_CONFIG(name, type, value)                     \
    Y_DECLARE_UNUSED static const type ShardDefault##name = value;             \
// BLOCKSTORE_SHARD_DECLARE_CONFIG

BLOCKSTORE_SHARD_DEFAULT_CONFIG(BLOCKSTORE_SHARD_DECLARE_CONFIG)

#undef BLOCKSTORE_SHARD_DECLARE_CONFIG

#define BLOCKSTORE_SHARD_COMPUTED_CONFIG(xxx)                                  \
    xxx(Hosts,                     TConfiguredHosts                           )\
// BLOCKSTORE_SHARD_COMPUTED_CONFIG

#define BLOCKSTORE_SHARD_CONFIG(xxx)                                           \
    BLOCKSTORE_SHARD_DEFAULT_CONFIG(xxx)                                       \
    BLOCKSTORE_SHARD_COMPUTED_CONFIG(xxx)

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SHARDING_DEFAULT_CONFIG(xxx)                                \
    xxx(ShardId,                     TString,           {}                    )\
    xxx(DescribeTimeout,             TDuration,         TDuration::Seconds(30))\
// BLOCKSTORE_SERVER_CONFIG

#define BLOCKSTORE_SHARDING_DECLARE_CONFIG(name, type, value)                  \
    Y_DECLARE_UNUSED static const type ShardingDefault##name = value;          \
// BLOCKSTORE_SHARDING_DECLARE_CONFIG

BLOCKSTORE_SHARDING_DEFAULT_CONFIG(BLOCKSTORE_SHARDING_DECLARE_CONFIG)

#undef BLOCKSTORE_SHARDING_DECLARE_CONFIG

#define BLOCKSTORE_SHARDING_COMPUTED_CONFIG(xxx)                               \
    xxx(Shards,                     TConfiguredShards                         )\
// BLOCKSTORE_SHARDING_COMPUTED_CONFIG

#define BLOCKSTORE_SHARDING_CONFIG(xxx)                                        \
    BLOCKSTORE_SHARDING_DEFAULT_CONFIG(xxx)                                    \
    BLOCKSTORE_SHARDING_COMPUTED_CONFIG(xxx)

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

template <>
void DumpImpl(
    const TConfiguredShards& value,
    IOutputStream& os)
{
    Y_UNUSED(value);
    Y_UNUSED(os);
}

template <>
void DumpImpl(
    const TConfiguredHosts& value,
    IOutputStream& os)
{
    Y_UNUSED(value);
    Y_UNUSED(os);
}

template <>
void DumpImpl(
    const NProto::EShardDataTransport& value,
    IOutputStream& os)
{
    switch (value) {
        case NProto::UNSET:
            os << "UNSET";
            break;
        case NProto::GRPC:
            os << "GRPC";
            break;
        case NProto::NBD:
            os << "NBD";
            break;
        case NProto::RDMA:
            os << "RDMA";
            break;
        default:
            os << "(Unknown ESuDataTransport value "
                << static_cast<int>(value)
                << ")";
            break;
    }
}

NProto::TClientAppConfig CreateClientAppConfig(
    const NProto::TClientConfig& config)
{
    NProto::TClientAppConfig proto;
    auto& clientConfig = *proto.MutableClientConfig();
    clientConfig = config;
    clientConfig.SetIsServerSideClient(true);
    clientConfig.SetNoClientId(true);
    return proto;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TShardHostConfig::TShardHostConfig(
        NProto::TShardHostConfig hostConfig,
        TShardConfig shardConfig)
    : GrpcPort(shardConfig.GetGrpcPort())
    , SecureGrpcPort(shardConfig.GetSecureGrpcPort())
    , RdmaPort(shardConfig.GetRdmaPort())
    , NbdPort(shardConfig.GetNbdPort())
    , Fqdn(hostConfig.GetFqdn())
    , Transport(hostConfig.GetTransport() ?
        hostConfig.GetTransport():
        shardConfig.GetTransport())
{
}

////////////////////////////////////////////////////////////////////////////////

TShardConfig::TShardConfig(NProto::TShardConfig config)
    : Config(std::move(config))
{
    for (const auto& h: Config.GetHosts()) {
        ConfiguredHosts.emplace(h.GetFqdn(), TShardHostConfig(h, *this));
    }
}

const TConfiguredHosts& TShardConfig::GetHosts() const
{
    return ConfiguredHosts;
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TShardConfig::Get##name() const                                           \
{                                                                              \
    return NCloud::HasField(Config, #name)                                     \
                ? ConvertValue<type>(Config.Get##name())                       \
                : ShardDefault##name;                                          \
}
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_SHARD_DEFAULT_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TShardConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_SHARD_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TShardConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// BLOCKSTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                BLOCKSTORE_SHARD_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

////////////////////////////////////////////////////////////////////////////////

TShardingConfig::TShardingConfig(NProto::TShardingConfig config)
    : Config(config)
    , GrpcClientConfig(CreateClientAppConfig(config.GetGrpcClientConfig()))
{
    for (const auto& s: Config.GetShards()) {
        ConfiguredShards.emplace(s.GetShardId(), s);
    }
}

const NClient::TClientAppConfig& TShardingConfig::GetGrpcClientConfig() const
{
    return GrpcClientConfig;
}

const TConfiguredShards& TShardingConfig::GetShards() const
{
    return ConfiguredShards;
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TShardingConfig::Get##name() const                                        \
{                                                                              \
    return NCloud::HasField(Config, #name)                                     \
                ? ConvertValue<type>(Config.Get##name())                       \
                : ShardingDefault##name;                                       \
}
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_SHARDING_DEFAULT_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TShardingConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_SHARDING_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TShardingConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// BLOCKSTORE_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                BLOCKSTORE_SHARDING_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore::NSharding
