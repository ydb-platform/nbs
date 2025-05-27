#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NSharding {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SHARD_CONFIG(xxx)                                           \
    xxx(ShardId,                     TString,                {}               )\
    xxx(GrpcPort,                    ui32,                   9766             )\
    xxx(NbdPort,                     ui32,                   {}               )\
    xxx(RdmaPort,                    ui32,                   10040            )\
    xxx(SecureGrpcPort,              ui32,                   {}               )\
    xxx(ShardDescribeHostCnt,        ui32,                   1                )\
    xxx(MinShardConnections,         ui32,                   1                )\
    xxx(Hosts,                       TVector<TString>,       {}               )\
    xxx(Transport,            NProto::EShardDataTransport,   NProto::GRPC     )\
    xxx(FixedHost,            TString,                       {}               )\
// BLOCKSTORE_SERVER_CONFIG

#define BLOCKSTORE_SHARD_DECLARE_CONFIG(name, type, value)                     \
    Y_DECLARE_UNUSED static const type ShardDefault##name = value;             \
// BLOCKSTORE_SHARD_DECLARE_CONFIG

BLOCKSTORE_SHARD_CONFIG(BLOCKSTORE_SHARD_DECLARE_CONFIG)

#undef BLOCKSTORE_SHARDING_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_SHARDING_CONFIG(xxx)                                        \
    xxx(Shards,                      TShards,                {}               )\
    xxx(ShardId,                     TString,                {}               )\
// BLOCKSTORE_SERVER_CONFIG

#define BLOCKSTORE_SHARDING_DECLARE_CONFIG(name, type, value)                  \
    Y_DECLARE_UNUSED static const type ShardingDefault##name = value;          \
// BLOCKSTORE_SHARDING_DECLARE_CONFIG

BLOCKSTORE_SHARDING_CONFIG(BLOCKSTORE_SHARDING_DECLARE_CONFIG)

#undef BLOCKSTORE_SHARDING_DECLARE_CONFIG

////////////////////////////////////////////////////////////////////////////////

template <typename TTarget, typename TSource>
TTarget ConvertValue(const TSource& value)
{
    return static_cast<TTarget>(value);
}

template <>
TVector<TString> ConvertValue(
    const google::protobuf::RepeatedPtrField<TString>& value)
{
    TVector<TString> v;
    for (const auto& item: value) {
        v.emplace_back(item);
    }
    return v;
}

template <>
TShards ConvertValue(
    const google::protobuf::RepeatedPtrField<NProto::TShardInfo>& value)
{
    TShards v;
    for (const auto& item: value) {
        v.emplace(item.GetShardId(), TShardConfig(item));
    }
    return v;
}

template <typename T>
void DumpImpl(const T& t, IOutputStream& os)
{
    os << t;
}

template <>
void DumpImpl(
    const TShards& value,
    IOutputStream& os)
{
    Y_UNUSED(value);
    Y_UNUSED(os);
}

template <>
void DumpImpl(const TVector<TString>& value, IOutputStream& os)
{
    for (size_t i = 0; i < value.size(); ++i) {
        if (i) {
            os << ",";
        }
        os << value[i];
    }
}

template <>
void DumpImpl(
    const NProto::EShardDataTransport& value,
    IOutputStream& os)
{
    switch (value) {
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TShardConfig::TShardConfig(NProto::TShardInfo config)
    : Config(std::move(config))
{
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TShardConfig::Get##name() const                                           \
{                                                                              \
    return NCloud::HasField(Config, #name)                                     \
                ? ConvertValue<type>(Config.Get##name())                       \
                : ShardDefault##name;                                          \
}
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_SHARD_CONFIG(BLOCKSTORE_CONFIG_GETTER)

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
    : Config(std::move(config))
{
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TShardingConfig::Get##name() const                                        \
{                                                                              \
    return NCloud::HasField(Config, #name)                                     \
                ? ConvertValue<type>(Config.Get##name())                       \
                : ShardingDefault##name;                                       \
}
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_SHARDING_CONFIG(BLOCKSTORE_CONFIG_GETTER)

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
