#include "config.h"

#include <cloud/storage/core/libs/common/proto_helpers.h>

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/size_literals.h>
#include <util/generic/hash.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CELL_DEFAULT_CONFIG(xxx)                                   \
    xxx(CellId,                     TString,                 {}               )\
    xxx(GrpcPort,                    ui32,                   0                )\
    xxx(NbdPort,                     ui32,                   {}               )\
    xxx(RdmaPort,                    ui32,                   0                )\
    xxx(SecureGrpcPort,              ui32,                   {}               )\
    xxx(CellDescribeHostCnt,        ui32,                    1                )\
    xxx(MinCellConnections,         ui32,                    1                )\
    xxx(Transport,                                                             \
        NProto::ECellDataTransport,                                            \
        NProto::CELL_DATA_TRANSPORT_GRPC                                      )\
// BLOCKSTORE_CELL_DEFAULT_CONFIG

#define BLOCKSTORE_CELL_DECLARE_CONFIG(name, type, value)                     \
    Y_DECLARE_UNUSED static const type CellDefault##name = value;             \
// BLOCKSTORE_CELL_DECLARE_CONFIG

BLOCKSTORE_CELL_DEFAULT_CONFIG(BLOCKSTORE_CELL_DECLARE_CONFIG)

#undef BLOCKSTORE_CELL_DECLARE_CONFIG

#define BLOCKSTORE_CELL_COMPUTED_CONFIG(xxx)                                  \
    xxx(Hosts,                     TConfiguredHosts                           )\
// BLOCKSTORE_CELL_COMPUTED_CONFIG

#define BLOCKSTORE_CELL_CONFIG(xxx)                                           \
    BLOCKSTORE_CELL_DEFAULT_CONFIG(xxx)                                       \
    BLOCKSTORE_CELL_COMPUTED_CONFIG(xxx)

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CELLING_DEFAULT_CONFIG(xxx)                                \
    xxx(CellId,                      TString,           {}                    )\
    xxx(DescribeTimeout,             TDuration,         TDuration::Seconds(30))\
    xxx(RdmaTransportWorkers,        ui32,              0                     )\
// BLOCKSTORE_SERVER_CONFIG

#define BLOCKSTORE_CELLING_DECLARE_CONFIG(name, type, value)                  \
    Y_DECLARE_UNUSED static const type CellsDefault##name = value;             \
// BLOCKSTORE_CELLING_DECLARE_CONFIG

BLOCKSTORE_CELLING_DEFAULT_CONFIG(BLOCKSTORE_CELLING_DECLARE_CONFIG)

#undef BLOCKSTORE_CELLING_DECLARE_CONFIG

#define BLOCKSTORE_CELLING_COMPUTED_CONFIG(xxx)                               \
    xxx(Cells,                     TConfiguredCells                           )\
// BLOCKSTORE_CELLING_COMPUTED_CONFIG

#define BLOCKSTORE_CELLING_CONFIG(xxx)                                        \
    BLOCKSTORE_CELLING_DEFAULT_CONFIG(xxx)                                    \
    BLOCKSTORE_CELLING_COMPUTED_CONFIG(xxx)

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
    const TConfiguredCells& value,
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
    const NProto::ECellDataTransport& value,
    IOutputStream& os)
{
    switch (value) {
        case NProto::CELL_DATA_TRANSPORT_UNSET:
            os << "CELL_DATA_TRANSPORT_UNSET";
            break;
        case NProto::CELL_DATA_TRANSPORT_GRPC:
            os << "CELL_DATA_TRANSPORT_GRPC";
            break;
        case NProto::CELL_DATA_TRANSPORT_NBD:
            os << "CELL_DATA_TRANSPORT_NBD";
            break;
        case NProto::CELL_DATA_TRANSPORT_RDMA:
            os << "CELL_DATA_TRANSPORT_RDMA";
            break;
        default:
            os << "(Unknown ECellDataTransport value "
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

TCellHostConfig::TCellHostConfig(
        NProto::TCellHostConfig hostConfig,
        TCellConfig cellConfig)
    : GrpcPort(cellConfig.GetGrpcPort())
    , SecureGrpcPort(cellConfig.GetSecureGrpcPort())
    , RdmaPort(cellConfig.GetRdmaPort())
    , NbdPort(cellConfig.GetNbdPort())
    , Fqdn(hostConfig.GetFqdn())
    , Transport(hostConfig.GetTransport() ?
        hostConfig.GetTransport():
        cellConfig.GetTransport())
{
}

////////////////////////////////////////////////////////////////////////////////

TCellConfig::TCellConfig(NProto::TCellConfig config)
    : Config(std::move(config))
{
    for (const auto& h: Config.GetHosts()) {
        ConfiguredHosts.emplace(h.GetFqdn(), TCellHostConfig(h, *this));
    }
}

const TConfiguredHosts& TCellConfig::GetHosts() const
{
    return ConfiguredHosts;
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TCellConfig::Get##name() const                                           \
{                                                                              \
    return NCloud::HasField(Config, #name)                                     \
                ? ConvertValue<type>(Config.Get##name())                       \
                : CellDefault##name;                                          \
}
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_CELL_DEFAULT_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TCellConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_CELL_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TCellConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_CELL_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

////////////////////////////////////////////////////////////////////////////////

TCellsConfig::TCellsConfig(NProto::TCellsConfig config)
    : Config(config)
    , GrpcClientConfig(CreateClientAppConfig(config.GetGrpcClientConfig()))
{
    for (const auto& s: Config.GetCells()) {
        ConfiguredCells.emplace(s.GetCellId(), s);
    }
}

const NClient::TClientAppConfig& TCellsConfig::GetGrpcClientConfig() const
{
    return GrpcClientConfig;
}

const TConfiguredCells& TCellsConfig::GetCells() const
{
    return ConfiguredCells;
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TCellsConfig::Get##name() const                                        \
{                                                                              \
    return NCloud::HasField(Config, #name)                                     \
                ? ConvertValue<type>(Config.Get##name())                       \
                : CellsDefault##name;                                       \
}
// BLOCKSTORE_CONFIG_GETTER

BLOCKSTORE_CELLING_DEFAULT_CONFIG(BLOCKSTORE_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TCellsConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CONFIG_DUMP(name, ...)                                      \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CONFIG_DUMP

    BLOCKSTORE_CELLING_CONFIG(BLOCKSTORE_CONFIG_DUMP);

#undef BLOCKSTORE_CONFIG_DUMP
}

void TCellsConfig::DumpHtml(IOutputStream& out) const
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
                BLOCKSTORE_CELLING_CONFIG(BLOCKSTORE_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore::NCells
