#include "config.h"

#include <library/cpp/monlib/service/pages/templates.h>

#include <util/generic/hash.h>
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NCells {

namespace {

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CELL_DEFAULT_CONFIG(xxx)                                    \
    xxx(CellId,                      TString,                {}               )\
    xxx(GrpcPort,                    ui32,                   0                )\
    xxx(NbdPort,                     ui32,                   {}               )\
    xxx(RdmaPort,                    ui32,                   0                )\
    xxx(SecureGrpcPort,              ui32,                   {}               )\
    xxx(DescribeVolumeHostCount,     ui32,                   1                )\
    xxx(MinCellConnections,          ui32,                   1                )\
    xxx(Transport,                                                             \
        NProto::ECellDataTransport,                                            \
        NProto::CELL_DATA_TRANSPORT_GRPC                                      )\
    xxx(StrictCellIdCheckInDescribeVolume,   bool,           false            )\
                                                                               \
    xxx(HeartbeatFailCount,          ui32,           5                        )\
    xxx(HeartbeatTimeout,            TDuration,      TDuration::Seconds(1)    )\
    xxx(HeartbeatInterval,           TDuration,      TDuration::Seconds(1)    )\

// BLOCKSTORE_CELL_DEFAULT_CONFIG

#define BLOCKSTORE_CELL_DECLARE_CONFIG(name, type, value)                      \
    inline static const type CellDefault##name = value;                        \
// BLOCKSTORE_CELL_DECLARE_CONFIG

#define BLOCKSTORE_CELL_COMPUTED_CONFIG(xxx)                                   \
    xxx(Hosts,                     TCellConfigByCellId                        )\
// BLOCKSTORE_CELL_COMPUTED_CONFIG

#define BLOCKSTORE_CELL_CONFIG(xxx)                                            \
    BLOCKSTORE_CELL_DEFAULT_CONFIG(xxx)                                        \
    BLOCKSTORE_CELL_COMPUTED_CONFIG(xxx)

////////////////////////////////////////////////////////////////////////////////

#define BLOCKSTORE_CELLS_DEFAULT_CONFIG(xxx)                                   \
    xxx(CellId,                      TString,           {}                    )\
    xxx(DescribeVolumeTimeout,       TDuration,         TDuration::Seconds(30))\
    xxx(RdmaTransportWorkers,        ui32,              0                     )\
    xxx(CellsEnabled,                bool,              false                 )\
// BLOCKSTORE_CELLS_DEFAULT_CONFIG

#define BLOCKSTORE_CELLS_DECLARE_CONFIG(name, type, value)                     \
    inline static const type CellsDefault##name = value;                       \
// BLOCKSTORE_CELL_DECLARE_CONFIG

#define BLOCKSTORE_CELLS_COMPUTED_CONFIG(xxx)                                  \
    xxx(Cells,                     TCellConfigByCellId                        )\
// BLOCKSTORE_CELLS_COMPUTED_CONFIG

#define BLOCKSTORE_CELLS_CONFIG(xxx)                                           \
    BLOCKSTORE_CELLS_DEFAULT_CONFIG(xxx)                                       \
    BLOCKSTORE_CELLS_COMPUTED_CONFIG(xxx)
// BLOCKSTORE_CELLS_CONFIG

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
// CONFIG_ITEM_IS_SET_CHECKER

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
void DumpImpl(const TCellConfigByCellId& value, IOutputStream& os)
{
    Y_UNUSED(value);
    Y_UNUSED(os);
}

template <>
void DumpImpl(const TConfiguredHostsByFqdn& value, IOutputStream& os)
{
    Y_UNUSED(value);
    Y_UNUSED(os);
}

template <>
void DumpImpl(
    const NProto::ECellDataTransport& value,
    IOutputStream& os)
{
    os << NProto::ECellDataTransport_Name(value);
}

NProto::TClientAppConfig CreateClientAppConfig(
    const NProto::TClientConfig& config)
{
    NProto::TClientAppConfig proto;
    auto& clientConfig = *proto.MutableClientConfig();
    clientConfig = config;
    return proto;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TCellHostConfig::TCellHostConfig(
        const NProto::TCellHostConfig& hostConfig,
        const TCellConfig& cellConfig)
    : GrpcPort(cellConfig.GetGrpcPort())
    , SecureGrpcPort(cellConfig.GetSecureGrpcPort())
    , RdmaPort(cellConfig.GetRdmaPort())
    , NbdPort(cellConfig.GetNbdPort())
    , Fqdn(hostConfig.GetFqdn())
    , Transport(hostConfig.GetTransport() ?
        hostConfig.GetTransport():
        cellConfig.GetTransport())
    , HeartbeatInterval(cellConfig.GetHeartbeatInterval())
    , HeartbeatTimeout(cellConfig.GetHeartbeatTimeout())
    , HeartbeatFailCount(cellConfig.GetHeartbeatFailCount())
{
}

////////////////////////////////////////////////////////////////////////////////

struct TCellConfig::TImpl
{
    const NProto::TCellConfig Config;
    TConfiguredHostsByFqdn ConfiguredHosts;

    BLOCKSTORE_CELL_DEFAULT_CONFIG(BLOCKSTORE_CELL_DECLARE_CONFIG);
    BLOCKSTORE_CELL_DEFAULT_CONFIG(CONFIG_ITEM_IS_SET_CHECKER);

    explicit TImpl(NProto::TCellConfig config = {})
        : Config(std::move(config))
    {
    }

    [[nodiscard]] const NProto::TCellConfig& GetCellConfig() const
    {
        return Config;
    }
};

TCellConfig::TCellConfig(NProto::TCellConfig config)
    : Impl(std::make_unique<TImpl>(std::move(config)))
{
    for (const auto& h: Impl->Config.GetHosts()) {
        Impl->ConfiguredHosts.emplace(h.GetFqdn(),TCellHostConfig(h, *this));
    }
}

TCellConfig::~TCellConfig() = default;

const NProto::TCellConfig& TCellConfig::GetCellConfig() const
{
    return Impl->GetCellConfig();
}

const TConfiguredHostsByFqdn& TCellConfig::GetHosts() const
{
    return Impl->ConfiguredHosts;
}

#define BLOCKSTORE_CONFIG_GETTER(name, type, ...)                              \
type TCellConfig::Get##name() const                                            \
{                                                                              \
    return Impl->Is##name##Set(Impl->Config)                                   \
        ? ConvertValue<type>(Impl->Config.Get##name())                         \
        : TImpl::CellDefault##name;                                            \
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

struct TCellsConfig::TImpl
{
    const NProto::TCellsConfig Config;
    TCellConfigByCellId ConfiguredCells;
    NClient::TClientAppConfig GrpcClientConfig;

    BLOCKSTORE_CELLS_DEFAULT_CONFIG(BLOCKSTORE_CELLS_DECLARE_CONFIG);
    BLOCKSTORE_CELLS_DEFAULT_CONFIG(CONFIG_ITEM_IS_SET_CHECKER);

    explicit TImpl(NProto::TCellsConfig config)
        : Config(std::move(config))
        , GrpcClientConfig(CreateClientAppConfig(config.GetGrpcClientConfig()))
    {
        for (const auto& cell: Config.GetCells()) {
            ConfiguredCells.emplace(
                cell.GetCellId(),
                std::make_shared<TCellConfig>(cell));
        }
    }

    [[nodiscard]] const NProto::TCellsConfig& GetCellsConfig() const
    {
        return Config;
    }
};

TCellsConfig::TCellsConfig(NProto::TCellsConfig config)
    : Impl(std::make_unique<TImpl>(std::move(config)))
{
}

TCellsConfig::~TCellsConfig() = default;

const NProto::TCellsConfig& TCellsConfig::GetCellsConfig() const
{
    return Impl->Config;
}

const NClient::TClientAppConfig& TCellsConfig::GetGrpcClientConfig() const
{
    return Impl->GrpcClientConfig;
}

const TCellConfigByCellId& TCellsConfig::GetCells() const
{
    return Impl->ConfiguredCells;
}

#define BLOCKSTORE_CELLS_CONFIG_GETTER(name, type, ...)                        \
type TCellsConfig::Get##name() const                                           \
{                                                                              \
    return Impl->Is##name##Set(Impl->Config)                                   \
        ? ConvertValue<type>(Impl->Config.Get##name())                         \
        : TImpl::CellsDefault##name;                                           \
}
// BLOCKSTORE_CELLS_CONFIG_GETTER

BLOCKSTORE_CELLS_DEFAULT_CONFIG(BLOCKSTORE_CELLS_CONFIG_GETTER)

#undef BLOCKSTORE_CONFIG_GETTER

void TCellsConfig::Dump(IOutputStream& out) const
{
#define BLOCKSTORE_CELLS_CONFIG_DUMP(name, ...)                                \
    out << #name << ": ";                                                      \
    DumpImpl(Get##name(), out);                                                \
    out << Endl;                                                               \
// BLOCKSTORE_CELLS_CONFIG_DUMP

    BLOCKSTORE_CELLS_CONFIG(BLOCKSTORE_CELLS_CONFIG_DUMP);

#undef BLOCKSTORE_CELLS_CONFIG_DUMP
}

void TCellsConfig::DumpHtml(IOutputStream& out) const
{
#define BLOCKSTORE_CELLS_CONFIG_DUMP(name, ...)                                \
    TABLER() {                                                                 \
        TABLED() { out << #name; }                                             \
        TABLED() { DumpImpl(Get##name(), out); }                               \
    }                                                                          \
// BLOCKSTORE_CELLS_CONFIG_DUMP

    HTML(out) {
        TABLE_CLASS("table table-condensed") {
            TABLEBODY() {
                BLOCKSTORE_CELLS_CONFIG(BLOCKSTORE_CELLS_CONFIG_DUMP);
            }
        }
    }

#undef BLOCKSTORE_CELLS_CONFIG_DUMP
}

}   // namespace NCloud::NBlockStore::NCells
