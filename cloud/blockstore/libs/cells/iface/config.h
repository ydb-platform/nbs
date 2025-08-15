#pragma once

#include "public.h"

#include <cloud/blockstore/config/cells.pb.h>
#include <cloud/blockstore/libs/client/config.h>
#include <cloud/blockstore/libs/diagnostics/dumpable.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NCells {

////////////////////////////////////////////////////////////////////////////////

class TCellConfig;

struct TCellHostConfig
{
    TCellHostConfig(
        const NProto::TCellHostConfig& hostConfig,
        const TCellConfig& cellConfig);

    ui32 GetGrpcPort() const
    {
        return GrpcPort;
    }

    ui32 GetSecureGrpcPort() const
    {
        return SecureGrpcPort;
    }

    ui32 GetRdmaPort() const
    {
        return RdmaPort;
    }

    ui32 GetNbdPort() const
    {
        return NbdPort;
    }

    TString GetFqdn() const
    {
        return Fqdn;
    }

    NProto::ECellDataTransport GetTransport() const
    {
        return Transport;
    }

private:
    ui32 GrpcPort = 0;
    ui32 SecureGrpcPort = 0;
    ui32 RdmaPort = 0;
    ui32 NbdPort = 0;
    TString Fqdn;
    NProto::ECellDataTransport Transport = NProto::CELL_DATA_TRANSPORT_UNSET;
};

////////////////////////////////////////////////////////////////////////////////

using TConfiguredHostsByFqdn = THashMap<TString, TCellHostConfig>;

class TCellConfig: public IDumpable
{
private:
    const NProto::TCellConfig Config;
    TConfiguredHostsByFqdn ConfiguredHosts;

public:
    explicit TCellConfig(NProto::TCellConfig config = {});

    [[nodiscard]] const NProto::TCellConfig& GetCellConfig() const
    {
        return Config;
    }

    [[nodiscard]] TString GetCellId() const;
    [[nodiscard]] ui32 GetGrpcPort() const;
    [[nodiscard]] ui32 GetSecureGrpcPort() const;
    [[nodiscard]] ui32 GetRdmaPort() const;
    [[nodiscard]] ui32 GetNbdPort() const;
    [[nodiscard]] NProto::ECellDataTransport GetTransport() const;
    [[nodiscard]] const TConfiguredHostsByFqdn& GetHosts() const;
    [[nodiscard]] ui32 GetDescribeVolumeHostCount() const;
    [[nodiscard]] ui32 GetMinCellConnections() const;

    void Dump(IOutputStream& out) const override;
    void DumpHtml(IOutputStream& out) const override;
};

////////////////////////////////////////////////////////////////////////////////

using TCellConfigByCellId = THashMap<TString, TCellConfig>;

////////////////////////////////////////////////////////////////////////////////

class TCellsConfig: public IDumpable
{
private:
    const NProto::TCellsConfig Config;
    TCellConfigByCellId ConfiguredCells;
    NClient::TClientAppConfig GrpcClientConfig;

public:
    explicit TCellsConfig(NProto::TCellsConfig config = {});

    [[nodiscard]] const NProto::TCellsConfig& GetCellsConfig() const
    {
        return Config;
    }

    [[nodiscard]] TString GetCellId() const;
    [[nodiscard]] const TCellConfigByCellId& GetCells() const;
    [[nodiscard]] TDuration GetDescribeVolumeTimeout() const;
    [[nodiscard]] const NClient::TClientAppConfig& GetGrpcClientConfig() const;
    [[nodiscard]] ui32 GetRdmaTransportWorkers() const;
    [[nodiscard]] bool GetCellsEnabled() const;

    void Dump(IOutputStream& out) const override;
    void DumpHtml(IOutputStream& out) const override;
};

}   // namespace NCloud::NBlockStore::NCells
