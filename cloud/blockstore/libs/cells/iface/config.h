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
private:
    ui32 GrpcPort = 0;
    ui32 SecureGrpcPort = 0;
    ui32 RdmaPort = 0;
    ui32 NbdPort = 0;
    TString Fqdn;
    NProto::ECellDataTransport Transport = NProto::CELL_DATA_TRANSPORT_UNSET;

public:
    TCellHostConfig(
        const NProto::TCellHostConfig& hostConfig,
        const TCellConfig& cellConfig);

    TCellHostConfig() = default;

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
};

////////////////////////////////////////////////////////////////////////////////

using TConfiguredHostsByFqdn = THashMap<TString, TCellHostConfig>;

class TCellConfig: public IDumpable
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TCellConfig(NProto::TCellConfig config = {});
    ~TCellConfig() override;

    [[nodiscard]] const NProto::TCellConfig& GetCellConfig() const;

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

using TCellConfigByCellId = THashMap<TString, TCellConfigPtr>;

////////////////////////////////////////////////////////////////////////////////

class TCellsConfig: public IDumpable
{
private:
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    explicit TCellsConfig(NProto::TCellsConfig config = {});
    ~TCellsConfig() override;

    [[nodiscard]] const NProto::TCellsConfig& GetCellsConfig() const;

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
