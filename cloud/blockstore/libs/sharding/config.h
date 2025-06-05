#pragma once

#include "public.h"

#include <cloud/blockstore/config/sharding.pb.h>

#include <cloud/blockstore/libs/diagnostics/dumpable.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NSharding {

////////////////////////////////////////////////////////////////////////////////

class TShardConfig;
struct TShardHostConfig
{
    TShardHostConfig(
        NProto::TShardHostConfig hostConfig,
        TShardConfig shardConfig);

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

    NProto::EShardDataTransport GetTransport() const
    {
        return Transport;
    }

private:
    ui32 GrpcPort = 0;
    ui32 SecureGrpcPort = 0;
    ui32 RdmaPort = 0;
    ui32 NbdPort = 0;
    TString Fqdn;
    NProto::EShardDataTransport Transport;
};

////////////////////////////////////////////////////////////////////////////////

using TConfiguredHosts = THashMap<TString, TShardHostConfig>;

class TShardConfig
    : public IDumpable
{
private:
    const NProto::TShardConfig Config;
    TConfiguredHosts ConfiguredHosts;

public:
    explicit TShardConfig(NProto::TShardConfig config = {});

    [[nodiscard]] const NProto::TShardConfig& GetShardConfig() const
    {
        return Config;
    }

    [[nodiscard]] TString GetShardId() const;
    [[nodiscard]] ui32 GetGrpcPort() const;
    [[nodiscard]] ui32 GetSecureGrpcPort() const;
    [[nodiscard]] ui32 GetRdmaPort() const;
    [[nodiscard]] ui32 GetNbdPort() const;
    [[nodiscard]] NProto::EShardDataTransport GetTransport() const;
    [[nodiscard]] const TConfiguredHosts& GetHosts() const;
    [[nodiscard]] ui32 GetShardDescribeHostCnt() const;
    [[nodiscard]] TString GetFixedHost() const;
    [[nodiscard]] ui32 GetMinShardConnections() const;

    void Dump(IOutputStream& out) const override;
    void DumpHtml(IOutputStream& out) const override;
};

////////////////////////////////////////////////////////////////////////////////

using TConfiguredShards = THashMap<TString, TShardConfig>;

////////////////////////////////////////////////////////////////////////////////

class TShardingConfig
    : public IDumpable
{
private:
    const NProto::TShardingConfig Config;
    TConfiguredShards ConfiguredShards;

public:
    explicit TShardingConfig(NProto::TShardingConfig Config = {});

    [[nodiscard]] const NProto::TShardingConfig& GetShardingConfig() const
    {
        return Config;
    }

    [[nodiscard]] TString GetShardId() const;
    [[nodiscard]] const TConfiguredShards& GetShards() const;
    [[nodiscard]] TDuration GetDescribeTimeout() const;

    void Dump(IOutputStream& out) const override;
    void DumpHtml(IOutputStream& out) const override;
};

}   // namespace NCloud::NBlockStore::NSharding
