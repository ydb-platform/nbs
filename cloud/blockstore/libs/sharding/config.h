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

class TShardConfig
    : public IDumpable
{
private:
    const NProto::TShardInfo Config;

public:
    explicit TShardConfig(NProto::TShardInfo Config = {});

    [[nodiscard]] const NProto::TShardInfo& GetInfo() const
    {
        return Config;
    }

    [[nodiscard]] TString GetShardId() const;
    [[nodiscard]] ui32 GetGrpcPort() const;
    [[nodiscard]] ui32 GetSecureGrpcPort() const;
    [[nodiscard]] ui32 GetRdmaPort() const;
    [[nodiscard]] ui32 GetNbdPort() const;
    [[nodiscard]] NProto::EShardDataTransport GetTransport() const;
    [[nodiscard]] TVector<TString> GetHosts() const;
    [[nodiscard]] ui32 GetShardDescribeHostCnt() const;
    [[nodiscard]] TString GetFixedHost() const;
    [[nodiscard]] ui32 GetMinShardConnections() const;

    void Dump(IOutputStream& out) const override;
    void DumpHtml(IOutputStream& out) const override;
};

////////////////////////////////////////////////////////////////////////////////

using TShards = THashMap<TString, TShardConfig>;

////////////////////////////////////////////////////////////////////////////////

class TShardingConfig
    : public IDumpable
{
private:
    const NProto::TShardingConfig Config;

public:
    explicit TShardingConfig(NProto::TShardingConfig Config = {});

    [[nodiscard]] const NProto::TShardingConfig& GetShardingConfig() const
    {
        return Config;
    }

    [[nodiscard]] TString GetShardId() const;
    [[nodiscard]] TShards GetShards() const;

    void Dump(IOutputStream& out) const override;
    void DumpHtml(IOutputStream& out) const override;
};

}   // namespace NCloud::NBlockStore::NSharding
