#pragma once

#include "public.h"

#include <cloud/blockstore/config/logbroker.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

#include <variant>

namespace NCloud::NBlockStore::NLogbroker {

////////////////////////////////////////////////////////////////////////////////

class TIamJwtFile
{
private:
    NProto::TLogbrokerConfig::TIamJwtFile Config;

public:
    explicit TIamJwtFile(
            NProto::TLogbrokerConfig::TIamJwtFile config = {})
        : Config(std::move(config))
    {}

    [[nodiscard]] TString GetIamEndpoint() const;
    [[nodiscard]] TString GetJwtFilename() const;
};

////////////////////////////////////////////////////////////////////////////////

struct TIamMetadataServer
{
private:
    NProto::TLogbrokerConfig::TIamMetadataServer Config;

public:
    explicit TIamMetadataServer(
            NProto::TLogbrokerConfig::TIamMetadataServer config = {})
        : Config(std::move(config))
    {}

    [[nodiscard]] TString GetEndpoint() const;
};

////////////////////////////////////////////////////////////////////////////////

using TAuthConfig =
    std::variant<std::monostate, TIamMetadataServer, TIamJwtFile>;

////////////////////////////////////////////////////////////////////////////////

class TLogbrokerConfig
{
private:
    const NProto::TLogbrokerConfig Config;

public:
    explicit TLogbrokerConfig(NProto::TLogbrokerConfig config = {});

    TString GetAddress() const;
    ui32 GetPort() const;
    TString GetDatabase() const;
    bool GetUseLogbrokerCDS() const;
    TString GetCaCertFilename() const;

    TString GetTopic() const;
    TString GetSourceId() const;

    TString GetMetadataServerAddress() const;

    NProto::TLogbrokerConfig::EProtocol GetProtocol() const;

    TAuthConfig GetAuthConfig() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NBlockStore::NLogbroker
