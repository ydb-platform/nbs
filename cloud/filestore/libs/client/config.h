#pragma once

#include "public.h"

#include <cloud/filestore/config/client.pb.h>

#ifdef THROW
#define THROW_OLD THROW
#undef THROW
#endif

#include <library/cpp/xml/document/xml-document.h>
#undef THROW

#ifdef THROW_OLD
#define THROW THROW_OLD
#undef THROW_OLD
#endif

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NClient {

////////////////////////////////////////////////////////////////////////////////

class TClientConfig
{
private:
    const NProto::TClientConfig ProtoConfig;

public:
    TClientConfig(const NProto::TClientConfig& protoConfig)
        : ProtoConfig(protoConfig)
    {}

    TString GetHost() const;
    ui32 GetPort() const;

    ui32 GetThreadsCount() const;
    ui32 GetMaxMessageSize() const;
    ui32 GetMemoryQuotaBytes() const;
    ui32 GetGrpcThreadsLimit() const;

    TDuration GetRequestTimeout() const;
    TDuration GetGrpcReconnectBackoff() const;
    TDuration GetRetryTimeout() const;
    TDuration GetRetryTimeoutIncrement() const;
    TDuration GetConnectionErrorMaxRetryTimeout() const;

    ui32 GetSecurePort() const;
    TString GetRootCertsFile() const;
    TString GetCertFile() const;
    TString GetCertPrivateKeyFile() const;
    TString GetAuthToken() const;
    bool GetSkipCertVerification() const;

    TString GetUnixSocketPath() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
    void DumpXml(NXml::TNode& root) const;
};

////////////////////////////////////////////////////////////////////////////////

class TSessionConfig
{
private:
    const NProto::TSessionConfig ProtoConfig;

public:
    TSessionConfig(const NProto::TSessionConfig& protoConfig)
        : ProtoConfig(protoConfig)
    {}

    TString GetFileSystemId() const;
    TString GetClientId() const;

    TDuration GetSessionPingTimeout() const;
    TDuration GetSessionRetryTimeout() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
    void DumpXml(NXml::TNode& root) const;

};

}   // namespace NCloud::NFileStore::NClient
