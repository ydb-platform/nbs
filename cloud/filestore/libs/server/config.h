#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>
#include <cloud/storage/core/protos/certificate.pb.h>

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

namespace NCloud::NFileStore::NServer {

////////////////////////////////////////////////////////////////////////////////

struct TCertificate
{
    TString CertFile;
    TString CertPrivateKeyFile;
};

////////////////////////////////////////////////////////////////////////////////

class TServerConfig
{
private:
    const NProto::TServerConfig ProtoConfig;

public:
    TServerConfig(const NProto::TServerConfig& protoConfig = {})
        : ProtoConfig(protoConfig)
    {}

    TString GetHost() const;
    ui32 GetPort() const;

    ui32 GetMaxMessageSize() const;
    ui32 GetMemoryQuotaBytes() const;
    ui32 GetPreparedRequestsCount() const;

    ui32 GetThreadsCount() const;
    ui32 GetGrpcThreadsLimit() const;

    bool GetKeepAliveEnabled() const;
    TDuration GetKeepAliveIdleTimeout() const;
    TDuration GetKeepAliveProbeTimeout() const;
    ui32 GetKeepAliveProbesCount() const;

    TDuration GetShutdownTimeout() const;

    TString GetSecureHost() const;
    ui32 GetSecurePort() const;
    TString GetRootCertsFile() const;
    TVector<TCertificate> GetCerts() const;

    TString GetUnixSocketPath() const;
    ui32 GetUnixSocketBacklog() const;

    TVector<TString> GetActionsNoAuth() const;

    const NProto::TServerConfig& GetProto() const
    {
        return ProtoConfig;
    }

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
    void DumpXml(NXml::TNode& root) const;
};

}   // namespace NCloud::NFileStore::NServer
