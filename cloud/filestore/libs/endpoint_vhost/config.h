#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>
#include <cloud/filestore/config/vhost.pb.h>

#include <util/datetime/base.h>
#include <util/generic/vector.h>

namespace NCloud::NFileStore::NVhost {

////////////////////////////////////////////////////////////////////////////////

class TVhostServiceConfig
{
private:
    NProto::TVhostServiceConfig ProtoConfig;

public:
    TVhostServiceConfig(const NProto::TVhostServiceConfig& protoConfig = {})
        : ProtoConfig(protoConfig)
    {}

    TVector<NProto::TServiceEndpoint> GetServiceEndpoints() const;
    TString GetRootKeyringName() const;
    TString GetEndpointsKeyringName() const;
    bool GetRequireEndpointsKeyring() const;
    NCloud::NProto::EEndpointStorageType GetEndpointStorageType() const;
    TString GetEndpointStorageDir() const;
    ui32 GetSocketAccessMode() const;
    bool GetEndpointStorageNotImplementedErrorIsFatal() const;
    const NProto::TLocalServiceConfig* GetLocalServiceConfig() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NFileStore::NVhost
