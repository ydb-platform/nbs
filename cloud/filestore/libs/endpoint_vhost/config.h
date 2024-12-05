#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>
#include <cloud/filestore/config/vhost.pb.h>

#include <cloud/storage/core/libs/xsl_render/xml_document.h>

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
    NCloud::NProto::EEndpointStorageType GetEndpointStorageType() const;
    TString GetEndpointStorageDir() const;
    ui32 GetSocketAccessMode() const;
    bool GetEndpointStorageNotImplementedErrorIsFatal() const;
    const NProto::TLocalServiceConfig* GetLocalServiceConfig() const;

    TString GetHandleOpsQueuePath() const;
    ui32 GetHandleOpsQueueSize() const;

    void Dump(IOutputStream& out) const;
    void DumpXml(NXml::TNode out) const;
};

}   // namespace NCloud::NFileStore::NVhost
