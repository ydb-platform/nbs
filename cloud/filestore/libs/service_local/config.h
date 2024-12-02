#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>

#include <cloud/storage/core/libs/xsl_render/xml_document.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TLocalFileStoreConfig
{
private:
    NProto::TLocalServiceConfig ProtoConfig;

public:
    TLocalFileStoreConfig(const NProto::TLocalServiceConfig& protoConfig = {})
        : ProtoConfig(protoConfig)
    {}

    TString GetRootPath() const;
    TString GetPathPrefix() const;
    ui32 GetDefaultPermissions() const;
    TDuration GetIdleSessionTimeout() const;
    ui32 GetNumThreads() const;
    TString GetStatePath() const;
    ui32 GetMaxNodeCount() const;
    ui32 GetMaxHandlePerSessionCount() const;
    bool GetDirectIoEnabled() const;
    ui32 GetDirectIoAlign() const;
    bool GetGuestWritebackCacheEnabled() const;

    void Dump(IOutputStream& out) const;
    void DumpXml(NXml::TNode& root) const;

    bool GetAsyncDestroyHandleEnabled() const;
    TDuration GetAsyncHandleOperationPeriod() const;
};

}   // namespace NCloud::NFileStore
