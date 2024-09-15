#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>

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
    ui32 GetMaxInodeCount() const;
    ui32 GetMaxHandlePerSessionCount() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NFileStore
