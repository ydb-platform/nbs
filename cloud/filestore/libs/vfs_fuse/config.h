#pragma once

#include "config.h"

#include <cloud/filestore/config/filesystem.pb.h>
#include <cloud/filestore/config/vfs.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NFuse {

////////////////////////////////////////////////////////////////////////////////

struct TFileSystemConfig
{
private:
    const NProto::TFileSystemConfig ProtoConfig;

public:
    TFileSystemConfig(const NProto::TFileSystemConfig& protoConfig)
        : ProtoConfig(protoConfig)
    {}

    TString GetFileSystemId() const;
    ui32 GetBlockSize() const;

    TDuration GetLockRetryTimeout() const;
    TDuration GetEntryTimeout() const;
    TDuration GetNegativeEntryTimeout() const;
    TDuration GetAttrTimeout() const;

    ui32 GetXAttrCacheLimit() const;
    TDuration GetXAttrCacheTimeout() const;

    ui32 GetMaxBufferSize() const;
    ui32 GetPreferredBlockSize() const;

    bool GetAsyncDestroyHandleEnabled() const;
    TDuration GetAsyncHandleOperationPeriod() const;

    bool GetDirectIoEnabled() const;
    ui32 GetDirectIoAlign() const;

    bool GetGuestWritebackCacheEnabled() const;

    void Dump(IOutputStream& out) const;
};

}   // namespace NCloud::NFileStore::NFuse
