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

    bool GetGuestWriteBackCacheEnabled() const;

    bool GetZeroCopyEnabled() const;

    bool GetGuestPageCacheDisabled() const;
    bool GetExtendedAttributesDisabled() const;

    bool GetServerWriteBackCacheEnabled() const;

    bool GetGuestKeepCacheAllowed() const;

    ui32 GetMaxBackground() const;

    ui32 GetMaxFuseLoopThreads() const;

    bool GetZeroCopyWriteEnabled() const;

    bool GetFSyncQueueDisabled() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NFileStore::NFuse
