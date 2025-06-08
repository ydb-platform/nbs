#pragma once

#include "config.h"

#include <cloud/filestore/config/vfs.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

namespace NCloud::NFileStore::NVFS {

////////////////////////////////////////////////////////////////////////////////

struct TVFSConfig
{
private:
    const NProto::TVFSConfig ProtoConfig;

public:
    TVFSConfig(const NProto::TVFSConfig& protoConfig)
        : ProtoConfig(protoConfig)
    {}

    TString GetFileSystemId() const;
    TString GetClientId() const;

    TString GetSocketPath() const;
    TString GetMountPath() const;
    bool GetReadOnly() const;
    bool GetDebug() const;

    TDuration GetLockRetryTimeout() const;

    ui32 GetMaxWritePages() const;
    ui32 GetMaxBackground() const;

    ui64 GetMountSeqNumber() const;
    ui32 GetVhostQueuesCount() const;

    ui32 GetXAttrCacheSize() const;
    TDuration GetXAttrCacheTimeout() const;

    TString GetHandleOpsQueuePath() const;
    ui32 GetHandleOpsQueueSize() const;

    TString GetWriteBackCachePath() const;
    ui32 GetWriteBackCacheCapacity() const;
    TDuration GetWriteBackCacheAutomaticFlushPeriod() const;

    bool GetGuestKeepCacheAllowed() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

}   // namespace NCloud::NFileStore::NVFS
