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
    ui32 GetMaxNodeCount() const;
    ui32 GetMaxHandlePerSessionCount() const;
    bool GetDirectIoEnabled() const;
    ui32 GetDirectIoAlign() const;
    bool GetGuestWriteBackCacheEnabled() const;
    ui32 GetNodeCleanupBatchSize() const;
    bool GetZeroCopyEnabled() const;
    bool GetGuestPageCacheDisabled() const;
    bool GetExtendedAttributesDisabled() const;

    void Dump(IOutputStream& out) const;
    TString DumpStr() const;
    void DumpHtml(IOutputStream& out) const;

    bool GetAsyncDestroyHandleEnabled() const;
    TDuration GetAsyncHandleOperationPeriod() const;

    bool GetOpenNodeByHandleEnabled() const;

    bool GetServerWriteBackCacheEnabled() const;

    bool GetDontPopulateNodeCacheWhenListingNodes() const;

    bool GetGuestOnlyPermissionsCheckEnabled() const;

    ui32 GetMaxEntriesPerListNodes() const;
};

}   // namespace NCloud::NFileStore
