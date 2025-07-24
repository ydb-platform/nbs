#pragma once

#include "public.h"

#include <cloud/filestore/config/server.pb.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>

#include <variant>

namespace NCloud::NFileStore {

////////////////////////////////////////////////////////////////////////////////

class TNullFileIOConfig
{
private:
    NProto::TNullFileIOConfig Proto;

public:
    explicit TNullFileIOConfig(NProto::TNullFileIOConfig proto = {})
        : Proto(std::move(proto))
    {}
};

////////////////////////////////////////////////////////////////////////////////

class TAioConfig
{
private:
    NProto::TAioConfig Proto;

public:
    explicit TAioConfig(NProto::TAioConfig proto = {})
        : Proto(std::move(proto))
    {}

    [[nodiscard]] ui32 GetEntries() const;
};

////////////////////////////////////////////////////////////////////////////////

class TIoUringConfig
{
private:
    NProto::TIoUringConfig Proto;

public:
    explicit TIoUringConfig(NProto::TIoUringConfig proto = {})
        : Proto(std::move(proto))
    {}

    [[nodiscard]] ui32 GetEntries() const;
    [[nodiscard]] bool GetShareKernelWorkers() const;
    [[nodiscard]] ui32 GetMaxKernelWorkersCount() const;
    [[nodiscard]] bool GetForceAsyncIO() const;
};

////////////////////////////////////////////////////////////////////////////////

using TFileIOConfig =
    std::variant<TNullFileIOConfig, TAioConfig, TIoUringConfig>;

////////////////////////////////////////////////////////////////////////////////

class TLocalFileStoreConfig
{
private:
    NProto::TLocalServiceConfig ProtoConfig;

public:
    TLocalFileStoreConfig(NProto::TLocalServiceConfig protoConfig = {})
        : ProtoConfig(std::move(protoConfig))
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

    ui32 GetMaxResponseEntries() const;

    TFileIOConfig GetFileIOConfig() const;
};

}   // namespace NCloud::NFileStore
