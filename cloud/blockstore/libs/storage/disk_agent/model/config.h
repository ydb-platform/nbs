#pragma once

#include "public.h"

#include <cloud/blockstore/config/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/stream/output.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentConfig
{
private:
    NProto::TDiskAgentConfig Config;
    TString Rack;

public:
    TDiskAgentConfig() = default;

    TDiskAgentConfig(
            NProto::TDiskAgentConfig config,
            TString rack)
        : Config(std::move(config))
        , Rack(std::move(rack))
    {}

    bool GetEnabled() const;
    TString GetAgentId() const;
    ui64 GetSeqNumber() const;
    bool GetDedicatedDiskAgent() const;

    const auto& GetMemoryDevices() const
    {
        return Config.GetMemoryDevices();
    }

    const auto& GetFileDevices() const
    {
        return Config.GetFileDevices();
    }

    const auto& GetNVMeDevices() const
    {
        return Config.GetNvmeDevices();
    }

    const auto& GetNVMeTarget() const
    {
        return Config.GetNvmeTarget();
    }

    const auto& DeprecatedGetRdmaTarget() const
    {
        return Config.GetRdmaTarget();
    }

    auto DeprecatedHasRdmaTarget() const
    {
        return Config.HasRdmaTarget();
    }

    ui32 GetPageSize() const;
    ui32 GetMaxPageCount() const;
    ui32 GetPageDropSize() const;

    TDuration GetRegisterRetryTimeout() const;
    TDuration GetSecureEraseTimeout() const;
    TDuration GetDeviceIOTimeout() const;
    bool GetDeviceIOTimeoutsDisabled() const;
    TDuration GetShutdownTimeout() const;

    NProto::EDiskAgentBackendType GetBackend() const;
    NProto::EDeviceEraseMethod GetDeviceEraseMethod() const;

    bool GetAcquireRequired() const;

    TDuration GetReleaseInactiveSessionsTimeout() const;

    const TString& GetRack() const
    {
        return Rack;
    }

    bool GetDirectIoFlagDisabled() const;

    bool GetDeviceLockingEnabled() const;

    bool GetDeviceHealthCheckDisabled() const;

    const NProto::TStorageDiscoveryConfig& GetStorageDiscoveryConfig() const
    {
        return Config.GetStorageDiscoveryConfig();
    }

    TString GetCachedConfigPath() const;
    TString GetCachedSessionsPath() const;

    bool GetTemporaryAgent() const;

    ui32 GetIOParserActorCount() const;
    bool GetOffloadAllIORequestsParsingEnabled() const;
    bool GetDisableNodeBrokerRegistrationOnDevicelessAgent() const;
    ui32 GetMaxAIOContextEvents() const;
    ui32 GetPathsPerFileIOService() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] auto LoadDiskAgentConfig(
    const TString& path) -> TResultOrError<NProto::TDiskAgentConfig>;

[[nodiscard]] auto SaveDiskAgentConfig(
    const TString& path,
    const NProto::TDiskAgentConfig& proto) -> NProto::TError;

}   // namespace NCloud::NBlockStore::NStorage
