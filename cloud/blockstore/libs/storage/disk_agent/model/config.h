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
    ui32 NetworkMbitThroughput = 0;

public:
    TDiskAgentConfig() = default;

    TDiskAgentConfig(
            NProto::TDiskAgentConfig config,
            TString rack,
            ui32 networkMbitThroughput)
        : Config(std::move(config))
        , Rack(std::move(rack))
        , NetworkMbitThroughput(networkMbitThroughput)
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
    bool GetIOParserActorAllocateStorageEnabled() const;
    bool GetDisableNodeBrokerRegistrationOnDevicelessAgent() const;
    ui32 GetMaxAIOContextEvents() const;
    ui32 GetPathsPerFileIOService() const;
    bool GetDisableBrokenDevices() const;

    const auto& GetDevicesWithSuspendedIO() const
    {
        return Config.GetDevicesWithSuspendedIO();
    }

    const auto& GetPathToSerialNumberMapping() const
    {
        return Config.GetPathToSerialNumberMapping();
    }

    const NProto::TDiskAgentThrottlingConfig& GetThrottlerConfig() const
    {
        return Config.GetThrottlingConfig();
    }

    ui32 GetNetworkMbitThroughput() const
    {
        return NetworkMbitThroughput;
    }

    ui32 GetMaxParallelSecureErasesAllowed() const;

    bool GetUseLocalStorageSubmissionThread() const;
    bool GetUseOneSubmissionThreadPerAIOServiceEnabled() const;

    [[nodiscard]] bool GetKickOutOldClientsEnabled() const;

    bool GetEnableDataIntegrityValidationForDrBasedDisks() const;

    void Dump(IOutputStream& out) const;
    void DumpHtml(IOutputStream& out) const;
};

////////////////////////////////////////////////////////////////////////////////

[[nodiscard]] auto LoadDiskAgentConfig(
    const TString& path) -> TResultOrError<NProto::TDiskAgentConfig>;

[[nodiscard]] auto UpdateDevicesWithSuspendedIO(
    const TString& path,
    const TVector<TString>& uuids) -> NProto::TError;

[[nodiscard]] auto SaveDiskAgentConfig(
    const TString& path,
    const NProto::TDiskAgentConfig& proto) -> NProto::TError;

}   // namespace NCloud::NBlockStore::NStorage
