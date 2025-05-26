#pragma once

#include "public.h"

#include "rdma_target.h"
#include "storage_with_stats.h"

#include <cloud/blockstore/config/disk.pb.h>
#include <cloud/blockstore/libs/common/public.h>
#include <cloud/blockstore/libs/diagnostics/public.h>
#include <cloud/blockstore/libs/nvme/public.h>
#include <cloud/blockstore/libs/rdma/iface/public.h>
#include <cloud/blockstore/libs/service/public.h>
#include <cloud/blockstore/libs/service/storage.h>
#include <cloud/blockstore/libs/spdk/iface/public.h>
#include <cloud/blockstore/libs/storage/core/public.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_client.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/device_guard.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <contrib/ydb/library/actors/core/actorid.h>
#include <contrib/ydb/library/actors/core/actorsystem.h>

#include <library/cpp/threading/future/future.h>

#include <util/generic/hash.h>
#include <util/generic/vector.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

class TDiskAgentState: public IMultiagentWriteHandler
{
private:
    struct TDeviceState
    {
        NProto::TDeviceConfig Config;
        std::shared_ptr<TStorageAdapter> StorageAdapter;

        TStorageIoStatsPtr Stats;
    };

private:
    NActors::TActorSystem* ActorSystem;
    const NActors::TActorId DiskAgentId;
    const TStorageConfigPtr StorageConfig;
    const TDiskAgentConfigPtr AgentConfig;
    const NSpdk::ISpdkEnvPtr Spdk;
    const ICachingAllocatorPtr Allocator;
    const IStorageProviderPtr StorageProvider;
    const IProfileLogPtr ProfileLog;
    const IBlockDigestGeneratorPtr BlockDigestGenerator;

    ILoggingServicePtr Logging;
    TLog Log;
    NSpdk::ISpdkTargetPtr SpdkTarget;
    NRdma::IServerPtr RdmaServer;
    IRdmaTargetPtr RdmaTarget;
    NNvme::INvmeManagerPtr NvmeManager;

    THashMap<TString, TDeviceState> Devices;
    TDeviceClientPtr DeviceClient;

    ui32 InitErrorsCount = 0;
    bool PartiallySuspended = false;

    TRdmaTargetConfigPtr RdmaTargetConfig;
    TOldRequestCounters OldRequestCounters;

public:
    TDiskAgentState(
        NActors::TActorSystem* actorSystem,
        NActors::TActorId diskAgentId,
        TStorageConfigPtr storageConfig,
        TDiskAgentConfigPtr agentConfig,
        NSpdk::ISpdkEnvPtr spdk,
        ICachingAllocatorPtr allocator,
        IStorageProviderPtr storageProvider,
        IProfileLogPtr profileLog,
        IBlockDigestGeneratorPtr blockDigestGenerator,
        ILoggingServicePtr logging,
        NRdma::IServerPtr rdmaServer,
        NNvme::INvmeManagerPtr nvmeManager,
        TRdmaTargetConfigPtr rdmaTargetConfig,
        TOldRequestCounters oldRequestCounters);

    struct TInitializeResult
    {
        TVector<NProto::TDeviceConfig> Configs;
        TVector<TString> Errors;
        TVector<TString> ConfigMismatchErrors;
        TVector<TString> DevicesWithSuspendedIO;

        TDeviceGuard Guard;
    };

    NThreading::TFuture<TInitializeResult> Initialize();

    NThreading::TFuture<NProto::TAgentStats> CollectStats();

    NThreading::TFuture<NProto::TReadDeviceBlocksResponse> Read(
        TInstant now,
        NProto::TReadDeviceBlocksRequest request);

    NThreading::TFuture<NProto::TWriteDeviceBlocksResponse> Write(
        TInstant now,
        NProto::TWriteDeviceBlocksRequest request);

    NThreading::TFuture<NProto::TWriteDeviceBlocksResponse> WriteBlocks(
        TInstant now,
        const TString& deviceUUID,
        std::shared_ptr<NProto::TWriteBlocksRequest> request,
        ui32 blockSize,
        TStringBuf buffer);

    NThreading::TFuture<NProto::TZeroDeviceBlocksResponse> WriteZeroes(
        TInstant now,
        NProto::TZeroDeviceBlocksRequest request);

    NThreading::TFuture<NProto::TError> SecureErase(const TString& uuid, TInstant now);

    NThreading::TFuture<NProto::TChecksumDeviceBlocksResponse> Checksum(
        TInstant now,
        NProto::TChecksumDeviceBlocksRequest request);

    // Implements IMultiagentWriteHandler
    NThreading::TFuture<TMultiAgentWriteResponsePrivate> PerformMultiAgentWrite(
        TCallContextPtr callContext,
        std::shared_ptr<NProto::TWriteDeviceBlocksRequest> request) override;

    void CheckIOTimeouts(TInstant now);

    const TString& GetDeviceName(const TString& uuid) const;
    const NProto::TDeviceConfig* FindDeviceConfig(const TString& uuid) const;

    TVector<NProto::TDeviceConfig> GetDevices() const;
    TVector<NProto::TDeviceConfig> GetEnabledDevices() const;

    TVector<TString> GetDeviceIds() const;

    ui32 GetDevicesCount() const;

    TDeviceClient::TSessionInfo GetWriterSession(const TString& uuid) const;
    TVector<TDeviceClient::TSessionInfo> GetReaderSessions(
        const TString& uuid) const;

    // @return `true` if any session has been updated (excluding `LastActivityTs`
    // field) or a new one has been added.
    bool AcquireDevices(
        const TVector<TString>& uuids,
        const TString& clientId,
        TInstant now,
        NProto::EVolumeAccessMode accessMode,
        ui64 mountSeqNumber,
        const TString& diskId,
        ui32 volumeGeneration);

    void ReleaseDevices(
        const TVector<TString>& uuids,
        const TString& clientId,
        const TString& diskId,
        ui32 volumeGeneration);

    TVector<NProto::TDiskAgentDeviceSession> GetSessions() const;

    void DisableDevice(const TString& uuid);
    void SuspendDevice(const TString& uuid);
    void EnableDevice(const TString& uuid);
    bool IsDeviceDisabled(const TString& uuid) const;
    bool IsDeviceSuspended(const TString& uuid) const;
    void ReportDisabledDeviceError(const TString& uuid);

    void StopTarget();

    void SetPartiallySuspended(bool partiallySuspended);
    bool GetPartiallySuspended() const;

private:
    const TDeviceState& GetDeviceState(
        const TString& uuid,
        const TString& clientId,
        const NProto::EVolumeAccessMode accessMode) const;

    const TDeviceState& GetDeviceStateImpl(
        const TString& uuid,
        const TString& clientId,
        const NProto::EVolumeAccessMode accessMode) const;
    const TDeviceState& GetDeviceStateImpl(const TString& uuid) const;

    void EnsureAccessToDevices(
        const TVector<TString>& uuids,
        const TString& clientId,
        NProto::EVolumeAccessMode accessMode) const;

    template <typename T>
    void WriteProfileLog(
        TInstant now,
        const TString& uuid,
        const T& req,
        ui32 blockSize,
        ESysRequestType requestType);

    NThreading::TFuture<TInitializeResult> InitSpdkStorage();
    NThreading::TFuture<TInitializeResult> InitAioStorage();

    void InitRdmaTarget();

    void RestoreSessions(TDeviceClient& client) const;

    void CheckIfDeviceIsDisabled(const TString& uuid, const TString& clientId);
};

}   // namespace NCloud::NBlockStore::NStorage
