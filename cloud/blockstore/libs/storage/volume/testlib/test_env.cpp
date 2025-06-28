#include "test_env.h"

#include <cloud/blockstore/libs/endpoints/endpoint_events.h>
#include <cloud/blockstore/libs/storage/testlib/ss_proxy_mock.h>

#include <cloud/storage/core/libs/common/media.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

using namespace NKikimr;

using namespace NCloud::NBlockStore::NStorage::NPartition;

using namespace NCloud::NStorage;

namespace NTestVolume {

////////////////////////////////////////////////////////////////////////////////

TString GetBlockContent(char fill, size_t size)
{
    return TString(size, fill);
}

TDiagnosticsConfigPtr CreateTestDiagnosticsConfig()
{
    return std::make_shared<TDiagnosticsConfig>(NProto::TDiagnosticsConfig());
}

void CheckForkJoin(const NLWTrace::TShuttleTrace& trace, bool forkRequired)
{
    bool forkSeen = false;
    ui32 forkBudget = 0;
    for (const auto& event: trace.GetEvents()) {
        if (event.GetName() == "Fork") {
            ++forkBudget;
            forkSeen = true;
        } else if (event.GetName() == "Join") {
            UNIT_ASSERT(forkBudget > 0);
            --forkBudget;
        }
    }

    UNIT_ASSERT(!forkRequired || forkSeen);
    UNIT_ASSERT_VALUES_EQUAL(0, forkBudget);
}

bool HasProbe(const NLWTrace::TShuttleTrace& trace, const TString& probeName)
{
    for (const auto& event: trace.GetEvents()) {
        if (event.GetName() == probeName) {
            return true;
        }
    }
    return false;
}

////////////////////////////////////////////////////////////////////////////////

void TFakeHiveProxy::HandleGetStorageInfo(
    const TEvHiveProxy::TEvGetStorageInfoRequest::TPtr& ev,
    const TActorContext& ctx)
{
    const auto* msg = ev->Get();

    TTabletStorageInfoPtr storageInfo;

    const bool isVolumeTablet = AnyOf(
        TestVolumeTablets,
        [&](ui64 tabletId) { return msg->TabletId == tabletId; });
    const bool isPartitionTablet = AnyOf(
        TestPartitionTablets,
        [&](ui64 tabletId) { return msg->TabletId == tabletId; });

    if (isVolumeTablet) {
        storageInfo =
            CreateTestTabletInfo(msg->TabletId, TTabletTypes::BlockStoreVolume);
    } else if (isPartitionTablet) {
        storageInfo = CreateTestTabletInfo(
            msg->TabletId,
            TTabletTypes::BlockStorePartition);
    } else {
        Y_ABORT("Unexpected tablet id in fake hive proxy");
    }

    auto response = std::make_unique<TEvHiveProxy::TEvGetStorageInfoResponse>(
        std::move(storageInfo));

    NCloud::Reply(ctx, *ev, std::move(response));
}

void TFakeHiveProxy::HandleBootExternal(
    const TEvHiveProxy::TEvBootExternalRequest::TPtr& ev,
    const TActorContext& ctx)
{
    using EBootMode = TEvHiveProxy::TEvBootExternalResponse::EBootMode;

    const auto& msg = ev->Get();

    ui32 generation = 0;
    TTabletStorageInfoPtr storageInfo;
    const bool isVolumeTablet = AnyOf(
        TestVolumeTablets,
        [&](ui64 tabletId) { return msg->TabletId == tabletId; });
    if (!isVolumeTablet) {
        generation = ++PartitionGeneration;
        storageInfo = CreateTestTabletInfo(
            msg->TabletId,
            TTabletTypes::BlockStorePartition);
    } else {
        Y_ABORT("Unexpected tablet id in fake hive proxy");
    }

    auto response = std::make_unique<TEvHiveProxy::TEvBootExternalResponse>(
        std::move(storageInfo),
        generation,
        EBootMode::MASTER,
        0);

    NCloud::Reply(ctx, *ev, std::move(response));
}

////////////////////////////////////////////////////////////////////////////////

TActorId TVolumeClient::GetSender() const
{
    return Sender;
}

void TVolumeClient::ReconnectPipe()
{
    PipeClient = Runtime.ConnectToPipe(
        VolumeTabletId,
        Sender,
        NodeIdx,
        NKikimr::GetPipeConfigWithRetries());
}

void TVolumeClient::RebootTablet()
{
    TVector<ui64> tablets = { VolumeTabletId };
    auto guard = CreateTabletScheduledEventsGuard(
        tablets,
        Runtime,
        Sender);

    NKikimr::RebootTablet(Runtime, VolumeTabletId, Sender);

    // sooner or later after reset pipe will reconnect
    // but we do not want to wait
    ReconnectPipe();
}

void TVolumeClient::RebootSysTablet()
{
    TVector<ui64> tablets = {VolumeTabletId };
    auto guard = CreateTabletScheduledEventsGuard(
        tablets,
        Runtime,
        Sender);

    NKikimr::RebootTablet(Runtime, VolumeTabletId, Sender, 0, true);

    // sooner or later after reset pipe will reconnect
    // but we do not want to wait
    ReconnectPipe();
}

std::unique_ptr<TEvBlockStore::TEvUpdateVolumeConfig>
TVolumeClient::CreateUpdateVolumeConfigRequest(
    ui64 maxBandwidth,
    ui32 maxIops,
    ui32 burstPercentage,
    ui64 maxPostponedWeight,
    bool throttlingEnabled,
    ui32 version,
    NCloud::NProto::EStorageMediaKind mediaKind,
    ui64 blockCount,
    TString diskId,
    TString cloudId,
    TString folderId,
    ui32 partitionCount,
    ui32 blocksPerStripe,
    TString tags,
    TString baseDiskId,
    TString baseDiskCheckpointId,
    NProto::EEncryptionMode encryption)
{
    auto request = std::make_unique<TEvBlockStore::TEvUpdateVolumeConfig>();
    request->Record.SetTxId(123);

    auto& volumeConfig = *request->Record.MutableVolumeConfig();
    volumeConfig.SetDiskId(std::move(diskId));
    volumeConfig.SetCloudId(std::move(cloudId));
    volumeConfig.SetFolderId(std::move(folderId));
    volumeConfig.SetBlockSize(DefaultBlockSize);
    volumeConfig.SetStorageMediaKind(mediaKind);
    for (ui32 i = 0; i < partitionCount; ++i) {
        volumeConfig.AddPartitions()->SetBlockCount(blockCount);
    }

    volumeConfig.SetBaseDiskId(std::move(baseDiskId));
    volumeConfig.SetBaseDiskCheckpointId(std::move(baseDiskCheckpointId));

    auto* cps = volumeConfig.MutableExplicitChannelProfiles();
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::System));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Log));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Index));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Merged));
    cps->Add()->SetDataKind(static_cast<ui32>(EChannelDataKind::Fresh));

    volumeConfig.SetVersion(version);
    volumeConfig.SetPerformanceProfileMaxReadBandwidth(maxBandwidth);
    volumeConfig.SetPerformanceProfileMaxWriteBandwidth(maxBandwidth);
    volumeConfig.SetPerformanceProfileMaxReadIops(maxIops);
    volumeConfig.SetPerformanceProfileMaxWriteIops(maxIops);
    volumeConfig.SetPerformanceProfileBurstPercentage(burstPercentage);
    volumeConfig.SetPerformanceProfileMaxPostponedWeight(maxPostponedWeight);
    volumeConfig.SetPerformanceProfileThrottlingEnabled(throttlingEnabled);

    if (!IsDiskRegistryMediaKind(mediaKind)) {
        for (ui32 i = 0; i < partitionCount; ++i) {
            auto* part = request->Record.AddPartitions();
            part->SetPartitionId(i);
            part->SetTabletId(NKikimr::MakeTabletID(0, HiveId, 2 + i));
        }
    }

    volumeConfig.SetBlocksPerStripe(blocksPerStripe);

    if (!tags.empty()) {
        *volumeConfig.MutableTagsStr() = std::move(tags);
    }

    if (encryption != NProto::NO_ENCRYPTION) {
        volumeConfig.MutableEncryptionDesc()->SetMode(encryption);
    }

    return request;
}

std::unique_ptr<TEvVolume::TEvWaitReadyRequest> TVolumeClient::CreateWaitReadyRequest()
{
    return std::make_unique<TEvVolume::TEvWaitReadyRequest>();
}

std::unique_ptr<TEvVolume::TEvAddClientRequest> TVolumeClient::CreateAddClientRequest(
    const NProto::TVolumeClientInfo& info)
{
    auto request = std::make_unique<TEvVolume::TEvAddClientRequest>();
    request->Record.MutableHeaders()->SetClientId(info.GetClientId());
    request->Record.SetInstanceId(info.GetInstanceId());
    request->Record.SetVolumeAccessMode(info.GetVolumeAccessMode());
    request->Record.SetVolumeMountMode(info.GetVolumeMountMode());
    request->Record.SetMountFlags(info.GetMountFlags());
    request->Record.SetMountSeqNumber(info.GetMountSeqNumber());
    return request;
}

std::unique_ptr<TEvVolume::TEvRemoveClientRequest> TVolumeClient::CreateRemoveClientRequest(
    const TString& clientId)
{
    auto request = std::make_unique<TEvVolume::TEvRemoveClientRequest>();
    request->Record.MutableHeaders()->SetClientId(clientId);
    return request;
}

std::unique_ptr<TEvService::TEvStatVolumeRequest> TVolumeClient::CreateStatVolumeRequest(
    const TString& clientId,
    const TVector<TString>& storageConfigFields,
    const bool noPartition)
{
    auto request = std::make_unique<TEvService::TEvStatVolumeRequest>();
    request->Record.MutableHeaders()->SetClientId(clientId);
    for (const auto& field: storageConfigFields) {
        request->Record.AddStorageConfigFields(field);
    }
    request->Record.SetNoPartition(noPartition);
    return request;
}

std::unique_ptr<TEvService::TEvReadBlocksRequest> TVolumeClient::CreateReadBlocksRequest(
    const TBlockRange64& readRange,
    const TString& clientId,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksRequest>();
    request->Record.SetStartIndex(readRange.Start);
    request->Record.SetBlocksCount(readRange.Size());
    request->Record.SetCheckpointId(checkpointId);
    request->Record.MutableHeaders()->SetClientId(clientId);

    return request;
}

std::unique_ptr<TEvService::TEvReadBlocksLocalRequest>
TVolumeClient::CreateReadBlocksLocalRequest(
    const TBlockRange64& readRange,
    const TGuardedSgList& sglist,
    const TString& clientId,
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvReadBlocksLocalRequest>();
    request->Record.SetCheckpointId(checkpointId);
    request->Record.SetStartIndex(readRange.Start);
    request->Record.SetBlocksCount(readRange.Size());
    request->Record.MutableHeaders()->SetClientId(clientId);

    request->Record.Sglist = sglist;
    request->Record.BlockSize = DefaultBlockSize;
    return request;
}

std::unique_ptr<TEvService::TEvWriteBlocksRequest> TVolumeClient::CreateWriteBlocksRequest(
    const TBlockRange64& writeRange,
    const TString& clientId,
    char fill)
{
    return CreateWriteBlocksRequest(
        writeRange,
        clientId,
        GetBlockContent(fill));
}

std::unique_ptr<TEvService::TEvWriteBlocksRequest> TVolumeClient::CreateWriteBlocksRequest(
    const TBlockRange64& writeRange,
    const TString& clientId,
    const TString& blockContent)
{
    auto request = std::make_unique<TEvService::TEvWriteBlocksRequest>();
    request->Record.SetStartIndex(writeRange.Start);
    request->Record.MutableHeaders()->SetClientId(clientId);

    auto& buffers = *request->Record.MutableBlocks()->MutableBuffers();
    for (ui32 i = 0; i < writeRange.Size(); ++i) {
        *buffers.Add() = blockContent;
    }

    return request;
}

std::unique_ptr<TEvService::TEvWriteBlocksLocalRequest>
TVolumeClient::CreateWriteBlocksLocalRequest(
    const TBlockRange64& writeRange,
    const TString& clientId,
    const TString& blockContent)
{
    TSgList sglist;
    sglist.resize(writeRange.Size(), {blockContent.data(), blockContent.size()});

    auto request = std::make_unique<TEvService::TEvWriteBlocksLocalRequest>();
    request->Record.SetStartIndex(writeRange.Start);
    request->Record.MutableHeaders()->SetClientId(clientId);
    request->Record.Sglist = TGuardedSgList(std::move(sglist));
    request->Record.BlocksCount = writeRange.Size();
    request->Record.BlockSize = DefaultBlockSize;
    return request;
}

std::unique_ptr<TEvService::TEvZeroBlocksRequest> TVolumeClient::CreateZeroBlocksRequest(
    const TBlockRange64& zeroRange,
    const TString& clientId)
{
    auto request = std::make_unique<TEvService::TEvZeroBlocksRequest>();
    request->Record.SetStartIndex(zeroRange.Start);
    request->Record.SetBlocksCount(zeroRange.Size());
    request->Record.MutableHeaders()->SetClientId(clientId);

    return request;
}

std::unique_ptr<TEvVolume::TEvDescribeBlocksRequest> TVolumeClient::CreateDescribeBlocksRequest(
    const TBlockRange64& range,
    const TString& clientId,
    ui32 blocksCountToRead)
{
    auto request = std::make_unique<TEvVolume::TEvDescribeBlocksRequest>();
    request->Record.SetStartIndex(range.Start);
    request->Record.SetBlocksCount(range.Size());
    request->Record.SetBlocksCountToRead(blocksCountToRead);
    request->Record.MutableHeaders()->SetClientId(clientId);
    return request;
}

std::unique_ptr<TEvService::TEvCreateCheckpointRequest>
TVolumeClient::CreateCreateCheckpointRequest(
    const TString& checkpointId,
    NProto::ECheckpointType checkpointType)
{
    auto request = std::make_unique<TEvService::TEvCreateCheckpointRequest>();
    request->Record.SetCheckpointId(checkpointId);
    request->Record.SetCheckpointType(checkpointType);
    return request;
}

std::unique_ptr<TEvService::TEvDeleteCheckpointRequest>
TVolumeClient::CreateDeleteCheckpointRequest(
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvService::TEvDeleteCheckpointRequest>();
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvService::TEvGetChangedBlocksRequest>
TVolumeClient::CreateGetChangedBlocksRequest(
    const TBlockRange64& range,
    const TString& lowCheckpointId,
    const TString& highCheckpointId)
{
    auto request = std::make_unique<TEvService::TEvGetChangedBlocksRequest>();
    request->Record.SetStartIndex(range.Start);
    request->Record.SetBlocksCount(range.Size());
    request->Record.SetLowCheckpointId(lowCheckpointId);
    request->Record.SetHighCheckpointId(highCheckpointId);
    return request;
}

std::unique_ptr<TEvVolume::TEvDeleteCheckpointDataRequest>
TVolumeClient::CreateDeleteCheckpointDataRequest(
    const TString& checkpointId)
{
    auto request = std::make_unique<TEvVolume::TEvDeleteCheckpointDataRequest>();
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvService::TEvGetCheckpointStatusRequest>
TVolumeClient::CreateGetCheckpointStatusRequest(const TString& checkpointId)
{
    auto request =
        std::make_unique<TEvService::TEvGetCheckpointStatusRequest>();
    request->Record.SetCheckpointId(checkpointId);
    return request;
}

std::unique_ptr<TEvPartition::TEvBackpressureReport>
TVolumeClient::CreateBackpressureReport(
    const TBackpressureReport& report)
{
    return std::make_unique<TEvPartition::TEvBackpressureReport>(report);
}

std::unique_ptr<TEvVolume::TEvCompactRangeRequest>
TVolumeClient::CreateCompactRangeRequest(
    const TBlockRange64& range,
    const TString& operationId)
{
    auto request = std::make_unique<TEvVolume::TEvCompactRangeRequest>();
    request->Record.SetStartIndex(range.Start);
    request->Record.SetBlocksCount(range.Size());
    request->Record.SetOperationId(operationId);
    return request;
}

std::unique_ptr<TEvVolume::TEvGetCompactionStatusRequest>
TVolumeClient::CreateGetCompactionStatusRequest(
    const TString& operationId)
{
    auto request = std::make_unique<TEvVolume::TEvGetCompactionStatusRequest>();
    request->Record.SetOperationId(operationId);
    return request;
}

std::unique_ptr<TEvVolume::TEvUpdateUsedBlocksRequest>
TVolumeClient::CreateUpdateUsedBlocksRequest(
    const TVector<TBlockRange64>& ranges,
    bool used)
{
    auto request = std::make_unique<TEvVolume::TEvUpdateUsedBlocksRequest>();
    for (const auto& range: ranges) {
        request->Record.AddStartIndices(range.Start);
        request->Record.AddBlockCounts(range.Size());
    }
    request->Record.SetUsed(used);
    return request;
}

std::unique_ptr<NMon::TEvRemoteHttpInfo> TVolumeClient::CreateRemoteHttpInfo(
    const TString& params,
    HTTP_METHOD method)
{
    return std::make_unique<NMon::TEvRemoteHttpInfo>(params, method);
}

std::unique_ptr<TEvVolume::TEvRebuildMetadataRequest>
TVolumeClient::CreateRebuildMetadataRequest(
    NProto::ERebuildMetadataType type,
    ui32 batchSize)
{
    auto request = std::make_unique<TEvVolume::TEvRebuildMetadataRequest>();
    request->Record.SetMetadataType(type);
    request->Record.SetBatchSize(batchSize);
    return request;
}

std::unique_ptr<TEvVolume::TEvGetRebuildMetadataStatusRequest>
TVolumeClient::CreateGetRebuildMetadataStatusRequest()
{
    auto request = std::make_unique<TEvVolume::TEvGetRebuildMetadataStatusRequest>();
    return request;
}

std::unique_ptr<NMon::TEvRemoteHttpInfo> TVolumeClient::CreateRemoteHttpInfo(
    const TString& params)
{
    return std::make_unique<NMon::TEvRemoteHttpInfo>(params);
}

std::unique_ptr<TEvVolume::TEvGetVolumeInfoRequest>
TVolumeClient::CreateGetVolumeInfoRequest()
{
    return std::make_unique<TEvVolume::TEvGetVolumeInfoRequest>();
}

std::unique_ptr<TEvVolume::TEvUpdateVolumeParamsRequest>
TVolumeClient::CreateUpdateVolumeParamsRequest(
    const TString& diskId,
    const THashMap<TString, NProto::TUpdateVolumeParamsMapValue>& volumeParams)
{
    auto request = std::make_unique<TEvVolume::TEvUpdateVolumeParamsRequest>();
    request->Record.SetDiskId(diskId);

    auto* mutableVolumeParams = request->Record.MutableVolumeParams();
    for (const auto& [key, value]: volumeParams) {
        mutableVolumeParams->insert({key, value});
    }

    return request;
}

std::unique_ptr<TEvVolume::TEvChangeStorageConfigRequest> TVolumeClient::CreateChangeStorageConfigRequest(
    NProto::TStorageServiceConfig patch)
{
    auto request = std::make_unique<TEvVolume::TEvChangeStorageConfigRequest>();
    *request->Record.MutableStorageConfig() = std::move(patch);
    return request;
}

std::unique_ptr<TEvVolume::TEvGetStorageConfigRequest> TVolumeClient::CreateGetStorageConfigRequest()
{
    auto request = std::make_unique<TEvVolume::TEvGetStorageConfigRequest>();
    return request;
}

std::unique_ptr<TEvVolumePrivate::TEvDeviceTimedOutRequest>
TVolumeClient::CreateDeviceTimedOutRequest(
    TString deviceUUID)
{
    auto request =
        std::make_unique<TEvVolumePrivate::TEvDeviceTimedOutRequest>(
            std::move(deviceUUID));
    return request;
}

std::unique_ptr<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>
TVolumeClient::CreateUpdateShadowDiskStateRequest(
    TString checkpointId,
    TEvVolumePrivate::TEvUpdateShadowDiskStateRequest::EReason reason,
    ui64 processedBlockCount)
{
    return make_unique<TEvVolumePrivate::TEvUpdateShadowDiskStateRequest>(
        std::move(checkpointId),
        reason,
        processedBlockCount);
}

std::unique_ptr<TEvVolumePrivate::TEvReadMetaHistoryRequest>
TVolumeClient::CreateReadMetaHistoryRequest()
{
    return std::make_unique<TEvVolumePrivate::TEvReadMetaHistoryRequest>();
}

std::unique_ptr<TEvVolume::TEvGracefulShutdownRequest>
TVolumeClient::CreateGracefulShutdownRequest()
{
    return std::make_unique<TEvVolume::TEvGracefulShutdownRequest>();
}

std::unique_ptr<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>
TVolumeClient::CreateLinkLeaderVolumeToFollowerRequest(
    const TLeaderFollowerLink& link)
{
    auto result =
        std::make_unique<TEvVolume::TEvLinkLeaderVolumeToFollowerRequest>();
    result->Record.SetDiskId(link.LeaderDiskId);
    result->Record.SetFollowerDiskId(link.FollowerDiskId);
    result->Record.SetLeaderShardId(link.LeaderShardId);
    result->Record.SetFollowerShardId(link.FollowerShardId);

    return result;
}

std::unique_ptr<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>
TVolumeClient::CreateUnlinkLeaderVolumeFromFollowerRequest(
    const TLeaderFollowerLink& link)
{
    auto result =
        std::make_unique<TEvVolume::TEvUnlinkLeaderVolumeFromFollowerRequest>();
    result->Record.SetDiskId(link.LeaderDiskId);
    result->Record.SetFollowerDiskId(link.FollowerDiskId);
    result->Record.SetLeaderShardId(link.LeaderShardId);
    result->Record.SetFollowerShardId(link.FollowerShardId);
    return result;
}

std::unique_ptr<TEvVolumePrivate::TEvUpdateFollowerStateRequest>
TVolumeClient::CreateUpdateFollowerStateRequest(
    TFollowerDiskInfo followerDiskInfo)
{
    auto result =
        std::make_unique<TEvVolumePrivate::TEvUpdateFollowerStateRequest>(
            std::move(followerDiskInfo));
    return result;
}

void TVolumeClient::SendRemoteHttpInfo(
    const TString& params,
    HTTP_METHOD method)
{
    auto request = CreateRemoteHttpInfo(params, method);
    SendToPipe(std::move(request));
}

void TVolumeClient::SendRemoteHttpInfo(
    const TString& params)
{
    auto request = CreateRemoteHttpInfo(params);
    SendToPipe(std::move(request));
}

std::unique_ptr<NMon::TEvRemoteHttpInfoRes> TVolumeClient::RecvCreateRemoteHttpInfoRes()
{
    return RecvResponse<NMon::TEvRemoteHttpInfoRes>();
}

std::unique_ptr<NMon::TEvRemoteHttpInfoRes> TVolumeClient::RemoteHttpInfo(
    const TString& params,
    HTTP_METHOD method)
{
    auto request = CreateRemoteHttpInfo(params, method);
    SendToPipe(std::move(request));

    auto response = RecvResponse<NMon::TEvRemoteHttpInfoRes>();
    return response;
}

std::unique_ptr<NMon::TEvRemoteHttpInfoRes> TVolumeClient::RemoteHttpInfo(
    const TString& params)
{
    return RemoteHttpInfo(params, HTTP_METHOD::HTTP_METHOD_GET);
}

void TVolumeClient::SendPartitionTabletMetrics(
    ui64 tabletId,
    const NKikimrTabletBase::TMetrics& metrics)
{
    auto request = std::make_unique<NKikimr::TEvLocal::TEvTabletMetrics>(
        tabletId,
        0,
        metrics);
    SendToPipe(std::move(request));
}

////////////////////////////////////////////////////////////////////////////////

std::unique_ptr<TTestActorRuntime> PrepareTestActorRuntime(
    NProto::TStorageServiceConfig storageServiceConfig,
    TDiskRegistryStatePtr diskRegistryState,
    NProto::TFeaturesConfig featuresConfig,
    NRdma::IClientPtr rdmaClient,
    TVector<TDiskAgentStatePtr> diskAgentStates)
{
    const ui32 agentCount = Max<ui32>(diskAgentStates.size(), 1);
    auto runtime = std::make_unique<TTestBasicRuntime>(agentCount);

    runtime->AppendToLogSettings(
        TBlockStoreComponents::START,
        TBlockStoreComponents::END,
        GetComponentName);

    for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
        runtime->SetLogPriority(i, NLog::PRI_INFO);
    }
    // runtime->SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);

    runtime->SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

    runtime->AddLocalService(
        MakeHiveProxyServiceId(),
        TActorSetupCmd(new TFakeHiveProxy(), TMailboxType::Simple, 0));

    runtime->AddLocalService(
        MakeStorageStatsServiceId(),
        NActors::TActorSetupCmd(
            new TFakeStorageStatsService(),
            NActors::TMailboxType::Simple,
            0
        )
    );

    runtime->AddLocalService(
        MakeStorageServiceId(),
        NActors::TActorSetupCmd(
            new TFakeStorageService(),
            NActors::TMailboxType::Simple,
            0
        )
    );

    runtime->AddLocalService(
        MakeSSProxyServiceId(),
        TActorSetupCmd(new TSSProxyMock(), TMailboxType::Simple, 0));

    if (!diskRegistryState) {
        diskRegistryState = MakeIntrusive<TDiskRegistryState>();
    }

    if (diskRegistryState->Devices.empty()) {
        TVector<NProto::TDeviceConfig> devices;

        devices.push_back(MakeDevice("uuid0", "dev0", "transport0"));
        devices.push_back(MakeDevice("uuid1", "dev1", "transport1"));
        devices.push_back(MakeDevice("uuid2", "dev2", "transport2"));

        auto dev0m =
            MakeDevice("uuid0_migration", "dev0_migration", "transport0_migration");
        auto dev2m =
            MakeDevice("uuid2_migration", "dev2_migration", "transport2_migration");

        devices.push_back(dev0m);
        devices.push_back(dev2m);

        diskRegistryState->Devices = std::move(devices);
        diskRegistryState->MigrationDevices["uuid0"] = dev0m;
        diskRegistryState->MigrationDevices["uuid2"] = dev2m;
    }

    for (auto& d: diskRegistryState->Devices) {
        d.SetNodeId(runtime->GetNodeId(d.GetNodeId()));
    }

    for (auto& [_, d]: diskRegistryState->MigrationDevices) {
        d.SetNodeId(runtime->GetNodeId(d.GetNodeId()));
    }

    Sort(
        diskRegistryState->Devices,
        [](const NProto::TDeviceConfig& lhs, const NProto::TDeviceConfig& rhs)
        { return lhs.GetNodeId() < rhs.GetNodeId(); });

    runtime->AddLocalService(
        MakeDiskRegistryProxyServiceId(),
        TActorSetupCmd(
            new TDiskRegistryProxyMock(diskRegistryState),
            TMailboxType::Simple,
            0
        )
    );
    runtime->EnableScheduleForActor(MakeDiskRegistryProxyServiceId());

    SetupTabletServices(*runtime);

    for (ui32 i = 0; i < agentCount; i++) {
        struct TByNodeId
        {
            auto operator()(const NProto::TDeviceConfig& device) const
            {
                return device.GetNodeId();
            }
        };
        const ui32 nodeId = runtime->GetNodeId(i);
        auto begin = LowerBoundBy(
            diskRegistryState->Devices.begin(),
            diskRegistryState->Devices.end(),
            nodeId,
            TByNodeId());
        auto end = UpperBoundBy(
            diskRegistryState->Devices.begin(),
            diskRegistryState->Devices.end(),
            nodeId,
            TByNodeId());

        auto state = diskAgentStates.size() > i ? std::move(diskAgentStates[i])
                                                : TDiskAgentStatePtr();
        const auto actorId = runtime->Register(
            new TDiskAgentMock(
                {
                    begin,
                    end,
                },
                std::move(state)),
            i);
        runtime->RegisterService(
            MakeDiskAgentServiceId(nodeId),
            actorId,
            i);
    }

    auto config = CreateTestStorageConfig(
        std::move(storageServiceConfig),
        std::move(featuresConfig));
    auto diagConfig = CreateTestDiagnosticsConfig();

    auto createFunc = [=](const TActorId& owner, TTabletStorageInfo* info)
    {
        auto tablet = CreateVolumeTablet(
            owner,
            info,
            config,
            diagConfig,
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            CreateTraceSerializer(
                CreateLoggingService("console"),
                "BLOCKSTORE_TRACE",
                NLwTraceMonPage::TraceManager(false)),
            rdmaClient,
            NServer::CreateEndpointEventProxy(),
            EVolumeStartMode::ONLINE,
            {}   // diskId
        );
        return tablet.release();
    };

    for (ui64 volumeTablet: TestVolumeTablets) {
        TTabletStorageInfoPtr info =
            CreateTestTabletInfo(volumeTablet, TTabletTypes::BlockStoreVolume);
        CreateTestBootstrapper(*runtime, info.Get(), createFunc);
    }

    return runtime;
}

TTestRuntimeBuilder& TTestRuntimeBuilder::With(NProto::TStorageServiceConfig config)
{
    StorageServiceConfig = std::move(config);

    return *this;
}

TTestRuntimeBuilder& TTestRuntimeBuilder::With(TDiskRegistryStatePtr state)
{
    DiskRegistryState = std::move(state);

    return *this;
}

std::unique_ptr<TTestActorRuntime> TTestRuntimeBuilder::Build()
{
    return PrepareTestActorRuntime(
        std::move(StorageServiceConfig),
        std::move(DiskRegistryState));
}

////////////////////////////////////////////////////////////////////////////////

NProto::TVolumeClientInfo CreateVolumeClientInfo(
    NProto::EVolumeAccessMode accessMode,
    NProto::EVolumeMountMode mountMode,
    ui32 mountFlags,
    ui64 mountSeqNumber)
{
    NProto::TVolumeClientInfo info;
    info.SetClientId(CreateGuidAsString());
    info.SetVolumeAccessMode(accessMode);
    info.SetMountSeqNumber(mountSeqNumber);
    info.SetVolumeMountMode(mountMode);
    info.SetMountFlags(mountFlags);
    return info;
}

TString BuildRemoteHttpQuery(ui64 tabletId, const TVector<std::pair<TString, TString>>& keyValues)
{
    auto res = TStringBuilder()
        << "/app?TabletID="
        << tabletId;
    for (const auto& p : keyValues) {
        res << "&" << p.first << "=" << p.second;
    }
    return res;
}

void CheckVolumeSendsStatsEvenIfPartitionsAreDead(
    std::unique_ptr<TTestActorRuntime> runtime,
    TVolumeClient& volume,
    ui64 expectedBytesCount,
    bool isReplicatedVolume)
{
    volume.WaitReady();

    ui64 bytesCount = 0;
    ui64 channelHistorySize = 0;
    ui32 partStatsSaved = 0;
    bool stopPartCounters = false;

    auto obs = [&] (TAutoPtr<IEventHandle>& event) {
        if (event->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters &&
            event->Recipient != MakeStorageStatsServiceId() &&
            stopPartCounters)
        {
            return TTestActorRuntime::EEventAction::DROP;
        } else if (event->GetTypeRewrite() == TEvVolumePrivate::EvPartStatsSaved) {
            ++partStatsSaved;
        } else if (event->Recipient == MakeStorageStatsServiceId()
                && event->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters)
        {
            auto* msg = event->Get<TEvStatsService::TEvVolumePartCounters>();

            bytesCount = msg->DiskCounters->Simple.BytesCount.Value;
            channelHistorySize = msg->DiskCounters->Simple.ChannelHistorySize.Value;
        }

        return TTestActorRuntime::DefaultObserverFunc(event);
    };

    runtime->SetObserverFunc(obs);

    auto clientInfo = CreateVolumeClientInfo(
        NProto::VOLUME_ACCESS_READ_WRITE,
        NProto::VOLUME_MOUNT_LOCAL,
        0);
    volume.AddClient(clientInfo);

    runtime->AdvanceCurrentTime(UpdateCountersInterval);
    runtime->DispatchEvents({}, TDuration::Seconds(1));
    runtime->AdvanceCurrentTime(UpdateCountersInterval);
    runtime->DispatchEvents({}, TDuration::Seconds(1));
    runtime->AdvanceCurrentTime(UpdateCountersInterval);
    runtime->DispatchEvents({}, TDuration::Seconds(1));

    UNIT_ASSERT_C(partStatsSaved >= 2, ToString(partStatsSaved));
    UNIT_ASSERT_VALUES_EQUAL(
        expectedBytesCount,
        bytesCount);
    if (isReplicatedVolume) {
        UNIT_ASSERT_VALUES_UNEQUAL(0, channelHistorySize);
    }

    stopPartCounters = true;
    bytesCount = 0;
    channelHistorySize = 0;

    runtime->SetObserverFunc(obs);
    volume.SendToPipe(
        std::make_unique<TEvVolumePrivate::TEvUpdateCounters>()
    );
    runtime->DispatchEvents({}, TDuration::Seconds(1));

    UNIT_ASSERT_VALUES_EQUAL(
        expectedBytesCount,
        bytesCount);
    if (isReplicatedVolume) {
        UNIT_ASSERT_VALUES_UNEQUAL(0, channelHistorySize);
    }

    // partition stats should be sent not just once
    bytesCount = 0;
    channelHistorySize = 0;

    volume.SendToPipe(
        std::make_unique<TEvVolumePrivate::TEvUpdateCounters>()
    );
    runtime->DispatchEvents({}, TDuration::Seconds(1));

    UNIT_ASSERT_VALUES_EQUAL(
        expectedBytesCount,
        bytesCount);
    if (isReplicatedVolume) {
        UNIT_ASSERT_VALUES_UNEQUAL(0, channelHistorySize);
    }
}

void CheckRebuildMetadata(ui32 partCount, ui32 blocksPerStripe)
{
    NProto::TStorageServiceConfig config;
    config.SetMinChannelCount(4);
    auto runtime = PrepareTestActorRuntime(config);

    TVolumeClient volume(*runtime);
    volume.UpdateVolumeConfig(
        0,
        0,
        0,
        0,
        false,
        1,
        NProto::STORAGE_MEDIA_HYBRID,
        7 * 1024,   // block count per partition
        "vol0",
        "cloud",
        "folder",
        partCount,          // partition count
        blocksPerStripe
    );

    volume.WaitReady();

    {
        auto stat = volume.StatVolume();
        const auto& v = stat->Record.GetVolume();
        UNIT_ASSERT_VALUES_EQUAL(v.GetBlocksCount(), partCount * 7 * 1024);
        UNIT_ASSERT_VALUES_EQUAL(v.GetPartitionsCount(), partCount);
    }

    auto clientInfo = CreateVolumeClientInfo(
        NProto::VOLUME_ACCESS_READ_WRITE,
        NProto::VOLUME_MOUNT_LOCAL,
        false);
    volume.AddClient(clientInfo);

    volume.WriteBlocksLocal(
        TBlockRange64::WithLength(0, 1024 * partCount * 7),
        clientInfo.GetClientId(),
        GetBlockContent(1)
    );

    auto response = volume.RebuildMetadata(NProto::ERebuildMetadataType::BLOCK_COUNT, 10);

    auto progress = volume.GetRebuildMetadataStatus();
    UNIT_ASSERT(progress->Record.GetProgress().GetProcessed() != 0);
    UNIT_ASSERT(progress->Record.GetProgress().GetTotal() != 0);
}

}   // namespace NTestVolume

}   // namespace NCloud::NBlockStore::NStorage
