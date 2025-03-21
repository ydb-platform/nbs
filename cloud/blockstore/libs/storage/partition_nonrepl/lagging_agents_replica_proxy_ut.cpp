#include "lagging_agents_replica_proxy_actor.h"
#include "part_mirror_actor.h"
#include "ut_env.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_agent/model/public.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/model/processing_blocks.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/testlib/diagnostics.h>
#include <cloud/blockstore/libs/storage/testlib/disk_agent_mock.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DeviceBlockCount = 8192;
constexpr ui32 DeviceCountPerReplica = 3;
constexpr ui32 ReplicaCount = 3;
constexpr ui32 AgentCount = DeviceCountPerReplica * ReplicaCount;

[[nodiscard]] TBlockRange64 UnifyClosedIntervals(
    const TBlockRange64& first,
    const TBlockRange64& second)
{
    Y_DEBUG_ABORT_UNLESS(
        first.Start - 1 <= second.End && second.Start - 1 <= first.End);
    auto start = Min(first.Start, second.Start);
    auto end = Max(first.End, second.End);
    return TBlockRange64::MakeClosedInterval(start, end);
}

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    TTestBasicRuntime& Runtime;
    const TDevices Devices;
    const TVector<TDevices> Replicas;
    const TMigrations Migrations;
    const bool LocalRequests;
    TStorageConfigPtr Config;
    TActorId MirrorPartActorId;
    TActorId VolumeActorId;
    TStorageStatsServiceStatePtr StorageStatsServiceState;
    TVector<TDiskAgentStatePtr> DiskAgentStates;
    TVector<TActorId> ReplicaActors;
    TVector<TActorId> DiskAgentActors;

    TNonreplicatedPartitionConfigPtr PartConfig;
    THashMap<ui32, TActorId> Controllers;
    TVector<NProto::TLaggingAgent> LaggingAgents;

    static void AddDevice(
        ui32 nodeId,
        ui32 blockCount,
        TString name,
        TString agentId,
        TDevices& devices)
    {
        auto& device = *devices.Add();
        device.SetNodeId(nodeId);
        device.SetBlocksCount(blockCount);
        device.SetDeviceUUID(std::move(name));
        device.SetBlockSize(DefaultBlockSize);
        device.SetAgentId(std::move(agentId));
    }

    static TDevices ReplicaDevices(
        TTestBasicRuntime& runtime,
        ui32 replicaIndex)
    {
        TDevices devices;
        for (ui32 i = 0; i < DeviceCountPerReplica; i++) {
            const ui32 index = replicaIndex * DeviceCountPerReplica + i;
            AddDevice(
                runtime.GetNodeId(index),
                DeviceBlockCount,
                Sprintf("uuid-%u", index),
                Sprintf("agent-%u", index),
                devices);
        }

        return devices;
    }

    explicit TTestEnv(TTestBasicRuntime& runtime, bool localRequests)
        : TTestEnv(
              runtime,
              ReplicaDevices(runtime, 0),
              TVector<TDevices>{
                  ReplicaDevices(runtime, 1),
                  ReplicaDevices(runtime, 2),
              },
              {},   // migrations
              localRequests,
              {},   // freshDeviceIds
              {},   // outdatedDeviceIds
              {}    // configBase
          )
    {}

    TTestEnv(
        TTestBasicRuntime& runtime,
        TDevices devices,
        TVector<TDevices> replicas,
        TMigrations migrations,
        bool localRequests,
        THashSet<TString> freshDeviceIds,
        THashSet<TString> outdatedDeviceIds,
        NProto::TStorageServiceConfig configBase)
        : Runtime(runtime)
        , Devices(std::move(devices))
        , Replicas(std::move(replicas))
        , Migrations(std::move(migrations))
        , LocalRequests(localRequests)
        , MirrorPartActorId(Runtime.GetNodeId(0), "mirror-part")
        , VolumeActorId(Runtime.GetNodeId(0), "volume")
        , StorageStatsServiceState(MakeIntrusive<TStorageStatsServiceState>())
    {
        SetupLogging();

        Runtime.SetRegistrationObserverFunc(
            [&](TTestActorRuntimeBase& runtime,
                const auto& parentId,
                const auto& actorId)
            {
                runtime.EnableScheduleForActor(actorId);
                auto mirrorActorId =
                    runtime.GetLocalServiceId(MirrorPartActorId);
                if (mirrorActorId && parentId == mirrorActorId &&
                    ReplicaActors.size() < 3)
                {
                    AddReplica(actorId);
                }
            });

        auto volume = std::make_unique<TDummyActor>();
        Runtime.AddLocalService(
            VolumeActorId,
            TActorSetupCmd(volume.release(), TMailboxType::Simple, 0));

        auto diskRegistry = std::make_unique<TDummyActor>();
        Runtime.AddLocalService(
            MakeDiskRegistryProxyServiceId(),
            TActorSetupCmd(diskRegistry.release(), TMailboxType::Simple, 0));

        Runtime.AddLocalService(
            MakeStorageStatsServiceId(),
            TActorSetupCmd(
                new TStorageStatsServiceMock(StorageStatsServiceState),
                TMailboxType::Simple,
                0));

        NProto::TStorageServiceConfig storageConfig = std::move(configBase);
        storageConfig.SetMaxMigrationBandwidth(1);
        storageConfig.SetMaxMigrationIoDepth(1);
        storageConfig.SetLaggingDevicePingInterval(
            TDuration::Seconds(5).MilliSeconds());
        Config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig()));

        TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfigInitParams
            params{
                Devices,
                TNonreplicatedPartitionConfig::TVolumeInfo{
                    Now(),
                    // only SSD/HDD distinction matters
                    NProto::STORAGE_MEDIA_SSD_MIRROR3},
                "vol0",
                DefaultBlockSize,
                VolumeActorId};
        params.FreshDeviceIds = std::move(freshDeviceIds);
        params.OutdatedDeviceIds = std::move(outdatedDeviceIds);
        PartConfig =
            std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));

        auto mirrorPartition = std::make_unique<TMirrorPartitionActor>(
            Config,
            CreateDiagnosticsConfig(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            "",   // rwClientId
            PartConfig,
            std::move(migrations),
            Replicas,
            nullptr,   // rdmaClient
            VolumeActorId,
            TActorId()   // resyncActorId
        );

        Runtime.AddLocalService(
            MirrorPartActorId,
            TActorSetupCmd(mirrorPartition.release(), TMailboxType::Simple, 0));

        NKikimr::SetupTabletServices(Runtime);
        TDevices allDevices;
        for (const auto& d: Devices) {
            allDevices.Add()->CopyFrom(d);
        }
        for (const auto& r: Replicas) {
            for (const auto& d: r) {
                allDevices.Add()->CopyFrom(d);
            }
        }
        for (auto& m: migrations) {
            allDevices.Add()->CopyFrom(m.GetTargetDevice());
        }

        Sort(
            allDevices,
            [](const NProto::TDeviceConfig& lhs,
               const NProto::TDeviceConfig& rhs)
            { return lhs.GetNodeId() < rhs.GetNodeId(); });
        for (ui32 i = 0; i < AgentCount; i++) {
            struct TByNodeId
            {
                auto operator()(const NProto::TDeviceConfig& device) const
                {
                    return device.GetNodeId();
                }
            };
            const ui32 nodeId = Runtime.GetNodeId(i);
            auto begin = LowerBoundBy(
                allDevices.begin(),
                allDevices.end(),
                nodeId,
                TByNodeId());
            auto end = UpperBoundBy(
                allDevices.begin(),
                allDevices.end(),
                nodeId,
                TByNodeId());

            const auto actorId = Runtime.Register(
                new TDiskAgentMock(
                    {
                        begin,
                        end,
                    },
                    std::make_shared<TDiskAgentState>()),
                i);
            DiskAgentActors.push_back(actorId);
            Runtime.RegisterService(MakeDiskAgentServiceId(nodeId), actorId, i);
        }
    }

    void SetupLogging()
    {
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        for (ui32 i = TBlockStoreComponents::START;
             i < TBlockStoreComponents::END;
             ++i)
        {
            Runtime.SetLogPriority(i, NLog::PRI_DEBUG);
        }
    }

    void AddReplica(TActorId partActorId)
    {
        ReplicaActors.push_back(partActorId);
    }

    void WriteBlocksToPartition(TBlockRange64 range, char content)
    {
        if (LocalRequests) {
            WriteBlocksLocal(MirrorPartActorId, range, content);
        } else {
            WriteBlocks(MirrorPartActorId, range, content);
        }
    }

    void
    WriteBlocksToReplica(ui32 replicaIndex, TBlockRange64 range, char content)
    {
        if (LocalRequests) {
            WriteBlocksLocal(ReplicaActors[replicaIndex], range, content);
        } else {
            WriteBlocks(ReplicaActors[replicaIndex], range, content);
        }
    }

    void WriteBlocksToController(
        ui32 replicaIndex,
        TBlockRange64 range,
        char content)
    {
        if (LocalRequests) {
            WriteBlocksLocal(
                GetControllerActorId(replicaIndex),
                range,
                content);
        } else {
            WriteBlocks(GetControllerActorId(replicaIndex), range, content);
        }
    }

    void ReadFromPartitionAndCheckContents(TBlockRange64 range, char content)
    {
        ReadAndCheckContents(MirrorPartActorId, range, content);
    }

    void ReadFromControllerAndCheckContents(
        ui32 index,
        TBlockRange64 range,
        char content)
    {
        ReadAndCheckContents(GetControllerActorId(index), range, content);
    }

    void ReadFromReplicaAndCheckContents(
        ui32 replicaIndex,
        TBlockRange64 range,
        char content)
    {
        ReadAndCheckContents(ReplicaActors[replicaIndex], range, content);
    }

    const TDevices& GetReplicaDevices(ui32 replicaIndex)
    {
        if (replicaIndex == 0) {
            return Devices;
        }
        return Replicas[replicaIndex - 1];
    }

    NProto::TLaggingAgent AddLaggingAgent(ui32 nodeId, ui32 replicaIndex)
    {
        const auto& devices = GetReplicaDevices(replicaIndex);
        NProto::TLaggingAgent laggingAgent;
        for (int i = 0; i < devices.size(); i++) {
            const auto& device = devices[i];
            if (device.GetNodeId() == nodeId) {
                laggingAgent.SetAgentId(device.GetAgentId());
                laggingAgent.SetReplicaIndex(0);

                NProto::TLaggingDevice* laggingDevice =
                    laggingAgent.AddDevices();
                laggingDevice->SetDeviceUUID(device.GetDeviceUUID());
                laggingDevice->SetRowIndex(i);
            }
        }

        Y_DEBUG_ABORT_UNLESS(!laggingAgent.GetAgentId().empty());
        LaggingAgents.push_back(laggingAgent);

        TPartitionClient client(Runtime, MirrorPartActorId);
        client.SendRequest(
            GetControllerActorId(replicaIndex),
            std::make_unique<TEvNonreplPartitionPrivate::TEvAgentIsUnavailable>(
                laggingAgent));
        return laggingAgent;
    }

    TActorId GetControllerActorId(ui32 replicaIndex)
    {
        if (!Controllers.contains(replicaIndex)) {
            auto controller =
                std::make_unique<TLaggingAgentsReplicaProxyActor>(
                    Config,
                    CreateDiagnosticsConfig(),
                    PartConfig->Fork(GetReplicaDevices(replicaIndex)),
                    CreateProfileLogStub(),
                    CreateBlockDigestGeneratorStub(),
                    "",   // rwClientId
                    ReplicaActors[replicaIndex],
                    // Normally this would be mirror actor, but for testing
                    // purposes easier to read data from next replica.
                    ReplicaActors[(replicaIndex + 1) % ReplicaCount]);

            const auto actorId = Runtime.Register(controller.release(), 0);
            Runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            return Controllers[replicaIndex] = actorId;
        }
        return Controllers[replicaIndex];
    }

    void DeleteControllerActorId(ui32 replicaIndex)
    {
        if (!Controllers.contains(replicaIndex)) {
            return;
        }

        TPartitionClient client(Runtime, MirrorPartActorId);
        client.SendRequest(
            Controllers[replicaIndex],
            std::make_unique<TEvents::TEvPoisonPill>());
        Controllers.erase(replicaIndex);
    }

    void WaitForMigrationFinishEvent()
    {
        Runtime.AdvanceCurrentTime(Config->GetLaggingDevicePingInterval());
        bool migrationFinished = false;
        for (int i = 0; i < 100; i++) {
            migrationFinished = Runtime.DispatchEvents(
                {.FinalEvents =
                     {{TEvVolumePrivate::EvLaggingAgentMigrationFinished}}},
                TDuration::MilliSeconds(10));
            if (migrationFinished) {
                break;
            }
            Runtime.AdvanceCurrentTime(TDuration::Seconds(4));
        }
        if (!migrationFinished) {
            Runtime.DispatchEvents(
                {.FinalEvents = {
                     {TEvVolumePrivate::EvLaggingAgentMigrationFinished}}});
        }
    }

private:
    std::unique_ptr<TEvService::TEvWriteBlocksResponse>
    WriteBlocks(TActorId actorId, TBlockRange64 range, char content)
    {
        TPartitionClient client(Runtime, MirrorPartActorId);
        auto request = client.CreateWriteBlocksRequest(range, content);
        // request->MutableHeaders()->SetClientId("")
        client.SendRequest(actorId, std::move(request));
        auto response =
            client.RecvResponse<TEvService::TEvWriteBlocksResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            FormatError(response->GetError()));
        return response;
    }

    std::unique_ptr<TEvService::TEvWriteBlocksLocalResponse>
    WriteBlocksLocal(TActorId actorId, TBlockRange64 range, char content)
    {
        TPartitionClient client(Runtime, MirrorPartActorId);
        const TString data(DefaultBlockSize, content);
        auto request = client.CreateWriteBlocksLocalRequest(range, data);
        client.SendRequest(actorId, std::move(request));
        auto response =
            client.RecvResponse<TEvService::TEvWriteBlocksLocalResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());
        return response;
    }

    void
    ReadAndCheckContents(TActorId actorId, TBlockRange64 range, char content)
    {
        const int iterations = actorId == MirrorPartActorId ? ReplicaCount : 1;
        TPartitionClient client(Runtime, MirrorPartActorId);
        for (int i = 0; i < iterations; i++) {
            auto request = client.CreateReadBlocksRequest(range);
            client.SendRequest(actorId, std::move(request));
            auto response =
                client.RecvResponse<TEvService::TEvReadBlocksResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());

            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(range.Size(), blocks.BuffersSize());
            for (ui32 j = 0; j < blocks.BuffersSize(); ++j) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    TString(DefaultBlockSize, content),
                    blocks.GetBuffers(j),
                    TStringBuilder() << "Block number: " << j);
            }
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLaggingAgentsReplicaProxyActorTest)
{
    void ShouldMigrateDirtyBlocks(bool localRequests)
    {
        TTestBasicRuntime runtime(AgentCount);
        TTestEnv env(runtime, localRequests);
        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange = TBlockRange64::WithLength(
            0,
            DeviceBlockCount * DeviceCountPerReplica);
        env.WriteBlocksToPartition(fullDiskRange, 'A');

        // Second row in the first column is lagging.
        env.AddLaggingAgent(runtime.GetNodeId(1), 0);

        // Writes across the first and second devices should write only to the
        // first one.
        const auto firstAndSecondDevices =
            TBlockRange64::WithLength(DeviceBlockCount - 1, 2);
        const auto firstDevice =
            TBlockRange64::WithLength(DeviceBlockCount - 1, 1);
        const auto secondDeviceOneBlock =
            TBlockRange64::WithLength(DeviceBlockCount, 1);
        env.WriteBlocksToController(0, firstAndSecondDevices, 'B');
        env.ReadFromReplicaAndCheckContents(0, firstDevice, 'B');
        env.ReadFromReplicaAndCheckContents(0, secondDeviceOneBlock, 'A');

        // Fill other replicas to validate migration results below.
        env.WriteBlocksToReplica(1, secondDeviceOneBlock, 'B');
        env.WriteBlocksToReplica(2, secondDeviceOneBlock, 'B');

        bool seenHealthCheck = false;
        bool seenMigrationReads = false;
        bool seenMigrationWrites = false;
        bool seenMigrationFinish = false;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvNonreplPartitionPrivate::EvChecksumBlocksRequest: {
                        auto* msg = event->Get<TEvNonreplPartitionPrivate::
                                                   TEvChecksumBlocksRequest>();
                        auto clientId = msg->Record.GetHeaders().GetClientId();
                        if (clientId == CheckHealthClientId) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                env.ReplicaActors[0],
                                event->Recipient);
                            seenHealthCheck = true;
                        }
                        break;
                    }
                    case TEvService::EvReadBlocksRequest: {
                        if (!FindPtr(env.ReplicaActors, event->Recipient)) {
                            break;
                        }
                        auto* msg =
                            event->Get<TEvService::TEvReadBlocksRequest>();
                        auto clientId = msg->Record.GetHeaders().GetClientId();
                        if (clientId == BackgroundOpsClientId) {
                            UNIT_ASSERT_VALUES_EQUAL(
                                ProcessingRangeSize,
                                msg->Record.GetBlocksCount() *
                                    DefaultBlockSize);
                            constexpr ui64 RangeSize =
                                ProcessingRangeSize / DefaultBlockSize;
                            UNIT_ASSERT_VALUES_EQUAL(
                                0,
                                msg->Record.GetStartIndex() % RangeSize);
                            UNIT_ASSERT_VALUES_EQUAL(
                                env.ReplicaActors[1],
                                event->Recipient);
                            seenMigrationReads = true;
                        }
                        break;
                    }
                    case TEvService::EvWriteBlocksRequest: {
                        if (event->Recipient != env.ReplicaActors[0]) {
                            break;
                        }
                        auto* msg =
                            event->Get<TEvService::TEvWriteBlocksRequest>();
                        if (msg->Record.GetHeaders().GetClientId() ==
                            BackgroundOpsClientId)
                        {
                            const auto range =
                                BuildRequestBlockRange(*msg, DefaultBlockSize);
                            UNIT_ASSERT_VALUES_EQUAL(
                                ProcessingRangeSize,
                                range.Size() * DefaultBlockSize);
                            constexpr ui64 RangeSize =
                                ProcessingRangeSize / DefaultBlockSize;
                            UNIT_ASSERT_VALUES_EQUAL(
                                0,
                                range.Start % RangeSize);
                            UNIT_ASSERT(seenMigrationReads);
                            seenMigrationWrites = true;
                        }
                        break;
                    }
                    case TEvVolumePrivate::EvLaggingAgentMigrationFinished: {
                        UNIT_ASSERT(seenMigrationWrites);
                        seenMigrationFinish = true;
                        break;
                    }
                }
                return false;
            });

        runtime.AdvanceCurrentTime(env.Config->GetLaggingDevicePingInterval());
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT(seenHealthCheck);
        UNIT_ASSERT(seenMigrationReads);
        UNIT_ASSERT(seenMigrationWrites);
        UNIT_ASSERT(seenMigrationFinish);
        env.ReadFromReplicaAndCheckContents(0, firstAndSecondDevices, 'B');
    }

    Y_UNIT_TEST(ShouldMigrateDirtyBlocks)
    {
        ShouldMigrateDirtyBlocks(false);
        ShouldMigrateDirtyBlocks(true);
    }

    void ShouldNotWriteToLaggingDevices(bool localRequests)
    {
        TTestBasicRuntime runtime(AgentCount);
        TTestEnv env(runtime, localRequests);
        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange = TBlockRange64::WithLength(
            0,
            DeviceBlockCount * DeviceCountPerReplica);
        env.WriteBlocksToPartition(fullDiskRange, 'A');

        // Second row in the third column is lagging.
        env.AddLaggingAgent(runtime.GetNodeId(7), 2);

        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvWriteBlocksRequest: {
                        UNIT_ASSERT_VALUES_UNEQUAL(
                            event->Recipient,
                            env.ReplicaActors[2]);
                        break;
                    }
                }
                return false;
            });

        // Writes across the first and second devices should write only to the
        // first one.
        const auto secondDeviceStart =
            TBlockRange64::WithLength(DeviceBlockCount, 1);
        const auto secondDeviceEnd =
            TBlockRange64::WithLength(DeviceBlockCount * 2 - 1, 1);
        env.WriteBlocksToController(2, secondDeviceStart, 'B');
        env.WriteBlocksToController(2, secondDeviceEnd, 'C');

        // Write to the first replica for migration validation.
        env.WriteBlocksToReplica(0, secondDeviceStart, 'Y');
        env.WriteBlocksToReplica(0, secondDeviceEnd, 'Z');

        runtime.SetEventFilter([&](TTestActorRuntimeBase&,
                                   TAutoPtr<IEventHandle>&) { return false; });

        // Migration should copy the data from first replica to the third.
        env.WaitForMigrationFinishEvent();
        env.ReadFromReplicaAndCheckContents(2, secondDeviceStart, 'Y');
        env.ReadFromReplicaAndCheckContents(2, secondDeviceEnd, 'Z');
    }

    Y_UNIT_TEST(ShouldNotWriteToLaggingDevices)
    {
        ShouldNotWriteToLaggingDevices(false);
        ShouldNotWriteToLaggingDevices(true);
    }

    void LaggingAgentTwice(bool localRequests)
    {
        TTestBasicRuntime runtime(AgentCount);
        TTestEnv env(runtime, localRequests);
        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange = TBlockRange64::WithLength(
            0,
            DeviceBlockCount * DeviceCountPerReplica);
        env.WriteBlocksToPartition(fullDiskRange, '0');

        // Second row in the first column is lagging.
        auto laggingAgent = env.AddLaggingAgent(runtime.GetNodeId(1), 0);

        // Mark first half as dirty.
        const auto secondDeviceFirstHalf =
            TBlockRange64::WithLength(DeviceBlockCount, DeviceBlockCount / 2);
        env.WriteBlocksToController(0, secondDeviceFirstHalf, 'A');

        bool seenHealthCheck = false;
        bool seenMigrationReads = false;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvReadBlocksRequest: {
                        if (event->Recipient != env.ReplicaActors[0]) {
                            break;
                        }
                        auto* msg =
                            event->Get<TEvService::TEvReadBlocksRequest>();
                        auto clientId = msg->Record.GetHeaders().GetClientId();
                        if (clientId == CheckHealthClientId) {
                            seenHealthCheck = true;
                        } else if (clientId == BackgroundOpsClientId) {
                            UNIT_ASSERT(seenHealthCheck);
                            seenMigrationReads = true;
                        }
                        break;
                    }
                }
                return false;
            });

        // Health check should happen and notify the controller that the replica
        // is back online. Migration should start immediately after that.
        env.WaitForMigrationFinishEvent();

        // The agent is lagging again.
        seenHealthCheck = seenMigrationReads = false;
        env.AddLaggingAgent(runtime.GetNodeId(1), 0);

        // Mark second half as dirty.
        const auto secondDeviceSecondHalf = TBlockRange64::MakeClosedInterval(
            DeviceBlockCount * 1.5,
            DeviceBlockCount * 2 - 1);
        env.WriteBlocksToController(0, secondDeviceSecondHalf, 'B');

        // Initialize good replica to validate migration.
        const auto secondDevice =
            TBlockRange64::WithLength(DeviceBlockCount, DeviceBlockCount);
        env.WriteBlocksToReplica(1, secondDevice, 'C');

        // Wait for migration finish.
        env.WaitForMigrationFinishEvent();

        // Migration should migrate whole second device.
        env.ReadFromReplicaAndCheckContents(0, secondDevice, 'C');

        const auto firstDevice = TBlockRange64::WithLength(0, DeviceBlockCount);
        const auto thirdDevice =
            TBlockRange64::WithLength(DeviceBlockCount * 2, DeviceBlockCount);
        env.ReadFromReplicaAndCheckContents(0, firstDevice, '0');
        env.ReadFromReplicaAndCheckContents(0, thirdDevice, '0');
    }

    Y_UNIT_TEST(LaggingAgentTwice)
    {
        LaggingAgentTwice(false);
        LaggingAgentTwice(true);
    }

    void MultipleLaggingAgentsOnOneReplica(bool localRequests)
    {
        TTestBasicRuntime runtime(AgentCount);
        TTestEnv env(runtime, localRequests);
        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange = TBlockRange64::WithLength(
            0,
            DeviceBlockCount * DeviceCountPerReplica);
        env.WriteBlocksToPartition(fullDiskRange, '0');

        TVector<TActorId> writeBlocksRecipients;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvWriteBlocksLocalRequest:
                    case TEvService::EvWriteBlocksRequest: {
                        if (event->Sender != env.GetControllerActorId(0)) {
                            break;
                        }
                        auto* msg =
                            event->Get<TEvService::TEvReadBlocksRequest>();
                        auto clientId = msg->Record.GetHeaders().GetClientId();
                        if (clientId == BackgroundOpsClientId) {
                            break;
                        }
                        writeBlocksRecipients.push_back(event->Recipient);
                        break;
                    }
                }
                return false;
            });

        // First row in the first column is lagging.
        env.AddLaggingAgent(runtime.GetNodeId(0), 0);

        // Wait for migration finish of the first row.
        env.WaitForMigrationFinishEvent();

        // Third row in the first column is lagging.
        env.AddLaggingAgent(runtime.GetNodeId(2), 0);

        constexpr int HalfRangeCount = DeviceCountPerReplica * 2;
        std::array<TBlockRange64, HalfRangeCount> ranges;
        for (int i = 0; i < HalfRangeCount; i++) {
            ranges[i] = TBlockRange64::WithLength(
                DeviceBlockCount * i / 2,
                DeviceBlockCount / 2);
        }
        // Migrating controller should write data to replica.
        env.WriteBlocksToController(
            0,
            UnifyClosedIntervals(ranges[1], ranges[2]),
            'A');
        env.ReadFromReplicaAndCheckContents(
            0,
            UnifyClosedIntervals(ranges[1], ranges[2]),
            'A');

        // When an agent is unavailable, we just mark the dirty blocks.
        env.WriteBlocksToController(
            0,
            UnifyClosedIntervals(ranges[3], ranges[4]),
            'B');
        env.ReadFromReplicaAndCheckContents(0, ranges[3], 'B');
        env.ReadFromReplicaAndCheckContents(0, ranges[4], '0');

        // Second row in the first column is lagging.
        env.AddLaggingAgent(runtime.GetNodeId(1), 0);

        // Controller should write data to replica with migrating agent and skip
        // unavailable one.
        env.WriteBlocksToController(
            0,
            UnifyClosedIntervals(ranges[1], ranges[2]),
            'C');
        env.ReadFromReplicaAndCheckContents(0, ranges[1], 'C');
        env.ReadFromReplicaAndCheckContents(0, ranges[2], 'A');
    }

    Y_UNIT_TEST(MultipleLaggingAgentsOnOneReplica)
    {
        MultipleLaggingAgentsOnOneReplica(false);
        MultipleLaggingAgentsOnOneReplica(true);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
