#include "part_mirror_actor.h"
#include "ut_env.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/rdma_test/client_test.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/testlib/diagnostics.h>
#include <cloud/blockstore/libs/storage/testlib/disk_agent_mock.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 DeviceBlockCount = 4096;

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

struct TTestEnv
{
    TTestBasicRuntime& Runtime;
    const TDevices Devices;
    const TVector<TDevices> Replicas;
    const TMigrations Migrations;
    TStorageConfigPtr Config;
    TActorId MirrorPartActorId;
    TActorId VolumeActorId;
    TStorageStatsServiceStatePtr StorageStatsServiceState;
    TVector<TDiskAgentStatePtr> DiskAgentStates;
    TVector<TActorId> ReplicaActors;
    TVector<TActorId> DiskAgentActors;
    TVector<NProto::TLaggingAgent> LaggingAgents;
    NRdma::IClientPtr RdmaClient;

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

    static TDevices DefaultDevices(ui64 nodeId, TString agentId)
    {
        TDevices devices;
        AddDevice(nodeId, DeviceBlockCount, "uuid-1", agentId, devices);
        AddDevice(nodeId, DeviceBlockCount, "uuid-2", agentId, devices);
        AddDevice(nodeId, DeviceBlockCount, "uuid-3", agentId, devices);

        return devices;
    }

    static TDevices DefaultReplica(ui64 nodeId, ui32 replicaId, TString agentId)
    {
        auto devices = DefaultDevices(nodeId, std::move(agentId));
        for (auto& device: devices) {
            if (device.GetDeviceUUID()) {
                device.SetDeviceUUID(
                    TStringBuilder()
                    << device.GetDeviceUUID() << "/" << replicaId);
            }
        }
        return devices;
    }

    static TVector<TDevices> OneDevicePerAgentDevices(
        const TTestBasicRuntime& runtime,
        int deviceCountPerReplica)
    {
        TVector<TDevices> replicasDevices;
        TDevices devices;
        for (int i = 0; i < deviceCountPerReplica; i++) {
            TTestEnv::AddDevice(
                runtime.GetNodeId(i),
                DeviceBlockCount,
                Sprintf("uuid-%u", i),
                Sprintf("agent-%u", i),
                devices);
        }
        replicasDevices.push_back(std::move(devices));

        constexpr int ReplicaCount = 2;
        for (int i = 0; i < ReplicaCount; i++) {
            replicasDevices.push_back(TDevices());
            for (int j = 0; j < deviceCountPerReplica; j++) {
                auto& devices = replicasDevices.back();
                const ui32 num = (i + 1) * deviceCountPerReplica + j;
                TTestEnv::AddDevice(
                    runtime.GetNodeId(num),
                    DeviceBlockCount,
                    Sprintf("uuid-%u", num),
                    Sprintf("agent-%u", num),
                    devices);
            }
        }

        return replicasDevices;
    }

    explicit TTestEnv(TTestBasicRuntime& runtime, bool useRdma = false)
        : TTestEnv(
              runtime,
              DefaultDevices(runtime.GetNodeId(0), "agent-1"),
              TVector<TDevices>{
                  DefaultReplica(runtime.GetNodeId(1), 1, "agent-2"),
                  DefaultReplica(runtime.GetNodeId(2), 2, "agent-3"),
              },
              {},   // migrations
              {},   // freshDeviceIds
              {},   // outdatedDeviceIds
              {std::make_shared<TDiskAgentState>(),
               std::make_shared<TDiskAgentState>(),
               std::make_shared<TDiskAgentState>()},
              {},   // configBase
              useRdma)
    {}

    TTestEnv(
            TTestBasicRuntime& runtime,
            TDevices devices,
            TVector<TDevices> replicas,
            TMigrations migrations = {},
            THashSet<TString> freshDeviceIds = {},
            THashSet<TString> outdatedDeviceIds = {},
            TVector<TDiskAgentStatePtr> diskAgentStates = {},
            NProto::TStorageServiceConfig configBase = {},
            bool useRdma = false)
        : Runtime(runtime)
        , Devices(std::move(devices))
        , Replicas(std::move(replicas))
        , Migrations(std::move(migrations))
        , MirrorPartActorId(Runtime.GetNodeId(0), "mirror-part")
        , VolumeActorId(Runtime.GetNodeId(0), "volume")
        , StorageStatsServiceState(MakeIntrusive<TStorageStatsServiceState>())
        , DiskAgentStates(std::move(diskAgentStates))
        , RdmaClient(
              useRdma ? std::make_shared<TRdmaClientTest>()
                      : NRdma::IClientPtr())
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
        storageConfig.SetMaxMigrationIoDepth(4);
        storageConfig.SetLaggingDeviceMaxMigrationIoDepth(4);
        storageConfig.SetLaggingDevicePingInterval(
            TDuration::Seconds(30).MilliSeconds());
        Config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig()));

        TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfigInitParams
            params{
                Devices,
                TNonreplicatedPartitionConfig::TVolumeInfo{
                    .CreationTs = Now(),
                    // only SSD/HDD distinction matters
                    .MediaKind = NProto::STORAGE_MEDIA_SSD_MIRROR3,
                    .EncryptionMode = NProto::NO_ENCRYPTION},
                "vol0",
                DefaultBlockSize,
                VolumeActorId};
        params.FreshDeviceIds = std::move(freshDeviceIds);
        params.OutdatedDeviceIds = std::move(outdatedDeviceIds);
        params.UseSimpleMigrationBandwidthLimiter = false;
        params.LaggingDevicesAllowed = true;
        auto partConfig =
            std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));

        auto mirrorPartition = std::make_unique<TMirrorPartitionActor>(
            Config,
            CreateDiagnosticsConfig(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            "",   // rwClientId
            partConfig,
            Migrations,
            Replicas,
            RdmaClient,
            VolumeActorId,
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
        for (auto& m: Migrations) {
            allDevices.Add()->CopyFrom(m.GetTargetDevice());
        }

        Sort(
            allDevices,
            [](const NProto::TDeviceConfig& lhs,
               const NProto::TDeviceConfig& rhs)
            { return lhs.GetNodeId() < rhs.GetNodeId(); });
        const ui32 agentCount = DiskAgentStates.size();
        for (ui32 i = 0; i < agentCount; i++) {
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
                    DiskAgentStates[i]),
                i);
            DiskAgentActors.push_back(MakeDiskAgentServiceId(nodeId));
            Runtime.RegisterService(MakeDiskAgentServiceId(nodeId), actorId, i);
        }

        if (useRdma) {
            Rdma().InitAllEndpoints();
        }
    }

    TRdmaClientTest& Rdma()
    {
        return static_cast<TRdmaClientTest&>(*RdmaClient);
    }

    void AddReplica(TActorId partActorId)
    {
        ReplicaActors.push_back(partActorId);
    }

    void ReadAndCheckContents(TBlockRange64 range, char content)
    {
        TPartitionClient client(Runtime, MirrorPartActorId);
        for (int i = 0; i < 3; i++) {
            auto response = client.ReadBlocks(range);
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

    void
    ReadAndCheckContents(ui32 replicaIndex, TBlockRange64 range, char content)
    {
        TPartitionClient client(Runtime, ReplicaActors[replicaIndex]);
        auto response = client.ReadBlocks(range);
        const auto& blocks = response->Record.GetBlocks();
        UNIT_ASSERT_VALUES_EQUAL(range.Size(), blocks.BuffersSize());
        for (ui32 j = 0; j < blocks.BuffersSize(); ++j) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                TString(DefaultBlockSize, content),
                blocks.GetBuffers(j),
                TStringBuilder() << "Block number: " << j);
        }
    }

    void ReadFromDiskAgentAndCheckContents(
        ui32 nodeId,
        const TString& deviceId,
        TBlockRange64 range,
        char content)
    {
        auto request =
            std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();

        request->Record.SetStartIndex(range.Start);
        request->Record.SetBlockSize(DefaultBlockSize);
        request->Record.SetDeviceUUID(deviceId);
        request->Record.SetBlocksCount(range.Size());

        auto diskAgentActorId = MakeDiskAgentServiceId(nodeId);
        Runtime.Send(new IEventHandle(
            diskAgentActorId,
            Runtime.AllocateEdgeActor(),
            request.release()));

        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<TEvDiskAgent::TEvReadDeviceBlocksResponse>(
            handle,
            WaitTimeout);

        UNIT_ASSERT(handle);
        auto response =
            handle->Release<TEvDiskAgent::TEvReadDeviceBlocksResponse>();

        const auto& blocks = response->Record.GetBlocks();
        for (ui32 j = 0; j < blocks.BuffersSize(); ++j) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                TString(DefaultBlockSize, content),
                blocks.GetBuffers(j),
                TStringBuilder() << "Block number: " << j);
        }
    }

    void SetupLogging()
    {
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        for (int i = TBlockStoreComponents::START;
             i < TBlockStoreComponents::END;
             ++i)
        {
            Runtime.SetLogPriority(i, NLog::PRI_TRACE);
        }
    }

    void AddLaggingAgent(const TString& agentId)
    {
        NProto::TLaggingAgent laggingAgent;
        for (int i = 0; i < Devices.size(); i++) {
            const auto& device = Devices[i];
            if (device.GetAgentId() == agentId) {
                laggingAgent.SetAgentId(device.GetAgentId());
                laggingAgent.SetReplicaIndex(0);

                NProto::TLaggingDevice* laggingDevice =
                    laggingAgent.AddDevices();
                laggingDevice->SetDeviceUUID(device.GetDeviceUUID());
                laggingDevice->SetRowIndex(i);
            }
        }

        for (size_t replicaIndex = 0; replicaIndex < Replicas.size();
             replicaIndex++)
        {
            for (int i = 0; i < Replicas[replicaIndex].size(); i++) {
                const auto& device = Replicas[replicaIndex][i];
                if (device.GetAgentId() == agentId) {
                    laggingAgent.SetAgentId(device.GetAgentId());
                    laggingAgent.SetReplicaIndex(replicaIndex + 1);

                    NProto::TLaggingDevice* laggingDevice =
                        laggingAgent.AddDevices();
                    laggingDevice->SetDeviceUUID(device.GetDeviceUUID());
                    laggingDevice->SetRowIndex(i);
                }
            }
        }

        Y_DEBUG_ABORT_UNLESS(!laggingAgent.GetAgentId().empty());
        LaggingAgents.push_back(laggingAgent);

        TPartitionClient client(Runtime, MirrorPartActorId);
        client.SendRequest(
            MirrorPartActorId,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvAddLaggingAgentRequest>(
                std::move(laggingAgent)));
        Runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
    }

    void AddLaggingAgent(const NProto::TLaggingAgent& laggingAgent)
    {
        Y_DEBUG_ABORT_UNLESS(!laggingAgent.GetAgentId().empty());
        Y_DEBUG_ABORT_UNLESS(!laggingAgent.GetDevices().empty());

        LaggingAgents.push_back(laggingAgent);
        TPartitionClient client(Runtime, MirrorPartActorId);
        client.SendRequest(
            MirrorPartActorId,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvAddLaggingAgentRequest>(
                laggingAgent));
        Runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
    }

    void RemoveLaggingAgent(const TString& agentId)
    {
        auto* laggingAgent = FindIfPtr(
            LaggingAgents,
            [&agentId](const NProto::TLaggingAgent& laggingAgent)
            { return laggingAgent.GetAgentId() == agentId; });
        Y_DEBUG_ABORT_UNLESS(laggingAgent);

        TPartitionClient client(Runtime, MirrorPartActorId);
        client.SendRequest(
            MirrorPartActorId,
            std::make_unique<
                TEvNonreplPartitionPrivate::TEvRemoveLaggingAgentRequest>(
                *laggingAgent));
    }

    void WaitForMigrationFinishEvent()
    {
        bool migrationFinished = false;
        for (int i = 0; i < 100; i++) {
            migrationFinished = Runtime.DispatchEvents(
                {.FinalEvents = {{TEvDiskRegistry::EvFinishMigrationRequest}}},
                TDuration::MilliSeconds(10));
            if (migrationFinished) {
                break;
            }
            Runtime.AdvanceCurrentTime(TDuration::Seconds(4));
        }
        if (!migrationFinished) {
            Runtime.DispatchEvents(
                {.FinalEvents = {{TEvDiskRegistry::EvFinishMigrationRequest}}});
        }
    }

    void WaitForLaggingMigrationFinishEvent()
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
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMirrorPartitionLaggingDevicesTest)
{
    Y_UNIT_TEST(ShouldMigrateLaggingDevices)
    {
        constexpr ui32 AgentCount = 3;
        TTestBasicRuntime runtime(AgentCount);

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange =
            TBlockRange64::WithLength(0, DeviceBlockCount * 3);
        client.WriteBlocks(fullDiskRange, 'A');

        // The second device in the second replica is lagging.
        env.AddLaggingAgent(env.Replicas[0][1].GetAgentId());

        // Write blocks.
        const auto firstAndSecondDevices =
            TBlockRange64::WithLength(DeviceBlockCount - 1, 2);
        client.WriteBlocks(firstAndSecondDevices, 'B');

        // The second replica is lagging and is untouched.
        env.ReadAndCheckContents(0, firstAndSecondDevices, 'B');
        env.ReadAndCheckContents(1, firstAndSecondDevices, 'A');
        env.ReadAndCheckContents(2, firstAndSecondDevices, 'B');

        env.WaitForLaggingMigrationFinishEvent();

        // After the migration all replicas are in sync.
        env.ReadAndCheckContents(firstAndSecondDevices, 'B');
    }

    Y_UNIT_TEST(ShouldDisableIOForOutdatedDevices)
    {
        constexpr ui32 AgentCount = 3;
        TTestBasicRuntime runtime(AgentCount);

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0), "agent-1"),
            TVector<TDevices>{
                TTestEnv::DefaultReplica(runtime.GetNodeId(1), 1, "agent-2"),
                TTestEnv::DefaultReplica(runtime.GetNodeId(2), 2, "agent-3"),
            },
            {},                       // migrations
            {},                       // freshDeviceIds
            {"uuid-1", "uuid-2/1"},   // outdatedDeviceIds
            {std::make_shared<TDiskAgentState>(),
             std::make_shared<TDiskAgentState>(),
             std::make_shared<TDiskAgentState>()},
            NProto::TStorageServiceConfig{});

        TPartitionClient client(runtime, env.MirrorPartActorId);

        constexpr ui32 RequestSize = 1024;
        const auto firstRow = TBlockRange64::WithLength(0, RequestSize);
        const auto secondRow =
            TBlockRange64::WithLength(DeviceBlockCount, RequestSize);
        const auto thirdRow =
            TBlockRange64::WithLength(DeviceBlockCount * 2, RequestSize);
        const auto secondAndThirdRow =
            TBlockRange64::WithLength((DeviceBlockCount * 2) - 1, RequestSize);

        TVector<TActorId> readActors;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvReadBlocksRequest: {
                        if (event->Recipient == env.MirrorPartActorId) {
                            break;
                        }
                        readActors.push_back(event->Recipient);
                        break;
                    }
                }
                return false;
            });

        // Read blocks three times from the first row. Mirror partition should
        // read from the 1st and 2nd replicas.
        for (int i = 0; i < 3; i++) {
            client.ReadBlocks(firstRow);
        }
        UNIT_ASSERT_VALUES_EQUAL(3, readActors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[1]) +
                Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        // Read blocks three times from the second row. Mirror partition should
        // read from the 0th and 2nd replicas.
        for (int i = 0; i < 3; i++) {
            client.ReadBlocks(secondRow);
        }
        UNIT_ASSERT_VALUES_EQUAL(3, readActors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[0]) +
                Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        // Read blocks three times from the third row. Mirror partition should
        // read once from each replica.
        for (int i = 0; i < 3; i++) {
            client.ReadBlocks(thirdRow);
        }
        UNIT_ASSERT_VALUES_EQUAL(3, readActors.size());
        ASSERT_VECTOR_CONTENTS_EQUAL(env.ReplicaActors, readActors);
        readActors.clear();

        // Read blocks three times from the both second and third rows. Mirror
        // partition should read from the 0th and 2nd replicas.
        for (int i = 0; i < 3; i++) {
            client.ReadBlocks(secondAndThirdRow);
        }
        UNIT_ASSERT_VALUES_EQUAL(3, readActors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[0]) +
                Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        // Write blocks into the first row. The request should be rejected since
        // uuid-1 is lagging.
        {
            client.SendWriteBlocksRequest(firstRow, 'A');
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // Write blocks into the second row. The request should be rejected
        // since uuid-2/1 is lagging.
        {
            client.SendWriteBlocksRequest(secondRow, 'B');
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // Write blocks into the third row.
        {
            client.SendWriteBlocksRequest(thirdRow, 'C');
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // Write blocks into the second and third rows. The request should be
        // rejected since uuid-2/1 is lagging.
        {
            client.SendWriteBlocksRequest(secondAndThirdRow, 'D');
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldNotReadFromLaggingDevices)
    {
        constexpr ui32 AgentCount = 3;
        TTestBasicRuntime runtime(AgentCount);

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange =
            TBlockRange64::WithLength(0, DeviceBlockCount * 3);
        client.WriteBlocks(fullDiskRange, 'A');

        TVector<TActorId> readActors;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvReadBlocksRequest: {
                        if (event->Recipient == env.MirrorPartActorId) {
                            break;
                        }
                        readActors.push_back(event->Recipient);
                        break;
                    }
                }
                return false;
            });

        // Read blocks three times. Mirror partition should read one time from
        // each replica.
        env.ReadAndCheckContents(fullDiskRange, 'A');
        UNIT_ASSERT_VALUES_EQUAL(3, readActors.size());
        UNIT_ASSERT_VALUES_EQUAL(env.ReplicaActors.size(), readActors.size());
        ASSERT_VECTOR_CONTENTS_EQUAL(env.ReplicaActors, readActors);
        readActors.clear();

        // First replica is lagging.
        env.AddLaggingAgent(env.Devices[0].GetAgentId());

        // Read blocks three times. We should not read from the lagging replica.
        env.ReadAndCheckContents(fullDiskRange, 'A');
        UNIT_ASSERT_VALUES_EQUAL(3, readActors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[1]) +
                Count(readActors, env.ReplicaActors[2]));
    }

    Y_UNIT_TEST(ShouldNotWriteOnLaggingAgents)
    {
        constexpr ui32 AgentCount = 3;
        TTestBasicRuntime runtime(AgentCount);

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange =
            TBlockRange64::WithLength(0, DeviceBlockCount * 3);
        client.WriteBlocks(fullDiskRange, 'A');

        TVector<TActorId> readActors;
        TVector<TActorId> writeDiskAgentActors;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvReadBlocksRequest: {
                        if (event->Recipient == env.MirrorPartActorId) {
                            break;
                        }
                        readActors.push_back(event->Recipient);
                        break;
                    }
                    case TEvDiskAgent::EvWriteDeviceBlocksRequest: {
                        writeDiskAgentActors.push_back(event->Recipient);
                        break;
                    }
                }
                return false;
            });

        // First replica is lagging.
        env.AddLaggingAgent(env.Replicas[0][0].GetAgentId());

        constexpr ui32 RequestSize = 1024;
        const auto firstRow = TBlockRange64::WithLength(0, RequestSize);
        const auto secondRow =
            TBlockRange64::WithLength(DeviceBlockCount, RequestSize);
        const auto thirdRow =
            TBlockRange64::WithLength(DeviceBlockCount * 2, RequestSize);
        const auto secondAndThirdRow =
            TBlockRange64::WithLength((DeviceBlockCount * 2) - 1, RequestSize);

        client.WriteBlocks(firstRow, 'B');
        UNIT_ASSERT_VALUES_EQUAL(2, writeDiskAgentActors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            writeDiskAgentActors.size(),
            Count(writeDiskAgentActors, env.DiskAgentActors[0]) +
                Count(writeDiskAgentActors, env.DiskAgentActors[2]));
        env.ReadAndCheckContents(firstRow, 'B');
        writeDiskAgentActors.clear();

        client.WriteBlocks(secondRow, 'C');
        UNIT_ASSERT_VALUES_EQUAL(2, writeDiskAgentActors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            writeDiskAgentActors.size(),
            Count(writeDiskAgentActors, env.DiskAgentActors[0]) +
                Count(writeDiskAgentActors, env.DiskAgentActors[2]));
        env.ReadAndCheckContents(secondRow, 'C');
        writeDiskAgentActors.clear();

        client.WriteBlocks(thirdRow, 'D');
        UNIT_ASSERT_VALUES_EQUAL(2, writeDiskAgentActors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            writeDiskAgentActors.size(),
            Count(writeDiskAgentActors, env.DiskAgentActors[0]) +
                Count(writeDiskAgentActors, env.DiskAgentActors[2]));
        env.ReadAndCheckContents(thirdRow, 'D');
        writeDiskAgentActors.clear();

        client.WriteBlocks(secondAndThirdRow, 'E');
        UNIT_ASSERT_VALUES_EQUAL(4, writeDiskAgentActors.size());
        UNIT_ASSERT_VALUES_EQUAL(
            writeDiskAgentActors.size(),
            Count(writeDiskAgentActors, env.DiskAgentActors[0]) +
                Count(writeDiskAgentActors, env.DiskAgentActors[2]));
        env.ReadAndCheckContents(secondAndThirdRow, 'E');
        writeDiskAgentActors.clear();
    }

    Y_UNIT_TEST(ShouldAddAndRemoveLaggingAgents)
    {
        constexpr ui32 AgentCount = 10;
        TTestBasicRuntime runtime(AgentCount);

        TVector<TDiskAgentStatePtr> diskAgentStates;
        for (ui32 i = 0; i < AgentCount; i++) {
            diskAgentStates.push_back(std::make_shared<TDiskAgentState>());
        }

        auto replicasDevices = TTestEnv::OneDevicePerAgentDevices(
            runtime,
            3   // deviceCountPerReplica
        );

        TMigrations migrations;
        {
            NProto::TDeviceMigration* migration = migrations.Add();
            migration->SetSourceDeviceId("uuid-3");

            auto& targetDevice = *migration->MutableTargetDevice();
            targetDevice.SetNodeId(runtime.GetNodeId(AgentCount - 1));
            targetDevice.SetBlocksCount(DeviceBlockCount);
            targetDevice.SetDeviceUUID("uuid-m");
            targetDevice.SetBlockSize(DefaultBlockSize);
            targetDevice.SetAgentId("agent-m");
        }

        TTestEnv env(
            runtime,
            std::move(replicasDevices[0]),
            {std::move(replicasDevices[1]), std::move(replicasDevices[2])},
            std::move(migrations),
            {"uuid-7"},   // freshDeviceIds
            {},           // outdatedDeviceIds
            std::move(diskAgentStates),
            {});

        // Migrate all the ranges.
        env.WaitForMigrationFinishEvent();

        // Current disk config:
        // ┌──────────────────┬──────────────────┬────────────────┐
        // │ uuid-0           │ uuid-3 -> uuid-m │ uuid-6         │
        // │──────────────────┼──────────────────┼────────────────│
        // │ uuid-1           │ uuid-4           │ uuid-7 (Fresh) │
        // │──────────────────┼──────────────────┼────────────────│
        // │ uuid-2           │ uuid-5           │ uuid-8         │
        // └──────────────────┴──────────────────┴────────────────┘

        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange =
            TBlockRange64::WithLength(0, DeviceBlockCount * 3);
        client.WriteBlocks(fullDiskRange, 'A');

        TVector<TActorId> mirrorActorChildren;
        runtime.SetRegistrationObserverFunc(
            [&](TTestActorRuntimeBase&,
                const auto& parentId,
                const auto& actorId)
            {
                runtime.EnableScheduleForActor(actorId);
                auto mirrorActorId =
                    runtime.GetLocalServiceId(env.MirrorPartActorId);
                Y_DEBUG_ABORT_UNLESS(mirrorActorId);
                if (parentId == mirrorActorId) {
                    mirrorActorChildren.push_back(actorId);
                }
            });

        TVector<TActorId> readActors;
        TSet<TString> unavailableAgents;
        size_t unavailableAgentsRequestCount = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvReadBlocksRequest: {
                        if (event->Recipient == env.MirrorPartActorId) {
                            break;
                        }
                        readActors.push_back(event->Recipient);
                        break;
                    }
                    case TEvNonreplPartitionPrivate::EvAgentIsUnavailable: {
                        const auto* msg =
                            event->Get<TEvNonreplPartitionPrivate::
                                           TEvAgentIsUnavailable>();
                        unavailableAgents.insert(msg->LaggingAgent.GetAgentId());
                        ++unavailableAgentsRequestCount;
                        break;
                    }
                    case TEvents::TEvPoisonPill::EventType: {
                        auto it = Find(mirrorActorChildren, event->Recipient);
                        if (it != mirrorActorChildren.end()) {
                            mirrorActorChildren.erase(it);
                        }
                        break;
                    }
                }
                return false;
            });

        runtime.SetScheduledEventFilter(
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event,
                auto&& delay,
                auto&& deadline)
            {
                Y_UNUSED(runtime);
                Y_UNUSED(delay);
                Y_UNUSED(deadline);
                switch (event->GetTypeRewrite()) {
                    case TEvents::TEvPoisonPill::EventType: {
                        auto it = Find(mirrorActorChildren, event->Recipient);
                        if (it != mirrorActorChildren.end()) {
                            mirrorActorChildren.erase(it);
                        }
                        break;
                    }
                }
                return false;
            });

        // uuid-7 is lagging
        env.AddLaggingAgent("agent-7");
        // 1 from mirror partition, 1 from lagging proxy, 2 from migration
        // partition
        UNIT_ASSERT_VALUES_EQUAL(1, unavailableAgents.size());
        UNIT_ASSERT_VALUES_EQUAL(6, unavailableAgentsRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, mirrorActorChildren.size());
        TActorId replica2Proxy = mirrorActorChildren.back();
        unavailableAgents.clear();
        unavailableAgentsRequestCount = 0;

        // uuid-8 is lagging
        env.AddLaggingAgent("agent-8");
        UNIT_ASSERT_VALUES_EQUAL(1, unavailableAgents.size());
        UNIT_ASSERT_VALUES_EQUAL(6, unavailableAgentsRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(1, mirrorActorChildren.size());
        unavailableAgents.clear();
        unavailableAgentsRequestCount = 0;

        // uuid-3 is lagging
        env.AddLaggingAgent("agent-3");
        UNIT_ASSERT_VALUES_EQUAL(1, unavailableAgents.size());
        UNIT_ASSERT_VALUES_EQUAL(7, unavailableAgentsRequestCount);
        UNIT_ASSERT_VALUES_EQUAL(2, mirrorActorChildren.size());
        TActorId replica1Proxy = mirrorActorChildren.back();
        unavailableAgents.clear();
        unavailableAgentsRequestCount = 0;

        // The first and second replicas
        const auto firstRow = TBlockRange64::MakeOneBlock(DeviceBlockCount - 1);
        const auto secondRow = TBlockRange64::MakeOneBlock(DeviceBlockCount);
        const auto firstAndSecondRows =
            UnifyClosedIntervals(firstRow, secondRow);
        client.WriteBlocks(firstAndSecondRows, 'B');
        env.ReadAndCheckContents(firstRow, 'B');
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[0]) +
                Count(readActors, env.ReplicaActors[2]));
        readActors.clear();
        env.ReadAndCheckContents(secondRow, 'B');
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[0]) +
                Count(readActors, env.ReplicaActors[1]));
        readActors.clear();

        env.ReadAndCheckContents(
            TBlockRange64::MakeOneBlock(firstRow.Start - 1),
            'A');
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[0]) +
                Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        env.ReadAndCheckContents(
            TBlockRange64::MakeOneBlock(secondRow.End + 1),
            'A');
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[0]) +
                Count(readActors, env.ReplicaActors[1]));
        readActors.clear();

        // uuid-3 is ok now.
        env.RemoveLaggingAgent("agent-3");
        UNIT_ASSERT(FindPtr(mirrorActorChildren, replica1Proxy));
        env.ReadAndCheckContents(0, firstRow, 'B');
        env.ReadAndCheckContents(2, firstRow, 'B');
        // Lagging device still has old data.
        env.ReadAndCheckContents(1, firstRow, 'A');
        // Second row is ok.
        env.ReadAndCheckContents(secondRow, 'B');
        // Fix the discrepancy.
        client.WriteBlocks(firstRow, 'B');

        readActors.clear();
        env.ReadAndCheckContents(firstAndSecondRows, 'B');
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            Count(readActors, env.ReplicaActors[0]) +
                Count(readActors, env.ReplicaActors[1]));
        readActors.clear();

        // Poison pill has killed the proxy actor.
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT(!FindPtr(mirrorActorChildren, replica1Proxy));

        // uuid-7 is ok now.
        env.RemoveLaggingAgent("agent-7");
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT(FindPtr(mirrorActorChildren, replica2Proxy));

        env.ReadAndCheckContents(TBlockRange64::MakeOneBlock(0), 'A');
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[0]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[1]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        // uuid-8 is ok now.
        env.RemoveLaggingAgent("agent-8");

        // Poison pill has killed the proxy actor.
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT(!FindPtr(mirrorActorChildren, replica2Proxy));
    }

    Y_UNIT_TEST(ShouldHandleLaggingTargetMigration)
    {
        constexpr ui32 AgentCount = 10;
        TTestBasicRuntime runtime(AgentCount);

        TVector<TDiskAgentStatePtr> diskAgentStates;
        for (ui32 i = 0; i < AgentCount; i++) {
            diskAgentStates.push_back(std::make_shared<TDiskAgentState>());
        }

        auto replicasDevices = TTestEnv::OneDevicePerAgentDevices(
            runtime,
            3   // deviceCountPerReplica
        );

        TMigrations migrations;
        {
            NProto::TDeviceMigration* migration = migrations.Add();
            migration->SetSourceDeviceId("uuid-4");

            auto& targetDevice = *migration->MutableTargetDevice();
            targetDevice.SetNodeId(runtime.GetNodeId(AgentCount - 1));
            targetDevice.SetBlocksCount(DeviceBlockCount);
            targetDevice.SetDeviceUUID("uuid-m");
            targetDevice.SetBlockSize(DefaultBlockSize);
            targetDevice.SetAgentId("agent-m");
        }

        TTestEnv env(
            runtime,
            std::move(replicasDevices[0]),
            {std::move(replicasDevices[1]), std::move(replicasDevices[2])},
            std::move(migrations),
            {},   // freshDeviceIds
            {},   // outdatedDeviceIds
            std::move(diskAgentStates),
            {});

        // Migrate all the ranges.
        env.WaitForMigrationFinishEvent();

        // Current disk config:
        // ┌──────────────────┬──────────────────┬────────────────┐
        // │ uuid-0           │ uuid-3           │ uuid-6         │
        // │──────────────────┼──────────────────┼────────────────│
        // │ uuid-1           │ uuid-4 -> uuid-m │ uuid-7         │
        // │──────────────────┼──────────────────┼────────────────│
        // │ uuid-2           │ uuid-5           │ uuid-8         │
        // └──────────────────┴──────────────────┴────────────────┘

        TPartitionClient client(runtime, env.MirrorPartActorId);

        const auto fullDiskRange =
            TBlockRange64::WithLength(0, DeviceBlockCount * 3);
        client.WriteBlocks(fullDiskRange, 'A');

        TVector<TActorId> readActors;
        THashSet<TString> migrationDisabledAgents;
        bool laggingAgentMigrationFinished = false;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvReadBlocksRequest: {
                        if (event->Recipient == env.MirrorPartActorId) {
                            break;
                        }
                        readActors.push_back(event->Recipient);
                        break;
                    }
                    case TEvNonreplPartitionPrivate::
                        EvLaggingMigrationDisabled: {
                        const auto* msg =
                            event->Get<TEvNonreplPartitionPrivate::
                                           TEvLaggingMigrationDisabled>();
                        migrationDisabledAgents.insert(msg->AgentId);
                        break;
                    }
                    case TEvNonreplPartitionPrivate::
                        EvLaggingMigrationEnabled: {
                        const auto* msg =
                            event->Get<TEvNonreplPartitionPrivate::
                                           TEvLaggingMigrationEnabled>();
                        migrationDisabledAgents.erase(msg->AgentId);
                        break;
                    }
                    case TEvVolumePrivate::EvLaggingAgentMigrationFinished: {
                        laggingAgentMigrationFinished = true;
                        break;
                    }
                }
                return false;
            });

        // uuid-m is lagging
        NProto::TLaggingAgent laggingAgent;
        {
            laggingAgent.SetAgentId("agent-m");
            laggingAgent.SetReplicaIndex(1);
            auto* laggingDevice = laggingAgent.MutableDevices()->Add();
            laggingDevice->SetDeviceUUID("uuid-m");
            laggingDevice->SetRowIndex(1);
        }
        env.AddLaggingAgent(laggingAgent);
        UNIT_ASSERT(migrationDisabledAgents.contains("agent-m"));
        UNIT_ASSERT_VALUES_EQUAL(1, migrationDisabledAgents.size());

        // The first and second replicas
        const auto firstRow = TBlockRange64::MakeOneBlock(0);
        const auto secondRow = TBlockRange64::MakeOneBlock(DeviceBlockCount);
        const auto thirdRow = TBlockRange64::MakeOneBlock(DeviceBlockCount * 2);

        client.WriteBlocks(firstRow, 'B');
        client.WriteBlocks(secondRow, 'B');
        client.WriteBlocks(thirdRow, 'B');

        // Reads should read from all 3 replicas.
        env.ReadAndCheckContents(firstRow, 'B');
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[0]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[1]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        env.ReadAndCheckContents(secondRow, 'B');
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[0]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[1]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        env.ReadAndCheckContents(thirdRow, 'B');
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[0]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[1]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        // Write to two devices at the same time.
        const auto firstAndSecondRow =
            TBlockRange64::WithLength(DeviceBlockCount - 1, 2);
        const auto secondAndThirdRow =
            TBlockRange64::WithLength(DeviceBlockCount * 2 - 1, 2);
        client.WriteBlocks(firstAndSecondRow, 'C');
        client.WriteBlocks(secondAndThirdRow, 'C');

        // Data is ok.
        env.ReadAndCheckContents(firstAndSecondRow, 'C');
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[0]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[1]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        env.ReadAndCheckContents(secondAndThirdRow, 'C');
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[0]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[1]));
        UNIT_ASSERT_VALUES_EQUAL(1, Count(readActors, env.ReplicaActors[2]));
        readActors.clear();

        // Check the data on disk agents. "uuid-m" should have obsolete data
        // since the writes to it are disabled.
        const auto deviceStart = TBlockRange64::MakeOneBlock(0);
        const auto deviceEnd =
            TBlockRange64::MakeOneBlock(DeviceBlockCount - 1);
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(AgentCount - 1),
            "uuid-m",
            deviceStart,
            'A');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(AgentCount - 1),
            "uuid-m",
            deviceEnd,
            'A');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(3),
            "uuid-3",
            deviceStart,
            'B');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(3),
            "uuid-3",
            deviceEnd,
            'C');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(4),
            "uuid-4",
            deviceStart,
            'C');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(4),
            "uuid-4",
            deviceEnd,
            'C');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(5),
            "uuid-5",
            deviceStart,
            'C');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(5),
            "uuid-5",
            deviceEnd,
            'A');

        // Wait until writes to "uuid-m" are enabled.
        runtime.AdvanceCurrentTime(env.Config->GetLaggingDevicePingInterval());
        runtime.DispatchEvents(
            {.FinalEvents = {
                 {TEvNonreplPartitionPrivate::EvAgentIsBackOnline}}});
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(0, migrationDisabledAgents.size());

        // Now user writes should be delivered to "uuid-m".
        client.WriteBlocks(firstAndSecondRow, 'D');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(AgentCount - 1),
            "uuid-m",
            deviceStart,
            'D');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(3),
            "uuid-3",
            deviceEnd,
            'D');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(4),
            "uuid-4",
            deviceStart,
            'D');

        if (!laggingAgentMigrationFinished) {
            env.WaitForLaggingMigrationFinishEvent();
        }

        // Lagging migration should catch up the data.
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(AgentCount - 1),
            "uuid-m",
            deviceStart,
            'D');
        env.ReadFromDiskAgentAndCheckContents(
            runtime.GetNodeId(AgentCount - 1),
            "uuid-m",
            deviceEnd,
            'C');
    }

    void DoShouldCancelWriteRequestsOnAllReplicas(bool useRdma)
    {
        constexpr ui32 AgentCount = 3;
        TTestBasicRuntime runtime(AgentCount);

        TTestEnv env(runtime, useRdma);

        TPartitionClient client(runtime, env.MirrorPartActorId);

        size_t writeDeviceBlocksRequestCount = 0;
        const auto timeoutWriteDeviceBlocksRequests =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvDiskAgent::EvWriteDeviceBlocksRequest: {
                    ++writeDeviceBlocksRequestCount;
                    return true;
                }
            }
            return false;
        };
        runtime.SetEventFilter(timeoutWriteDeviceBlocksRequests);

        auto promise = NThreading::NewPromise();
        if (useRdma) {
            env.Rdma().InjectFutureToWaitBeforeRequestProcessing(
                promise.GetFuture());
        }

        // Start write request.
        client.SendWriteBlocksRequest(TBlockRange64::MakeOneBlock(100), 'B');
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(useRdma ? 0 : 3, writeDeviceBlocksRequestCount);

        size_t cancelledRequestCount = 0;
        auto countCanceledResponses =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvService::EvWriteBlocksResponse: {
                    using TResponse = TEvService::TEvWriteBlocksResponse;
                    const auto& record = event->Get<TResponse>()->Record;
                    if (record.GetError().GetCode() == E_REJECTED &&
                        HasProtoFlag(
                            record.GetError().GetFlags(),
                            NProto::EF_INSTANT_RETRIABLE))
                    {
                        ++cancelledRequestCount;
                    }
                    break;
                }
            }
            return false;
        };

        runtime.SetEventFilter(countCanceledResponses);

        // First replica is lagging.
        env.AddLaggingAgent(env.Replicas[0][0].GetAgentId());

        // Write request cancelled.
        auto response = client.RecvWriteBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            FormatError(response->GetError()));
        UNIT_ASSERT_C(
            HasProtoFlag(response->GetError().GetFlags(), NProto::EF_INSTANT_RETRIABLE),
            FormatError(response->GetError()));

        // Expect all WriteDeviceBlocksRequest requests to be cancelled.
        // 3 from TNonreplicatedPartitionActor, 1 from TMirrorPartitionActor
        UNIT_ASSERT_VALUES_EQUAL(4, cancelledRequestCount);
    }

    Y_UNIT_TEST(ShouldCancelWriteRequestsOnAllReplicas)
    {
        DoShouldCancelWriteRequestsOnAllReplicas(false);
    }

    Y_UNIT_TEST(ShouldCancelWriteRequestsOnAllReplicasRdma)
    {
        DoShouldCancelWriteRequestsOnAllReplicas(true);
    }

    void DoShouldCancelReadRequestsOnlyForLaggingReplica(bool useRdma)
    {
        constexpr ui32 AgentCount = 3;
        TTestBasicRuntime runtime(AgentCount);

        TTestEnv env(runtime, useRdma);

        TPartitionClient client(runtime, env.MirrorPartActorId);

        size_t readDeviceBlocksRequestCount = 0;
        const auto timeoutReadDeviceBlocksRequests =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvDiskAgent::EvReadDeviceBlocksRequest: {
                    ++readDeviceBlocksRequestCount;
                    return true;
                }
            }
            return false;
        };
        runtime.SetEventFilter(timeoutReadDeviceBlocksRequests);

        auto promise = NThreading::NewPromise();
        if (useRdma) {
            env.Rdma().InjectFutureToWaitBeforeRequestProcessing(
                promise.GetFuture());
        }

        // Start three read request to to different replicas.
        client.SendReadBlocksRequest(TBlockRange64::MakeOneBlock(100));
        client.SendReadBlocksRequest(TBlockRange64::MakeOneBlock(100));
        client.SendReadBlocksRequest(TBlockRange64::MakeOneBlock(100));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(useRdma ? 0 : 3, readDeviceBlocksRequestCount);

        size_t cancelledRequestCount = 0;
        auto countCanceledResponses =
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvService::EvReadBlocksResponse: {
                    using TResponse = TEvService::TEvReadBlocksResponse;
                    const auto& record = event->Get<TResponse>()->Record;
                    if (record.GetError().GetCode() == E_REJECTED &&
                        HasProtoFlag(
                            record.GetError().GetFlags(),
                            NProto::EF_INSTANT_RETRIABLE))
                    {
                        ++cancelledRequestCount;
                    }
                    break;
                }
            }
            return false;
        };

        runtime.SetEventFilter(countCanceledResponses);

        // First replica is lagging.
        env.AddLaggingAgent(env.Replicas[0][0].GetAgentId());

        // one Read request cancelled.
        auto response = client.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            FormatError(response->GetError()));
        UNIT_ASSERT_C(
            HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_INSTANT_RETRIABLE),
            FormatError(response->GetError()));

        // Expect one ReadDeviceBlocksRequest requests to be cancelled.
        // 1 from TNonreplicatedPartitionActor, 1 from TMirrorPartitionActor
        UNIT_ASSERT_VALUES_EQUAL(2, cancelledRequestCount);

        if (useRdma) {
            // RDMA has a different strategy for handling timeouts, so we won't
            // get responses to hung requests.
            return;
        }

        // Expect requests to second ant third replicas timedout since we stole
        // response
        response = client.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            FormatError(response->GetError()));
        UNIT_ASSERT_C(
            !HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_INSTANT_RETRIABLE),
            FormatError(response->GetError()));

        response = client.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_REJECTED,
            response->GetStatus(),
            FormatError(response->GetError()));
        UNIT_ASSERT_C(
            !HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_INSTANT_RETRIABLE),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldCancelReadRequestsOnlyForLaggingReplica)
    {
        DoShouldCancelReadRequestsOnlyForLaggingReplica(false);
    }

    Y_UNIT_TEST(ShouldCancelReadRequestsOnlyForLaggingReplicaRdma)
    {
        DoShouldCancelReadRequestsOnlyForLaggingReplica(true);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
