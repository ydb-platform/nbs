#include "part_mirror.h"
#include "part_mirror_actor.h"
#include "part_nonrepl_actor.h"
#include "ut_env.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/critical_events.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/disk_registry_proxy.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
#include <cloud/blockstore/libs/storage/testlib/diagnostics.h>
#include <cloud/blockstore/libs/storage/testlib/disk_agent_mock.h>

#include <cloud/storage/core/libs/common/sglist_test.h>

#include <contrib/ydb/core/testlib/basics/runtime.h>
#include <contrib/ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/string/builder.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

class TTestRuntime: public TTestBasicRuntime
{
public:
    TTestRuntime()
    {
        SetRegistrationObserverFunc(
            [](auto& runtime, const auto& parentId, const auto& actorId)
            {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });
    }
};

struct TTestEnv
{
    TTestActorRuntime& Runtime;
    TStorageConfigPtr Config;
    TActorId ActorId;
    TActorId VolumeActorId;
    TStorageStatsServiceStatePtr StorageStatsServiceState;
    TDiskAgentStatePtr DiskAgentState;
    TVector<TActorId> ReplicaActors;
    TDuration ScrubbingInterval;

    static void AddDevice(
        ui32 nodeId,
        ui32 blockCount,
        TString name,
        TDevices& devices)
    {
        const auto k = DefaultBlockSize / DefaultDeviceBlockSize;

        auto& device = *devices.Add();
        device.SetNodeId(nodeId);
        device.SetBlocksCount(blockCount * k);
        device.SetDeviceUUID(name);
        device.SetBlockSize(DefaultDeviceBlockSize);
    }

    static TDevices DefaultDevices(ui64 nodeId)
    {
        TDevices devices;
        AddDevice(nodeId, 2048, "vasya", devices);
        AddDevice(nodeId, 3072, "petya", devices);
        AddDevice(0, 1024, "", devices);

        return devices;
    }

    static TDevices DefaultReplica(ui64 nodeId, ui32 replicaId)
    {
        auto devices = DefaultDevices(nodeId);
        for (auto& device: devices) {
            if (device.GetDeviceUUID()) {
                device.SetDeviceUUID(TStringBuilder() << device.GetDeviceUUID()
                    << "#" << replicaId);
            }
        }
        return devices;
    }

    static TMigrations DefaultMigrations(ui64 nodeId, ui32 replicaId)
    {
        TMigrations migrations;
        auto devices = replicaId
            ? DefaultReplica(nodeId, replicaId)
            : DefaultDevices(nodeId);
        for (auto& device: devices) {
            if (device.GetDeviceUUID()) {
                auto* m = migrations.Add();
                m->SetSourceDeviceId(device.GetDeviceUUID());
                m->MutableTargetDevice()->CopyFrom(device);
                m->MutableTargetDevice()->SetDeviceUUID(
                    TStringBuilder() << device.GetDeviceUUID() << "-migration");
            }
        }
        return migrations;
    }

    explicit TTestEnv(
            TTestActorRuntime& runtime,
            NProto::TStorageServiceConfig configBase = {})
        : TTestEnv(
            runtime,
            DefaultDevices(runtime.GetNodeId(0)),
            TVector<TDevices>{
                DefaultReplica(runtime.GetNodeId(0), 1),
                DefaultReplica(runtime.GetNodeId(0), 2),
            },
            {}, // migrations
            {}, // freshDeviceIds
            std::move(configBase)
        )
    {
    }

    TTestEnv(
            TTestActorRuntime& runtime,
            TDevices devices,
            TVector<TDevices> replicas,
            TMigrations migrations = {},
            THashSet<TString> freshDeviceIds = {},
            NProto::TStorageServiceConfig configBase = {})
        : Runtime(runtime)
        , ActorId(0, "YYY")
        , VolumeActorId(0, "VVV")
        , StorageStatsServiceState(MakeIntrusive<TStorageStatsServiceState>())
        , DiskAgentState(std::make_shared<TDiskAgentState>())
    {
        SetupLogging();

        NProto::TStorageServiceConfig storageConfig = std::move(configBase);
        storageConfig.SetMaxTimedOutDeviceStateDuration(20'000);
        storageConfig.SetNonReplicatedMinRequestTimeoutSSD(1'000);
        storageConfig.SetNonReplicatedMaxRequestTimeoutSSD(5'000);
        storageConfig.SetDataScrubbingEnabled(true);
        // set bandwidth to reach maximum bandwidth for scrubbing - 50 MiB/s
        storageConfig.SetScrubbingBandwidth(20000000);
        storageConfig.SetResyncRangeAfterScrubbing(true);

        Config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        ScrubbingInterval = CalculateScrubbingInterval(
            6144,
            512,
            Config->GetScrubbingBandwidth(),
            Config->GetMaxScrubbingBandwidth(),
            Config->GetMinScrubbingBandwidth());

        auto nodeId = Runtime.GetNodeId(0);

        TDevices allDevices;
        for (const auto& d: devices) {
            allDevices.Add()->CopyFrom(d);
        }
        for (const auto& r: replicas) {
            for (const auto& d: r) {
                allDevices.Add()->CopyFrom(d);
            }
        }
        for (auto& m: migrations) {
            allDevices.Add()->CopyFrom(m.GetTargetDevice());
            ToLogicalBlocks(*m.MutableTargetDevice(), DefaultBlockSize);
        }

        Runtime.AddLocalService(
            MakeDiskAgentServiceId(nodeId),
            TActorSetupCmd(
                new TDiskAgentMock(allDevices, DiskAgentState),
                TMailboxType::Simple,
                0
            )
        );

        TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfigInitParams
            params{
                ToLogicalBlocks(devices, DefaultBlockSize),
                TNonreplicatedPartitionConfig::TVolumeInfo{
                    Now(),
                    // only SSD/HDD distinction matters
                    NProto::STORAGE_MEDIA_SSD_MIRROR3},
                "test",
                DefaultBlockSize,
                VolumeActorId};
        params.FreshDeviceIds = std::move(freshDeviceIds);
        auto partConfig =
            std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));

        for (auto& replica: replicas) {
            replica = ToLogicalBlocks(replica, DefaultBlockSize);
        }

        auto part = std::make_unique<TMirrorPartitionActor>(
            Config,
            CreateDiagnosticsConfig(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            "", // rwClientId
            partConfig,
            std::move(migrations),
            replicas,
            nullptr, // rdmaClient
            VolumeActorId,
            TActorId() // resyncActorId
        );

        Runtime.AddLocalService(
            ActorId,
            TActorSetupCmd(part.release(), TMailboxType::Simple, 0)
        );

        AddReplica(partConfig->Fork(partConfig->GetDevices()), "ZZZ");
        for (size_t i = 0; i < replicas.size(); ++i) {
            AddReplica(partConfig->Fork(replicas[i]), Sprintf("ZZZ%zu", i));
        }

        auto volume = std::make_unique<TDummyActor>();

        Runtime.AddLocalService(
            VolumeActorId,
            TActorSetupCmd(volume.release(), TMailboxType::Simple, 0)
        );

        auto diskRegistry = std::make_unique<TDummyActor>();

        Runtime.AddLocalService(
            MakeDiskRegistryProxyServiceId(),
            TActorSetupCmd(diskRegistry.release(), TMailboxType::Simple, 0)
        );

        Runtime.AddLocalService(
            MakeStorageStatsServiceId(),
            TActorSetupCmd(
                new TStorageStatsServiceMock(StorageStatsServiceState),
                TMailboxType::Simple,
                0
            )
        );

        Runtime.AddLocalService(
            MakeStorageServiceId(),
            TActorSetupCmd(new TStorageServiceMock(), TMailboxType::Simple, 0));

        NKikimr::SetupTabletServices(Runtime);
    }

    void AddReplica(
        TNonreplicatedPartitionConfigPtr partConfig,
        TString name)
    {
        auto part = std::make_unique<TNonreplicatedPartitionActor>(
            Config,
            CreateDiagnosticsConfig(),
            partConfig,
            TActorId() // do not send stats
        );

        TActorId actorId(0, name);
        Runtime.AddLocalService(
            actorId,
            TActorSetupCmd(part.release(), TMailboxType::Simple, 0)
        );

        ReplicaActors.push_back(actorId);
    }

    void WriteMirror(TBlockRange64 range, char fill)
    {
        WriteActor(ActorId, range, fill);
    }

    void WriteReplica(int idx, TBlockRange64 range, char fill)
    {
        WriteActor(ReplicaActors[idx], range, fill);
    }

    void WriteActor(TActorId actorId, TBlockRange64 range, char fill)
    {
        TPartitionClient client(Runtime, actorId);
        client.WriteBlocks(range, fill);
    }

    void SetupLogging()
    {
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
            Runtime.SetLogPriority(i, NLog::PRI_DEBUG);
        }
        // Runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    }
};


void WaitUntilScrubbingFinishesCurrentCycle(TTestEnv& testEnv)
{
    auto& counters = testEnv.StorageStatsServiceState->Counters;
    ui64 prevScrubbingProgress = counters.Simple.ScrubbingProgress.Value;
    ui32 iterations = 0;
    while (iterations++ < 100) {
        testEnv.Runtime.AdvanceCurrentTime(UpdateCountersInterval);
        testEnv.Runtime.DispatchEvents({}, TDuration::MilliSeconds(50));
        if (prevScrubbingProgress > counters.Simple.ScrubbingProgress.Value)
        {
            break;
        }
        prevScrubbingProgress = counters.Simple.ScrubbingProgress.Value;
    }
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMirrorPartitionTest)
{
    // TODO: reduce code duplication (see part_nonrepl_ut.cpp)

    Y_UNIT_TEST(ShouldReadWriteZero)
    {
        TTestRuntime runtime;

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        {
            auto response = client.ReadBlocks(
                TBlockRange64::WithLength(1024, 3072));
            const auto& blocks = response->Record.GetBlocks();

            UNIT_ASSERT_VALUES_EQUAL(3072, blocks.BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(0).size());
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 0),
                blocks.GetBuffers(0)
            );

            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(3071).size());
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 0),
                blocks.GetBuffers(3071)
            );
        }

        client.WriteBlocks(TBlockRange64::WithLength(1024, 3072), 1);
        client.WriteBlocks(TBlockRange64::MakeClosedInterval(1024, 4023), 2);

        {
            auto response = client.ReadBlocks(
                TBlockRange64::WithLength(1024, 3072));
            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(3072, blocks.BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(0).size());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(2999).size());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(3000).size());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(3071).size());

            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 2),
                blocks.GetBuffers(0)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 2),
                blocks.GetBuffers(2999)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 1),
                blocks.GetBuffers(3000)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 1),
                blocks.GetBuffers(3071)
            );
        }

        client.ZeroBlocks(TBlockRange64::MakeClosedInterval(2024, 3023));

        {
            auto response = client.ReadBlocks(
                TBlockRange64::WithLength(1024, 3072));
            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(3072, blocks.BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 2),
                blocks.GetBuffers(0)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 2),
                blocks.GetBuffers(999)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 0),
                blocks.GetBuffers(1000)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 0),
                blocks.GetBuffers(1999)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 2),
                blocks.GetBuffers(2000)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 2),
                blocks.GetBuffers(2999)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 1),
                blocks.GetBuffers(3000)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 1),
                blocks.GetBuffers(3071)
            );
        }

        client.WriteBlocks(TBlockRange64::MakeClosedInterval(5000, 5199), 3);
        client.ZeroBlocks(TBlockRange64::MakeClosedInterval(5050, 5150));

        {
            auto response = client.ReadBlocks(
                TBlockRange64::MakeClosedInterval(5000, 5119));
            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(120, blocks.BuffersSize());
            for (ui32 i = 0; i < 50; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 3),
                    blocks.GetBuffers(i)
                );
            }

            for (ui32 i = 51; i < 120; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 0),
                    blocks.GetBuffers(i)
                );
            }
        }

        {
            client.SendReadBlocksRequest(
                TBlockRange64::MakeClosedInterval(5000, 5120));
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                response->GetStatus(),
                response->GetErrorReason());
        }

        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        UNIT_ASSERT_VALUES_EQUAL(4, counters.ReadBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * 9336,
            counters.ReadBlocks.RequestBytes
        );
        UNIT_ASSERT_VALUES_EQUAL(3 * 3, counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            3 * DefaultBlockSize * 6192,
            counters.WriteBlocks.RequestBytes
        );
        UNIT_ASSERT_VALUES_EQUAL(3 * 2, counters.ZeroBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            3 * DefaultBlockSize * 1070,
            counters.ZeroBlocks.RequestBytes
        );
    }

    Y_UNIT_TEST(ShouldLocalReadWrite)
    {
        TTestRuntime runtime;

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        const auto diskRange = TBlockRange64::WithLength(0, 5120);

        const auto blockRange1 = TBlockRange64::WithLength(1024, 3072);
        client.WriteBlocksLocal(blockRange1, TString(DefaultBlockSize, 'A'));

        {
            TVector<TString> blocks;

            client.ReadBlocksLocal(
                blockRange1,
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    blockRange1.Size(),
                    TString(DefaultBlockSize, '\0')
                )));

            for (const auto& block: blocks) {
                for (auto c: block) {
                    UNIT_ASSERT_VALUES_EQUAL('A', c);
                }
            }
        }

        const auto blockRange2 = TBlockRange64::MakeClosedInterval(5000, 5199);
        client.WriteBlocksLocal(blockRange2, TString(DefaultBlockSize, 'B'));

        const auto blockRange3 = TBlockRange64::MakeClosedInterval(5000, 5150);

        {
            TVector<TString> blocks;

            client.ReadBlocksLocal(
                TBlockRange64::MakeClosedInterval(5000, 5119),
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    120,
                    TString(DefaultBlockSize, '\0')
                )));

            for (ui32 i = 0; i < 120; ++i) {
                const auto& block = blocks[i];
                for (auto c: block) {
                    UNIT_ASSERT_VALUES_EQUAL('B', c);
                }
            }
        }

        {
            TVector<TString> blocks;

            client.SendReadBlocksLocalRequest(
                blockRange3,
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    blockRange3.Size(),
                    TString(DefaultBlockSize, '\0')
                )));
            auto response = client.RecvReadBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_INVALID_STATE,
                response->GetStatus(),
                response->GetErrorReason());
        }

        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        UNIT_ASSERT_VALUES_EQUAL(2, counters.ReadBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * (
                blockRange1.Size() + blockRange3.Intersect(diskRange).Size()
            ),
            counters.ReadBlocks.RequestBytes
        );
        UNIT_ASSERT_VALUES_EQUAL(3 * 2, counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            3 * DefaultBlockSize * (
                blockRange1.Size() + blockRange2.Intersect(diskRange).Size()
            ),
            counters.WriteBlocks.RequestBytes
        );
    }

    Y_UNIT_TEST(ShouldMirrorWriteAndZeroRequests)
    {
        TTestRuntime runtime;

        THashMap<TString, TBlockRange64> device2WriteRange;
        THashMap<TString, TBlockRange64> device2ZeroRange;

        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskAgent::EvWriteDeviceBlocksRequest: {
                        using TRequest =
                            TEvDiskAgent::TEvWriteDeviceBlocksRequest;
                        const auto& record = event->Get<TRequest>()->Record;
                        UNIT_ASSERT(!device2WriteRange.contains(record.GetDeviceUUID()));
                        device2WriteRange[record.GetDeviceUUID()] =
                            TBlockRange64::WithLength(
                                record.GetStartIndex(),
                                record.GetBlocks().GetBuffers().size());
                        break;
                    }

                    case TEvDiskAgent::EvZeroDeviceBlocksRequest: {
                        using TRequest =
                            TEvDiskAgent::TEvZeroDeviceBlocksRequest;
                        const auto& record = event->Get<TRequest>()->Record;
                        UNIT_ASSERT(!device2ZeroRange.contains(record.GetDeviceUUID()));
                        device2ZeroRange[record.GetDeviceUUID()] =
                            TBlockRange64::WithLength(
                                record.GetStartIndex(),
                                record.GetBlocksCount());
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        client.WriteBlocks(TBlockRange64::WithLength(1024, 3072), 1);
        client.ZeroBlocks(TBlockRange64::WithLength(0, 3072));

        UNIT_ASSERT_VALUES_EQUAL(6, device2WriteRange.size());
        UNIT_ASSERT_VALUES_EQUAL(6, device2ZeroRange.size());
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(1024, 1024)),
            DescribeRange(device2WriteRange["vasya"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 2048)),
            DescribeRange(device2WriteRange["petya"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(1024, 1024)),
            DescribeRange(device2WriteRange["vasya#1"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 2048)),
            DescribeRange(device2WriteRange["petya#1"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(1024, 1024)),
            DescribeRange(device2WriteRange["vasya#2"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 2048)),
            DescribeRange(device2WriteRange["petya#2"]));

        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 2048)),
            DescribeRange(device2ZeroRange["vasya"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 1024)),
            DescribeRange(device2ZeroRange["petya"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 2048)),
            DescribeRange(device2ZeroRange["vasya#1"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 1024)),
            DescribeRange(device2ZeroRange["petya#1"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 2048)),
            DescribeRange(device2ZeroRange["vasya#2"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::WithLength(0, 1024)),
            DescribeRange(device2ZeroRange["petya#2"]));

        device2WriteRange.clear();

        client.WriteBlocksLocal(
            TBlockRange64::MakeClosedInterval(1000, 4000),
            TString(DefaultBlockSize, 'A'));

        UNIT_ASSERT_VALUES_EQUAL(6, device2WriteRange.size());
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::MakeClosedInterval(1000, 2047)),
            DescribeRange(device2WriteRange["vasya"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::MakeClosedInterval(0, 1952)),
            DescribeRange(device2WriteRange["petya"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::MakeClosedInterval(1000, 2047)),
            DescribeRange(device2WriteRange["vasya#1"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::MakeClosedInterval(0, 1952)),
            DescribeRange(device2WriteRange["petya#1"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::MakeClosedInterval(1000, 2047)),
            DescribeRange(device2WriteRange["vasya#2"]));
        UNIT_ASSERT_VALUES_EQUAL(
            DescribeRange(TBlockRange64::MakeClosedInterval(0, 1952)),
            DescribeRange(device2WriteRange["petya#2"]));
    }

    Y_UNIT_TEST(ShouldNotReadFromFreshDevices)
    {
        TTestRuntime runtime;

        const THashSet<TString> freshDeviceIds{"vasya", "vasya#1", "petya#2"};
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TVector<TDevices>{
                TTestEnv::DefaultReplica(runtime.GetNodeId(0), 1),
                TTestEnv::DefaultReplica(runtime.GetNodeId(0), 2),
            },
            {}, // migrations
            freshDeviceIds);

        TPartitionClient client(runtime, env.ActorId);

        // vasya should be migrated => 2 ranges
        WaitForMigrations(runtime, 2);

        const auto diskRange = TBlockRange64::WithLength(0, 5120);
        client.WriteBlocksLocal(diskRange, TString(DefaultBlockSize, 'A'));

        {
            auto nodeId = runtime.GetNodeId(0);
            auto diskAgentActorId = MakeDiskAgentServiceId(nodeId);

            for (const auto& deviceId: freshDeviceIds) {
                auto sender = runtime.AllocateEdgeActor();

                auto request =
                    std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksRequest>();

                request->Record.SetStartIndex(0);
                request->Record.SetBlocksCount(Max<ui32>());
                request->Record.SetBlockSize(4_KB);
                request->Record.SetDeviceUUID(deviceId);

                runtime.Send(new IEventHandle(
                    diskAgentActorId,
                    sender,
                    request.release()));
            }

            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

#define TEST_READ(blockRange) {                                                \
            TVector<TString> blocks;                                           \
                                                                               \
            client.ReadBlocksLocal(                                            \
                blockRange,                                                    \
                TGuardedSgList(ResizeBlocks(                                   \
                    blocks,                                                    \
                    blockRange.Size(),                                         \
                    TString(DefaultBlockSize, '\0')                            \
                )));                                                           \
                                                                               \
            for (const auto& block: blocks) {                                  \
                for (auto c: block) {                                          \
                    UNIT_ASSERT_VALUES_EQUAL('A', c);                          \
                }                                                              \
            }                                                                  \
        }                                                                      \
// TEST_READ

        // doing multiple reads to check that none of them targets fresh devices
        TEST_READ(TBlockRange64::MakeOneBlock(0));
        TEST_READ(TBlockRange64::MakeOneBlock(0));
        TEST_READ(TBlockRange64::MakeOneBlock(0));

        TEST_READ(TBlockRange64::MakeOneBlock(2047));
        TEST_READ(TBlockRange64::MakeOneBlock(2047));
        TEST_READ(TBlockRange64::MakeOneBlock(2047));

        TEST_READ(TBlockRange64::MakeOneBlock(2048));
        TEST_READ(TBlockRange64::MakeOneBlock(2048));
        TEST_READ(TBlockRange64::MakeOneBlock(2048));

        TEST_READ(TBlockRange64::MakeOneBlock(5119));
        TEST_READ(TBlockRange64::MakeOneBlock(5119));
        TEST_READ(TBlockRange64::MakeOneBlock(5119));

#undef TEST_READ
    }

    Y_UNIT_TEST(ShouldReadFromSpecifiedReplica)
    {
        TTestRuntime runtime;
        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        const auto range1 = TBlockRange64::WithLength(0, 100);
        env.WriteMirror(range1, 'X');
        env.WriteReplica(0, range1, 'A');
        env.WriteReplica(1, range1, 'B');
        env.WriteReplica(2, range1, 'C');

        // Check that with 0 replica index we read all replicas round-robin
        for (char c: TVector{'A', 'B', 'C'})
        {
            auto response = client.ReadBlocks(range1, 0);
            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(100, blocks.BuffersSize());
            for (ui32 i = 0; i < 100; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, c),
                    blocks.GetBuffers(i));
            }
        }

        {
            auto response = client.ReadBlocks(range1, 2);
            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(100, blocks.BuffersSize());
            for (ui32 i = 0; i < 100; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 'B'),
                    blocks.GetBuffers(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldRejectReadWithWrongReplicaIndex)
    {
        TTestRuntime runtime;
        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        const auto range1 = TBlockRange64::WithLength(0, 100);
        env.WriteMirror(range1, 'X');
        env.WriteReplica(0, range1, 'A');
        env.WriteReplica(1, range1, 'B');
        env.WriteReplica(2, range1, 'C');

        {
            client.SendReadBlocksRequest(range1, 4);
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetErrorReason(),
                "incorrect ReplicaIndex");
        }
    }

    void DoShouldTryToSplitReadRequest(const THashSet<TString>& freshDeviceIds)
    {
        using namespace std::chrono_literals;
        TTestRuntime runtime;

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TVector<TDevices>{
                TTestEnv::DefaultReplica(runtime.GetNodeId(0), 1),
                TTestEnv::DefaultReplica(runtime.GetNodeId(0), 2),
            },
            {},   // migrations
            freshDeviceIds);

        TPartitionClient client(runtime, env.ActorId);

        runtime.AdvanceCurrentTime(100ms);
        runtime.DispatchEvents({}, 100ms);

        const TVector<TBlockRange64> diskRanges = {
            TBlockRange64::WithLength(2047, 1),
            TBlockRange64::WithLength(2048, 1),
            TBlockRange64::WithLength(2049, 1),
            TBlockRange64::WithLength(2050, 1),
            TBlockRange64::WithLength(2051, 1),
        };

        for (size_t i = 0; i < diskRanges.size(); ++i) {
            client.WriteBlocksLocal(
                diskRanges[i],
                TString(DefaultBlockSize, '0' + i));
        }

        {
            auto nodeId = runtime.GetNodeId(0);
            auto diskAgentActorId = MakeDiskAgentServiceId(nodeId);

            for (const auto& deviceId: freshDeviceIds) {
                auto sender = runtime.AllocateEdgeActor();

                auto request = std::make_unique<
                    TEvDiskAgent::TEvZeroDeviceBlocksRequest>();

                request->Record.SetStartIndex(0);
                request->Record.SetBlocksCount(Max<ui32>());
                request->Record.SetBlockSize(4_KB);
                request->Record.SetDeviceUUID(deviceId);

                runtime.Send(new IEventHandle(
                    diskAgentActorId,
                    sender,
                    request.release()));
            }

            runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        }

#define TEST_READ(blockRange)                                    \
    {                                                            \
        TVector<TString> blocks;                                 \
                                                                 \
        client.ReadBlocksLocal(                                  \
            blockRange,                                          \
            TGuardedSgList(ResizeBlocks(                         \
                blocks,                                          \
                blockRange.Size(),                               \
                TString(DefaultBlockSize, '\0'))));              \
                                                                 \
        ui64 blockIndex = diskRanges.front().Start;              \
        for (const auto& block: blocks) {                        \
            for (auto c: block) {                                \
                for (size_t i = 0; i < diskRanges.size(); ++i) { \
                    if (diskRanges[i].Contains(blockIndex)) {    \
                        UNIT_ASSERT_VALUES_EQUAL('0' + i, c);    \
                    }                                            \
                }                                                \
            }                                                    \
            ++blockIndex;                                        \
        }                                                        \
    }                                                            \
    // TEST_READ

        // doing multiple reads to check that none of them targets fresh devices
        TEST_READ(TBlockRange64::WithLength(2047, 5));
        TEST_READ(TBlockRange64::WithLength(2047, 5));
        TEST_READ(TBlockRange64::WithLength(2047, 5));

#undef TEST_READ
    }

    Y_UNIT_TEST(ShouldTryToSplitReadRequest)
    {
        auto getDeviceUUID = [](TString base, auto idx)
        {
            if (idx == 0) {
                return base;
            }

            return base + "#" + ToString(idx);
        };

        for (size_t vasyaFreshDeviceFirst = 0; vasyaFreshDeviceFirst < 3;
             ++vasyaFreshDeviceFirst)
        {
            for (size_t vasyaFreshDeviceSecond = vasyaFreshDeviceFirst + 1;
                 vasyaFreshDeviceSecond < 3;
                 ++vasyaFreshDeviceSecond)
            {
                for (size_t petyaFreshDeviceFirst = 0;
                     petyaFreshDeviceFirst < 3;
                     ++petyaFreshDeviceFirst)
                {
                    for (size_t petyaFreshDeviceSecond =
                             petyaFreshDeviceFirst + 1;
                         petyaFreshDeviceSecond < 3;
                         ++petyaFreshDeviceSecond)
                    {
                        THashSet<TString> freshDeviceIds = {
                            getDeviceUUID("vasya", vasyaFreshDeviceFirst),
                            getDeviceUUID("vasya", vasyaFreshDeviceSecond),
                            getDeviceUUID("petya", petyaFreshDeviceFirst),
                            getDeviceUUID("petya", petyaFreshDeviceSecond),
                        };
                        DoShouldTryToSplitReadRequest(freshDeviceIds);
                    }
                }
            }
        }
    }

    struct TMigrationTestRuntime
    {
        TTestRuntime Runtime;

        TString DiskId;
        TString SourceDeviceId;
        TString TargetDeviceId;
        bool FinishRequestObserved = false;
        ui32 MigratedRanges = 0;

        TMigrationTestRuntime()
        {
            auto obs = [&] (auto& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskRegistry::EvFinishMigrationRequest: {
                        UNIT_ASSERT(!FinishRequestObserved);
                        FinishRequestObserved = true;

                        using TEv = TEvDiskRegistry::TEvFinishMigrationRequest;
                        auto request = event->template Get<TEv>();
                        DiskId = request->Record.GetDiskId();
                        auto& migrations = request->Record.GetMigrations();
                        UNIT_ASSERT_VALUES_EQUAL(1, migrations.size());
                        SourceDeviceId = migrations[0].GetSourceDeviceId();
                        TargetDeviceId = migrations[0].GetTargetDeviceId();

                        break;
                    }

                    case TEvNonreplPartitionPrivate::EvRangeMigrated: {
                        using TEv = TEvNonreplPartitionPrivate::TEvRangeMigrated;
                        auto* msg = event->template Get<TEv>();
                        if (!HasError(msg->Error)) {
                            ++MigratedRanges;
                        }

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            };

            Runtime.SetRegistrationObserverFunc(
                [=] (auto& runtime, const auto& parentId, const auto& actorId)
            {
                Y_UNUSED(parentId);
                runtime.SetObserverFunc(obs);
                runtime.EnableScheduleForActor(actorId);
            });
        }

        void WriteToAgent(TString deviceId, char c, ui32 blockCount)
        {
            auto sender = Runtime.AllocateEdgeActor();

            auto request =
                std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();

            request->Record.SetStartIndex(0);
            request->Record.SetBlockSize(4_KB);
            request->Record.SetDeviceUUID(deviceId);
            auto& blocks = *request->Record.MutableBlocks();

            for (ui32 i = 0; i < blockCount; ++i) {
                *blocks.AddBuffers() = TString(4_KB, c);
            }

            auto diskAgentActorId = MakeDiskAgentServiceId(Runtime.GetNodeId(0));
            Runtime.Send(new IEventHandle(
                diskAgentActorId,
                sender,
                request.release()));
        };

        void TestAgentData(TString deviceId, char c, ui32 blockCount)
        {
            auto sender = Runtime.AllocateEdgeActor();

            auto request =
                std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksRequest>();

            request->Record.SetStartIndex(0);
            request->Record.SetBlockSize(4_KB);
            request->Record.SetDeviceUUID(deviceId);
            request->Record.SetBlocksCount(blockCount);

            auto diskAgentActorId = MakeDiskAgentServiceId(Runtime.GetNodeId(0));
            Runtime.Send(new IEventHandle(
                diskAgentActorId,
                sender,
                request.release()));

            TAutoPtr<IEventHandle> handle;
            using TResponse = TEvDiskAgent::TEvReadDeviceBlocksResponse;
            Runtime.GrabEdgeEventRethrow<TResponse>(handle, WaitTimeout);

            UNIT_ASSERT(handle);
            auto response = handle->Release<TResponse>();

            const auto& buffers = response->Record.GetBlocks().GetBuffers();
            UNIT_ASSERT_VALUES_EQUAL(blockCount, buffers.size());

            UNIT_ASSERT_VALUES_EQUAL(TString(4_KB, c), buffers[0]);
            UNIT_ASSERT_VALUES_EQUAL(TString(4_KB, c), buffers[blockCount - 1]);
        };
    };

    Y_UNIT_TEST(ShouldCopyDataToFreshDevices)
    {
        TMigrationTestRuntime mr;
        auto& runtime = mr.Runtime;

        const THashSet<TString> freshDeviceIds{"vasya", "vasya#1", "petya#2"};
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TVector<TDevices>{
                TTestEnv::DefaultReplica(runtime.GetNodeId(0), 1),
                TTestEnv::DefaultReplica(runtime.GetNodeId(0), 2),
            },
            {}, // migrations
            freshDeviceIds);

        mr.WriteToAgent("vasya#2", 'A', 2048);
        mr.WriteToAgent("petya", 'B', 3072);
        mr.WriteToAgent("petya#1", 'B', 3072);

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TPartitionClient client(runtime, env.ActorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvDiskRegistry::EvFinishMigrationRequest);
        runtime.DispatchEvents(options);
        // vasya should be migrated => 2 ranges
        UNIT_ASSERT_VALUES_EQUAL(2, mr.MigratedRanges);

        UNIT_ASSERT_VALUES_EQUAL("test", mr.DiskId);
        UNIT_ASSERT_VALUES_EQUAL("vasya#2", mr.SourceDeviceId);
        UNIT_ASSERT_VALUES_EQUAL("vasya", mr.TargetDeviceId);

        mr.TestAgentData("vasya", 'A', 2048);

        // TODO trigger and test migration for petya and petya#1
    }

    Y_UNIT_TEST(ShouldMigrateDevices)
    {
        // this test tests actual migrations, not replication

        TMigrationTestRuntime mr;
        auto& runtime = mr.Runtime;

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TVector<TDevices>{
                TTestEnv::DefaultReplica(runtime.GetNodeId(0), 1),
                TTestEnv::DefaultReplica(runtime.GetNodeId(0), 2),
            },
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0), 0),
            {}  // freshDeviceIds
        );

        mr.WriteToAgent("vasya", 'A', 2048);
        mr.WriteToAgent("petya", 'B', 3072);

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        TPartitionClient client(runtime, env.ActorId);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(
            TEvDiskRegistry::EvFinishMigrationRequest);
        runtime.DispatchEvents(options);
        // vasya should be migrated => 2 ranges
        UNIT_ASSERT_VALUES_EQUAL(2, mr.MigratedRanges);

        UNIT_ASSERT_VALUES_EQUAL("test", mr.DiskId);
        UNIT_ASSERT_VALUES_EQUAL("vasya", mr.SourceDeviceId);
        UNIT_ASSERT_VALUES_EQUAL("vasya-migration", mr.TargetDeviceId);

        mr.TestAgentData("vasya-migration", 'A', 2048);

        // TODO trigger and test migration for petya and petya#1
    }

    void DoShouldTransformAnyErrorToRetriable(NProto::TError error)
    {
        TTestRuntime runtime;

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        const auto blockRange = TBlockRange64::WithLength(1024, 3072);
        client.WriteBlocksLocal(blockRange, TString(DefaultBlockSize, 'A'));

        env.DiskAgentState->Error = std::move(error);

        {
            TVector<TString> blocks;

            client.SendReadBlocksLocalRequest(
                blockRange,
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    blockRange.Size(),
                    TString(DefaultBlockSize, '\0')
                )));
            auto response = client.RecvReadBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            TString data(DefaultBlockSize, 'B');
            client.SendWriteBlocksLocalRequest(blockRange, data);

            auto response = client.RecvWriteBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        env.DiskAgentState->Error = {};

        {
            TVector<TString> blocks;

            client.ReadBlocksLocal(
                blockRange,
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    blockRange.Size(),
                    TString(DefaultBlockSize, '\0')
                )));

            for (const auto& block: blocks) {
                for (auto c: block) {
                    UNIT_ASSERT_VALUES_EQUAL('A', c);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_FAIL)
    {
        DoShouldTransformAnyErrorToRetriable(MakeError(E_FAIL, "E_FAIL error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_ARGUMENT)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_ARGUMENT, "E_ARGUMENT error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_INVALID_STATE)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_INVALID_STATE, "E_INVALID_STATE error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_NOT_FOUND)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_TIMEOUT, "E_TIMEOUT error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_TIMEOUT)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_TIMEOUT, "E_TIMEOUT error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_UNAUTHORIZED)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_UNAUTHORIZED, "E_UNAUTHORIZED error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_ABORTED)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_ABORTED, "E_ABORTED error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_TRY_AGAIN)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_TRY_AGAIN, "E_TRY_AGAIN error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_IO)
    {
        DoShouldTransformAnyErrorToRetriable(MakeError(E_IO, "E_IO error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_CANCELLED)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_CANCELLED, "E_CANCELLED error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_IO_SILENT)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_IO_SILENT, "E_IO_SILENT error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_RETRY_TIMEOUT)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_RETRY_TIMEOUT, "E_RETRY_TIMEOUT error"));
    }

    Y_UNIT_TEST(ShouldTransformAnyErrorToRetriable_E_PRECONDITION_FAILED)
    {
        DoShouldTransformAnyErrorToRetriable(
            MakeError(E_PRECONDITION_FAILED, "E_PRECONDITION_FAILED error"));
    }

    Y_UNIT_TEST(ShouldReportSimpleCounters)
    {
        TTestRuntime runtime;

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.Simple;
        UNIT_ASSERT_VALUES_EQUAL(
            6 * 1024 * DefaultBlockSize,
            counters.BytesCount.Value);
    }

    Y_UNIT_TEST(ShouldCalculateScrubbingIntervalCorrectly)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0.8),
            CalculateScrubbingInterval(24379392, 4_KB, 20, 50, 5));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0.08),
            CalculateScrubbingInterval(2437939200, 4_KB, 20, 50, 5));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0.2),
            CalculateScrubbingInterval(268435456, 4_KB, 20, 50, 5));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0.8),
            CalculateScrubbingInterval(6144, 512, 50, 50, 5));
        UNIT_ASSERT_VALUES_EQUAL(
            TDuration::Seconds(0.1),
            CalculateScrubbingInterval(536870912, 4_KB, 20, 50, 5));
    }

    Y_UNIT_TEST(ShouldReportScrubbingCounter)
    {
        using namespace NMonitoring;

        TTestRuntime runtime;

        TTestEnv env(runtime);

        auto& counters = env.StorageStatsServiceState->Counters;

        runtime.DispatchEvents({}, env.ScrubbingInterval);
        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(50));

        UNIT_ASSERT_VALUES_EQUAL(
            2 * 4_MB,
            counters.Cumulative.ScrubbingThroughput.Value);
        UNIT_ASSERT_VALUES_EQUAL(33, counters.Simple.ScrubbingProgress.Value);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.Simple.ChecksumMismatches.Value);
    }

    Y_UNIT_TEST(ShouldFindChecksumMismatch)
    {
        using namespace NMonitoring;

        TTestRuntime runtime;

        TDynamicCountersPtr critEventsCounters = new TDynamicCounters();
        InitCriticalEventsCounter(critEventsCounters);

        NProto::TStorageServiceConfig config;
        config.SetAutomaticallyEnableBufferCopyingAfterChecksumMismatch(true);
        TTestEnv env(runtime, config);

        bool addTagRequest = false;
        bool addTagResponse = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == TEvService::EvAddTagsRequest) {
                    using TRequest = TEvService::TEvAddTagsRequest;
                    const auto& tags = event->template Get<TRequest>()->Tags;
                    UNIT_ASSERT_VALUES_EQUAL(1, tags.size());
                    UNIT_ASSERT_VALUES_EQUAL(
                        IntermediateWriteBufferTagName,
                        tags[0]);
                    addTagRequest = true;
                }

                if (event->GetTypeRewrite() ==
                    TEvService::EvAddTagsResponse) {
                    UNIT_ASSERT(addTagRequest);
                    addTagResponse = true;
                }

                return false;
            });

        const auto range1 = TBlockRange64::WithLength(0, 2);
        env.WriteMirror(range1, 'A');
        env.WriteReplica(1, range1, 'B');
        env.WriteReplica(2, range1, 'B');

        const auto range2 = TBlockRange64::WithLength(4096, 100);
        env.WriteMirror(range2, 'A');
        env.WriteReplica(2, range2, 'B');

        WaitUntilScrubbingFinishesCurrentCycle(env);

        auto& counters = env.StorageStatsServiceState->Counters;
        auto mirroredDiskMinorityChecksumMismatch =
            critEventsCounters->GetCounter(
                "AppCriticalEvents/MirroredDiskMinorityChecksumMismatch",
                true);

        UNIT_ASSERT_VALUES_EQUAL(2, mirroredDiskMinorityChecksumMismatch->Val());
        UNIT_ASSERT_VALUES_EQUAL(2, counters.Simple.ChecksumMismatches.Value);
        UNIT_ASSERT(addTagResponse);

        const auto range3 = TBlockRange64::WithLength(1025, 50);
        env.WriteMirror(range3, 'A');
        env.WriteReplica(1, range3, 'B');

        // at this point, scrubbing may not start from the beginning,
        // so we need to wait for 2 cycles to be sure that
        // it has scanned the entire disk at least once
        WaitUntilScrubbingFinishesCurrentCycle(env);
        WaitUntilScrubbingFinishesCurrentCycle(env);
        UNIT_ASSERT_VALUES_EQUAL(3, counters.Simple.ChecksumMismatches.Value);
        UNIT_ASSERT_VALUES_EQUAL(3, mirroredDiskMinorityChecksumMismatch->Val());

        // at this point, scrubbing may not start from the beginning,
        // so we need to wait for 2 cycles to be sure that
        // it has scanned the entire disk at least once
        WaitUntilScrubbingFinishesCurrentCycle(env);
        WaitUntilScrubbingFinishesCurrentCycle(env);

        // check that all ranges was resynced and there is no more mismatches
        UNIT_ASSERT_VALUES_EQUAL(3, counters.Simple.ChecksumMismatches.Value);
        UNIT_ASSERT_VALUES_EQUAL(3, mirroredDiskMinorityChecksumMismatch->Val());
    }

    Y_UNIT_TEST(ShouldReportAddTagFailedCritEvent)
    {
        using namespace NMonitoring;

        TDynamicCountersPtr critEventsCounters = new TDynamicCounters();
        InitCriticalEventsCounter(critEventsCounters);

        TTestRuntime runtime;

        NProto::TStorageServiceConfig config;
        config.SetAutomaticallyEnableBufferCopyingAfterChecksumMismatch(true);
        TTestEnv env(runtime, config);

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvService::EvAddTagsResponse) {
                    auto response =
                        std::make_unique<TEvService::TEvAddTagsResponse>(
                            MakeError(E_REJECTED, "error"));

                    runtime.Send(new IEventHandle(
                        event->Recipient,
                        event->Sender,
                        response.release(),
                        0,   // flags
                        event->Cookie));
                    return true;
                }

                return false;
            });

        const auto range1 = TBlockRange64::WithLength(0, 2);
        env.WriteMirror(range1, 'A');
        env.WriteReplica(1, range1, 'B');
        env.WriteReplica(2, range1, 'B');

        WaitUntilScrubbingFinishesCurrentCycle(env);

        auto addTagFailed = critEventsCounters->GetCounter(
            "AppCriticalEvents/MirroredDiskAddTagFailed",
            true);
        UNIT_ASSERT_VALUES_EQUAL(1, addTagFailed->Val());
    }

    Y_UNIT_TEST(ShouldPostponeScrubbingIfIntersectingWritePending)
    {
        using namespace NMonitoring;

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);

        TTestRuntime runtime;

        TTestEnv env(runtime);

        const auto range = TBlockRange64::WithLength(1030, 200);

        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'C');

        ui32 rangeCount = 0;
        TAutoPtr<IEventHandle> delayedRequest;
        runtime.SetScheduledEventFilter(
            [&] (auto& runtime, auto& event, auto&& delay, auto&& deadline)
        {
            Y_UNUSED(runtime);
            Y_UNUSED(delay);
            Y_UNUSED(deadline);
            if (event->GetTypeRewrite() ==
                TEvNonreplPartitionPrivate::EvScrubbingNextRange)
            {
                ++rangeCount;
                if (delayedRequest && rangeCount > 5) {
                    runtime.Send(delayedRequest.Release());
                }
            }

            return false;
        });
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                if (!delayedRequest) {
                    delayedRequest = event.Release();
                    return true;
                }
            }

            return false;
        });
        env.WriteActor(env.ActorId, range, 'D');

        auto mirroredDiskMinorityChecksumMismatch = counters->GetCounter(
            "AppCriticalEvents/MirroredDiskMinorityChecksumMismatch",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, mirroredDiskMinorityChecksumMismatch->Val());

        rangeCount = 0;
        ui32 iterations = 0;
        while (rangeCount < 5 && iterations++ < 100) {
            runtime.DispatchEvents({}, env.ScrubbingInterval);
        }

        UNIT_ASSERT_VALUES_EQUAL(0, mirroredDiskMinorityChecksumMismatch->Val());
    }

    Y_UNIT_TEST(ShouldNotFindMismatchIfChecksumIntersectedWithWrite)
    {
        using namespace NMonitoring;

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);

        TTestRuntime runtime;

        TTestEnv env(runtime);

        const auto range = TBlockRange64::WithLength(5, 200);

        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'C');

        enum {
            INIT,
            REQUESTS_RECEIVED,
            CHECKSUM_SENT,
            FINISH
        } state = INIT;

        ui32 rangeCount = 0;
        TAutoPtr<IEventHandle> delayedWriteRequest;
        TAutoPtr<IEventHandle> delayedChecksumRequest;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);
            switch (state) {
                case INIT: {
                    if (event->GetTypeRewrite() ==
                        TEvDiskAgent::EvWriteDeviceBlocksRequest)
                    {
                        if (!delayedWriteRequest) {
                            delayedWriteRequest = event.Release();
                            return true;
                        }
                    }
                    if (event->GetTypeRewrite() ==
                        TEvDiskAgent::EvChecksumDeviceBlocksRequest)
                    {
                        if (!delayedChecksumRequest) {
                            delayedChecksumRequest = event.Release();
                            return true;
                        }
                    }
                    if (delayedWriteRequest && delayedChecksumRequest) {
                        state = REQUESTS_RECEIVED;
                    }
                    break;
                }
                case REQUESTS_RECEIVED: {
                    state = CHECKSUM_SENT;
                    runtime.Send(delayedChecksumRequest.Release());
                    break;
                }
                case CHECKSUM_SENT: {
                    if (event->GetTypeRewrite() ==
                        TEvDiskAgent::EvChecksumDeviceBlocksResponse)
                    {
                        state = FINISH;
                        runtime.Send(delayedWriteRequest.Release());
                    }
                    break;
                }
                default:
                    break;
            }

            return false;
        });
        runtime.SetScheduledEventFilter(
            [&] (auto& runtime, auto& event, auto&& delay, auto&& deadline)
        {
            Y_UNUSED(runtime);
            Y_UNUSED(delay);
            Y_UNUSED(deadline);
            if (state == FINISH &&
                event->GetTypeRewrite() ==
                    TEvNonreplPartitionPrivate::EvScrubbingNextRange)
            {
                ++rangeCount;
            }

            return false;
        });

        runtime.DispatchEvents({}, env.ScrubbingInterval);

        env.WriteActor(env.ActorId, range, 'D');

        ui32 iterations = 0;
        while (rangeCount < 5 && iterations++ < 100) {
            runtime.DispatchEvents({}, env.ScrubbingInterval);
        }

        auto mirroredDiskMinorityChecksumMismatch = counters->GetCounter(
            "AppCriticalEvents/MirroredDiskMinorityChecksumMismatch",
            true);

        UNIT_ASSERT_VALUES_EQUAL(0, mirroredDiskMinorityChecksumMismatch->Val());
    }

    Y_UNIT_TEST(ShouldNotFindMismatchIfWriteRequestToOneReplicaHasError)
    {
        using namespace NMonitoring;

        TDynamicCountersPtr critEventsCounters = new TDynamicCounters();
        InitCriticalEventsCounter(critEventsCounters);

        TTestRuntime runtime;

        TTestEnv env(runtime);

        const auto range = TBlockRange64::WithLength(5, 200);

        env.WriteMirror(range, 'A');

        TAutoPtr<IEventHandle> delayedWriteResponse;
        ui32 writeDeviceResponses = 0;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                ++writeDeviceResponses;
                if (writeDeviceResponses == 3) {
                    auto response = std::make_unique<
                        TEvDiskAgent::TEvWriteDeviceBlocksResponse>(
                        MakeError(E_REJECTED, "error"));

                    delayedWriteResponse = new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        response.release(),
                        0, // flags
                        event->Cookie
                    );

                    return true;
                }
            }
            return false;
        });

        TPartitionClient client(runtime, env.ActorId);
        TString data(DefaultBlockSize, 'B');
        client.SendWriteBlocksLocalRequest(range, data);

        ui32 iterations = 0;
        while (!delayedWriteResponse && iterations++ < 100) {
            runtime.DispatchEvents({}, TDuration::MilliSeconds(50));
        }
        runtime.Send(delayedWriteResponse.Release());
        auto response = client.RecvWriteBlocksLocalResponse();

        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(50));

        auto mirroredDiskMinorityChecksumMismatch = critEventsCounters->GetCounter(
            "AppCriticalEvents/MirroredDiskMinorityChecksumMismatch",
            true);
        auto& counters = env.StorageStatsServiceState->Counters;

        UNIT_ASSERT_VALUES_EQUAL(0, mirroredDiskMinorityChecksumMismatch->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.Simple.ScrubbingProgress.Value);

        client.SendWriteBlocksLocalRequest(range, data);
        response = client.RecvWriteBlocksLocalResponse();

        iterations = 0;
        while (counters.Simple.ScrubbingProgress.Value == 0 &&
            iterations++ < 100)
        {
            runtime.AdvanceCurrentTime(UpdateCountersInterval);
            runtime.DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        UNIT_ASSERT_VALUES_EQUAL(0, mirroredDiskMinorityChecksumMismatch->Val());
    }

    Y_UNIT_TEST(ShouldRejectRequestsIfRangeResyncingAfterChecksumMismatch)
    {
        using namespace NMonitoring;

        TTestRuntime runtime;

        TAutoPtr<IEventHandle> delayedRangeResynced;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvNonreplPartitionPrivate::EvRangeResynced)
            {
                delayedRangeResynced = event.Release();
                return true;
            }
            return false;
        });

        ui32 rangeCount = 0;
        runtime.SetScheduledEventFilter(
            [&] (auto& runtime, auto& event, auto&& delay, auto&& deadline)
        {
            Y_UNUSED(runtime);
            Y_UNUSED(delay);
            Y_UNUSED(deadline);
            if (event->GetTypeRewrite() ==
                TEvNonreplPartitionPrivate::EvScrubbingNextRange)
            {
                ++rangeCount;
            }

            return false;
        });

        TDynamicCountersPtr critEventsCounters = new TDynamicCounters();
        InitCriticalEventsCounter(critEventsCounters);

        TTestEnv env(runtime);

        const auto range1 = TBlockRange64::WithLength(3100, 100);
        const auto range2 = TBlockRange64::WithLength(3150, 2);
        env.WriteMirror(range1, 'A');
        env.WriteReplica(2, range1, 'B');

        // wait for scrubbing 3rd range
        ui32 iterations = 0;
        while (rangeCount < 3 && iterations++ < 100) {
            runtime.DispatchEvents({}, env.ScrubbingInterval);
        }

        // wait for resync 3rd range
        iterations = 0;
        while (!delayedRangeResynced && iterations++ < 100) {
            runtime.AdvanceCurrentTime(env.Config->GetScrubbingChecksumMismatchTimeout());
            runtime.DispatchEvents({}, TDuration::MilliSeconds(50));
        }

        // check that read/write to 3rd range will be rejected
        TPartitionClient client(runtime, env.ActorId);
        {
            TString data(DefaultBlockSize, 'B');
            client.SendWriteBlocksLocalRequest(range2, data);
            auto response = client.RecvWriteBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            TVector<TString> blocks;
            client.SendReadBlocksLocalRequest(
                range2,
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    range2.Size(),
                    TString(DefaultBlockSize, '\0')
                )));
            auto response = client.RecvReadBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // check that after resync requests to range complete successfully
        runtime.Send(delayedRangeResynced.Release());
        runtime.DispatchEvents({}, TDuration::MilliSeconds(50));
        {
            client.WriteBlocks(range2, 'C');
            auto response = client.ReadBlocks(range2);
            const auto& blocks = response->Record.GetBlocks();
            for (ui32 i = 0; i < blocks.BuffersSize(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 'C'),
                    blocks.GetBuffers(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldStartResyncAfterScrubbingOnlyIfMajorityOfChecksumsAreEqual)
    {
        using namespace NMonitoring;

        TTestRuntime runtime;

        ui32 rangeResynced = 0;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);
            if (event->GetTypeRewrite() ==
                TEvNonreplPartitionPrivate::EvRangeResynced)
            {
                ++rangeResynced;
            }
            return false;
        });

        TTestEnv env(runtime);
        auto& counters = env.StorageStatsServiceState->Counters;
        TDynamicCountersPtr critEventsCounters = new TDynamicCounters();
        InitCriticalEventsCounter(critEventsCounters);
        auto mirroredDiskMajorityChecksumMismatch =
            critEventsCounters->GetCounter(
                "AppCriticalEvents/MirroredDiskMajorityChecksumMismatch",
                true);
        auto mirroredDiskMinorityChecksumMismatch =
            critEventsCounters->GetCounter(
                "AppCriticalEvents/MirroredDiskMinorityChecksumMismatch",
                true);

        // Write different data to all replicas
        const auto range = TBlockRange64::WithLength(2049, 50);
        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'C');

        // Wait util all ranges process in scrubbing at least two times.
        // We need to be sure that resync wasn't started.
        WaitUntilScrubbingFinishesCurrentCycle(env);
        WaitUntilScrubbingFinishesCurrentCycle(env);
        UNIT_ASSERT_VALUES_EQUAL(2, counters.Simple.ChecksumMismatches.Value);
        UNIT_ASSERT_VALUES_EQUAL(0, rangeResynced);
        UNIT_ASSERT_VALUES_EQUAL(2, mirroredDiskMajorityChecksumMismatch->Val());
        UNIT_ASSERT_VALUES_EQUAL(0, mirroredDiskMinorityChecksumMismatch->Val());

        // Make data in 1st and 3rd replica the same.
        env.WriteReplica(2, range, 'A');

        // Wait again until all ranges process in scrubbing at least two times.
        // Check that mismatch was found and range was resynced now
        WaitUntilScrubbingFinishesCurrentCycle(env);
        WaitUntilScrubbingFinishesCurrentCycle(env);
        UNIT_ASSERT_VALUES_EQUAL(3, counters.Simple.ChecksumMismatches.Value);
        UNIT_ASSERT_VALUES_EQUAL(1, rangeResynced);
        UNIT_ASSERT_VALUES_EQUAL(2, mirroredDiskMajorityChecksumMismatch->Val());
        UNIT_ASSERT_VALUES_EQUAL(1, mirroredDiskMinorityChecksumMismatch->Val());
    }

    Y_UNIT_TEST(ShouldRejectReadUponChecksumMismatchIfRead2IsEnabled)
    {
        using namespace NMonitoring;

        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TAutoPtr<IEventHandle> toSend;
        bool interceptReadDeviceBlocks = false;
        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
            Y_UNUSED(runtime);
            if (!toSend && interceptReadDeviceBlocks && event->GetTypeRewrite()
                    == TEvDiskAgent::EvReadDeviceBlocksRequest)
            {
                toSend = event;
                return true;
            }

            return false;
        });

        TDynamicCountersPtr critEventsCounters = new TDynamicCounters();
        InitCriticalEventsCounter(critEventsCounters);

        auto mirroredDiskChecksumMismatchUponRead =
            critEventsCounters->GetCounter(
                "AppCriticalEvents/MirroredDiskChecksumMismatchUponRead",
                true);

        NProto::TStorageServiceConfig config;
        config.SetMirrorReadReplicaCount(2);
        TTestEnv env(runtime, config);

        // Write different data to all replicas.
        const auto range = TBlockRange64::WithLength(2049, 50);
        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'B');

        // Read request should cause E_REJECTED error since data checksums in
        // replicas 0 and 1 are different.
        TPartitionClient client(runtime, env.ActorId);

        // hits replicas 0 and 1
        {
            TVector<TString> blocks;
            client.SendReadBlocksLocalRequest(
                range,
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    range.Size(),
                    TString(DefaultBlockSize, '\0')
                )));
            auto response = client.RecvReadBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // hits replicas 2 and 0
        {
            client.SendReadBlocksRequest(range);
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // hits replicas 1 and 2 and thus succeeds
        {
            client.SendReadBlocksRequest(range);
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            for (ui32 i = 0; i < range.Size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    TString(4_KB, 'B'),
                    response->Record.GetBlocks().GetBuffers(i),
                    TStringBuilder() << "block " << (range.Start + i));
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            mirroredDiskChecksumMismatchUponRead->Val());

        interceptReadDeviceBlocks = true;
        client.SendReadBlocksRequest(range);
        runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
        UNIT_ASSERT(toSend);
        client.WriteBlocks(range, 'C');
        runtime.Send(toSend);
        {
            // checksum mismatch => E_REJECTED
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        // write request made block range dirty => no crit event
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            mirroredDiskChecksumMismatchUponRead->Val());
    }

    Y_UNIT_TEST(ShouldHandleGetDeviceForRangeRequest)
    {
        using TEvGetDeviceForRangeRequest =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest;
        using TEvGetDeviceForRangeResponse =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse;
        using EPurpose = TEvGetDeviceForRangeRequest::EPurpose;

        TTestRuntime runtime;
        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        {   // Request to first device #1
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(2040, 8)));
            auto response1 = client.RecvResponse<
                TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response1->GetStatus()),
                response1->GetErrorReason());
            UNIT_ASSERT_STRING_CONTAINS(
                response1->Device.GetDeviceUUID(),
                "vasya");

            // Request to first device #2
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(2040, 8)));
            auto response2 =
                client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response2->GetStatus()),
                response2->GetErrorReason());
            UNIT_ASSERT_STRING_CONTAINS(
                response2->Device.GetDeviceUUID(),
                "vasya");

            // Replicas are rotated and requests should return different
            // devices.
            UNIT_ASSERT_STRINGS_UNEQUAL(
                response1->Device.GetDeviceUUID(),
                response2->Device.GetDeviceUUID());
        }
        {
            // Request to second device
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(2048, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            UNIT_ASSERT_STRING_CONTAINS(
                response->Device.GetDeviceUUID(),
                "petya");
        }

        {   // Request on the border of two devices
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(2040, 16)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->Error.GetCode());
        }

        {   // Request for writing purpose
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(0, 16)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->Error.GetCode());
        }
    }

    Y_UNIT_TEST(ShouldHandleGetDeviceForRangeRequestWhenResync)
    {
        using TEvGetDeviceForRangeRequest =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest;
        using TEvGetDeviceForRangeResponse =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse;
        using EPurpose = TEvGetDeviceForRangeRequest::EPurpose;

        TTestRuntime runtime;

        // Block range resync finish.
        bool rangeResyncedCatched = false;
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                    TEvNonreplPartitionPrivate::EvRangeResynced)
                {
                    rangeResyncedCatched = true;
                    return true;
                }
                return false;
            });

        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        const auto range = TBlockRange64::WithLength(0, 1024);

        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'B');

        // Start resync of range [0..1023]
        runtime.AdvanceCurrentTime(
            env.Config->GetScrubbingChecksumMismatchTimeout());
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return rangeResyncedCatched;
        };
        runtime.DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT(rangeResyncedCatched);

        {   // Request overlaps with resyncing range. Range resyncing due to the
            // scrubber found replicas mismatch.
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(1020, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                response->GetErrorReason());
        }
        {   // Request to not resyncing range
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(1024, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            UNIT_ASSERT_STRING_CONTAINS(
                response->Device.GetDeviceUUID(),
                "vasya");
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(1024, 8),
                response->DeviceBlockRange);
        }
        {   // Request with writing purpose
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(1024, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                response->GetError().GetCode(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldReadFromAllReplicas)
    {
        constexpr ui32 replicaCount = 3;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        client.WriteBlocks(
            TBlockRange64::MakeClosedInterval(0, 1024),
            1);

        ui32 checksumResponseCount = 0;

        TActorId recepient;
        TMap<TActorId, TSet<TActorId>> actorIds;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvNonreplPartitionPrivate::EvChecksumBlocksResponse: {
                        ++checksumResponseCount;
                        recepient = event->Recipient;
                        actorIds[event->Recipient].insert(event->Sender);
                        break;
                    }
                    case TEvService::EvReadBlocksResponse: {
                        actorIds[event->Recipient].insert(event->Sender);
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto range = TBlockRange64::WithLength(0, 1024);

        client.ReadBlocks(range, 0, replicaCount);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvService::EvReadBlocksResponse);
        runtime.DispatchEvents(options, TDuration::Seconds(3));

        // When requesting a read for three replicas, Readings are made from
        // one replica, and checksums are calculated from the other two.
        UNIT_ASSERT_VALUES_EQUAL(replicaCount - 1, checksumResponseCount);
        UNIT_ASSERT_VALUES_EQUAL(replicaCount, actorIds[recepient].size());
    }

    Y_UNIT_TEST(ShouldRejectReadWithWrongReplicaCount)
    {
        TTestRuntime runtime;
        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        const auto range = TBlockRange64::WithLength(0, 100);
        env.WriteMirror(range, 'X');

        {
            client.SendReadBlocksRequest(range, 0, 4);
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
            UNIT_ASSERT_STRING_CONTAINS(
                response->GetErrorReason(),
                "has incorrect replica count");
        }
    }

    Y_UNIT_TEST(ShouldRejectReadFromAllReplicaIfRangeNotResynced)
    {
        constexpr ui32 replicaCount = 3;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto range = TBlockRange64::WithLength(100, 100);

        TPartitionClient client(runtime, env.ActorId);
        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'C');
        env.WriteReplica(2, range, 'C');

        {
            client.SendReadBlocksRequest(range, 0, replicaCount);
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldCheckRange)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        client.WriteBlocks(
            TBlockRange64::MakeClosedInterval(0, 1024 * 1024),
            1);

        ui32 status = -1;
        ui32 error = -1;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvCheckRangeResponse: {
                        using TEv = TEvVolume::TEvCheckRangeResponse;
                        const auto* msg = event->Get<TEv>();
                        error = msg->GetStatus();
                        status = msg->Record.GetStatus().GetCode();

                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto checkRange = [&](ui32 idx, ui32 size)
        {
            status = -1;
            error = -1;

            const auto response = client.CheckRange("id", idx, size);

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
            runtime.DispatchEvents(options, TDuration::Seconds(3));

            UNIT_ASSERT_VALUES_EQUAL(S_OK, status);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error);
        };

        checkRange(0, 1024);
        checkRange(1024, 512);
        checkRange(1, 1);
        checkRange(1000, 1000);
    }

    Y_UNIT_TEST(ShouldCheckRangeWithBrokenBlocks)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        client.WriteBlocks(
            TBlockRange64::MakeClosedInterval(0, 1024 * 1024),
            1);

        ui32 status = -1;
        ui32 error = -1;

        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvVolume::EvCheckRangeResponse: {
                        using TEv = TEvVolume::TEvCheckRangeResponse;
                        const auto* msg = event->Get<TEv>();
                        error = msg->GetStatus();
                        status = msg->Record.GetStatus().GetCode();

                        break;
                    }
                    case TEvService::EvReadBlocksResponse: {
                        using TEv = TEvService::TEvReadBlocksResponse;

                        auto response = std::make_unique<TEv>(
                            MakeError(E_IO, "block is broken"));

                        runtime.Send(
                            new IEventHandle(
                                event->Recipient,
                                event->Sender,
                                response.release(),
                                0,   // flags
                                event->Cookie),
                            0);

                        return TTestActorRuntime::EEventAction::DROP;

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        const auto checkRange = [&](ui32 idx, ui32 size)
        {
            status = -1;
            error = -1;

            client.SendCheckRangeRequest("id", idx, size);
            const auto response =
                client.RecvResponse<TEvVolume::TEvCheckRangeResponse>();

            TDispatchOptions options;
            options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
            runtime.DispatchEvents(options, TDuration::Seconds(3));

            UNIT_ASSERT_VALUES_EQUAL(E_IO, status);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, error);

        };

        checkRange(0, 1024);
        checkRange(1024, 512);
        checkRange(1, 1);
        checkRange(1000, 1000);
    }

    Y_UNIT_TEST(ShouldSuccessfullyCheckRangeIfDiskIsEmpty)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        const ui32 idx = 0;
        const ui32 size = 1;
        const auto response = client.CheckRange("id", idx, size);

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);

        runtime.DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(S_OK, response->Record.GetStatus().GetCode());
    }

    Y_UNIT_TEST(ShouldntCheckRangeWithBigBlockCount)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        const ui32 idx = 0;

        client.SendCheckRangeRequest("id", idx, 16_MB/DefaultBlockSize + 1);
        const auto response =
            client.RecvResponse<TEvVolume::TEvCheckRangeResponse>();

        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);

        runtime.DispatchEvents(options, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, response->GetStatus());
    }

    Y_UNIT_TEST(ShouldCheckRangeFromAllReplicas)
    {
        constexpr ui32 replicaCount = 3;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);
        client.WriteBlocks(
            TBlockRange64::MakeClosedInterval(0, 1024),
            1);
        ui32 checksumResponseCount = 0;
        TActorId recepient;
        TMap<TActorId, TSet<TActorId>> actorIds;
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvNonreplPartitionPrivate::EvChecksumBlocksResponse: {
                        ++checksumResponseCount;
                        recepient = event->Recipient;
                        actorIds[event->Recipient].insert(event->Sender);
                        break;
                    }
                    case TEvService::EvReadBlocksResponse: {
                        actorIds[event->Recipient].insert(event->Sender);
                        break;
                    }
                }
                return TTestActorRuntime::DefaultObserverFunc(event);
            });
        client.CheckRange("disk-id", 0, 1024, replicaCount);
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvCheckRangeResponse);
        // When requesting a read for three replicas, Readings are made from
        // one replica, and checksums are calculated from the other two.
        UNIT_ASSERT_VALUES_EQUAL(replicaCount - 1, checksumResponseCount);
        UNIT_ASSERT_VALUES_EQUAL(replicaCount, actorIds[recepient].size());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
