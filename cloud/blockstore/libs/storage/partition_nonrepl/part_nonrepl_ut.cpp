#include "part_nonrepl.h"

#include "part_nonrepl_actor.h"
#include "ut_env.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/common/iovector.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/stats_service.h>
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
#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    TTestActorRuntime& Runtime;
    TActorId ActorId;
    TActorId VolumeActorId;
    TStorageStatsServiceStatePtr StorageStatsServiceState;
    TDiskAgentStatePtr DiskAgentState;

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

    struct TParams
    {
        NProto::EVolumeIOMode IOMode = NProto::VOLUME_IO_OK;
        bool MuteIOErrors = false;
        NProto::EStorageMediaKind MediaKind =
            NProto::STORAGE_MEDIA_SSD_NONREPLICATED;

        TDevices Devices;
    };

    explicit TTestEnv(TTestActorRuntime& runtime)
        : TTestEnv(runtime, TParams{})
    {}

    TTestEnv(TTestActorRuntime& runtime, TParams params)
        : Runtime(runtime)
        , ActorId(0, "YYY")
        , VolumeActorId(0, "VVV")
        , StorageStatsServiceState(MakeIntrusive<TStorageStatsServiceState>())
        , DiskAgentState(std::make_shared<TDiskAgentState>())
    {
        if (params.Devices.empty()) {
            params.Devices = DefaultDevices(runtime.GetNodeId(0));
        }

        SetupLogging();

        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetMaxTimedOutDeviceStateDuration(20'000);
        if (params.MediaKind == NProto::STORAGE_MEDIA_HDD_NONREPLICATED) {
            storageConfig.SetNonReplicatedMinRequestTimeoutSSD(60'000);
            storageConfig.SetNonReplicatedMaxRequestTimeoutSSD(60'000);
            storageConfig.SetNonReplicatedMinRequestTimeoutHDD(1'000);
            storageConfig.SetNonReplicatedMaxRequestTimeoutHDD(5'000);
        } else {
            storageConfig.SetNonReplicatedMinRequestTimeoutSSD(1'000);
            storageConfig.SetNonReplicatedMaxRequestTimeoutSSD(5'000);
            storageConfig.SetNonReplicatedMinRequestTimeoutHDD(60'000);
            storageConfig.SetNonReplicatedMaxRequestTimeoutHDD(60'000);
        }
        storageConfig.SetNonReplicatedAgentMaxTimeout(300'000);
        storageConfig.SetAssignIdToWriteAndZeroRequestsEnabled(true);

        auto config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        auto nodeId = Runtime.GetNodeId(0);

        Runtime.AddLocalService(
            MakeDiskAgentServiceId(nodeId),
            TActorSetupCmd(
                new TDiskAgentMock(params.Devices, DiskAgentState),
                TMailboxType::Simple,
                0
            )
        );

        auto partConfig = std::make_shared<TNonreplicatedPartitionConfig>(
            ToLogicalBlocks(params.Devices, DefaultBlockSize),
            params.IOMode,
            "test",
            DefaultBlockSize,
            TNonreplicatedPartitionConfig::TVolumeInfo{Now(), params.MediaKind},
            VolumeActorId,
            params.MuteIOErrors,
            THashSet<TString>(), // freshDeviceIds
            TDuration::Zero(), // maxTimedOutDeviceStateDuration
            false, // maxTimedOutDeviceStateDurationOverridden
            false // useSimpleMigrationBandwidthLimiter
        );

        auto part = std::make_unique<TNonreplicatedPartitionActor>(
            std::move(config),
            CreateDiagnosticsConfig(),
            std::move(partConfig),
            VolumeActorId
        );

        Runtime.AddLocalService(
            ActorId,
            TActorSetupCmd(part.release(), TMailboxType::Simple, 0)
        );

        auto dummy = std::make_unique<TDummyActor>();

        Runtime.AddLocalService(
            VolumeActorId,
            TActorSetupCmd(dummy.release(), TMailboxType::Simple, 0)
        );

        Runtime.AddLocalService(
            MakeStorageStatsServiceId(),
            TActorSetupCmd(
                new TStorageStatsServiceMock(StorageStatsServiceState),
                TMailboxType::Simple,
                0
            )
        );

        SetupTabletServices(Runtime);
    }

    void SetupLogging()
    {
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        // for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
        //    Runtime.SetLogPriority(i, NLog::PRI_DEBUG);
        // }
        // Runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    }

    void KillDiskAgent()
    {
        auto sender = Runtime.AllocateEdgeActor();
        auto nodeId = Runtime.GetNodeId(0);

        auto request = std::make_unique<TEvents::TEvPoisonPill>();

        Runtime.Send(new IEventHandle(
            MakeDiskAgentServiceId(nodeId),
            sender,
            request.release()));

        Runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNonreplicatedPartitionTest)
{
    Y_UNIT_TEST(ShouldReadWriteZero)
    {
        TTestBasicRuntime runtime;

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
            auto response =
                client.ReadBlocks(TBlockRange64::WithLength(1024, 3072));
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
            auto response =
                client.ReadBlocks(TBlockRange64::WithLength(1024, 3072));
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

        client.WriteBlocks(TBlockRange64::WithLength(5000, 200), 3);
        client.ZeroBlocks(TBlockRange64::MakeClosedInterval(5050, 5150));

        {
            auto response = client.ReadBlocks(
                TBlockRange64::WithLength(5000, 200));
            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(200, blocks.BuffersSize());
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

            for (ui32 i = 120; i < 200; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(),
                    blocks.GetBuffers(i)
                );
            }
        }

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        UNIT_ASSERT_VALUES_EQUAL(4, counters.ReadBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * 9336,
            counters.ReadBlocks.RequestBytes
        );
        UNIT_ASSERT_VALUES_EQUAL(3, counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * 6192,
            counters.WriteBlocks.RequestBytes
        );
        UNIT_ASSERT_VALUES_EQUAL(2, counters.ZeroBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * 1070,
            counters.ZeroBlocks.RequestBytes
        );
    }

    Y_UNIT_TEST(ShouldLocalReadWrite)
    {
        TTestBasicRuntime runtime;

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

        const auto blockRange2 = TBlockRange64::WithLength(5000, 200);
        client.WriteBlocksLocal(blockRange2, TString(DefaultBlockSize, 'B'));

        const auto blockRange3 = TBlockRange64::MakeClosedInterval(5000, 5150);

        {
            TVector<TString> blocks;

            client.ReadBlocksLocal(
                blockRange3,
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    blockRange3.Size(),
                    TString(DefaultBlockSize, '\0')
                )));

            for (ui32 i = 0; i < 120; ++i) {
                const auto& block = blocks[i];
                for (auto c: block) {
                    UNIT_ASSERT_VALUES_EQUAL('B', c);
                }
            }

            for (ui32 i = 120; i < blockRange3.Size(); ++i) {
                const auto& block = blocks[i];
                for (auto c: block) {
                    UNIT_ASSERT_VALUES_EQUAL(0, c);
                }
            }
        }

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        auto& transportCounters =
            env.StorageStatsServiceState->Counters.Interconnect;
        UNIT_ASSERT_VALUES_EQUAL(2, counters.ReadBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            transportCounters.ReadCount.Value,
            counters.ReadBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * (
                blockRange1.Size() + blockRange3.Intersect(diskRange).Size()
            ),
            counters.ReadBlocks.RequestBytes
        );
        UNIT_ASSERT_VALUES_EQUAL(
            transportCounters.ReadBytes.Value,
            counters.ReadBlocks.RequestBytes);
        UNIT_ASSERT_VALUES_EQUAL(2, counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            transportCounters.WriteCount.Value,
            counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * (
                blockRange1.Size() + blockRange2.Intersect(diskRange).Size()
            ),
            counters.WriteBlocks.RequestBytes
        );
        UNIT_ASSERT_VALUES_EQUAL(
            transportCounters.WriteBytes.Value,
            counters.WriteBlocks.RequestBytes);
    }

    Y_UNIT_TEST(ShouldWriteLargeBuffer)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        client.WriteBlocks(TBlockRange64::WithLength(1024, 3072), 1, 2048);

        {
            auto response =
                client.ReadBlocks(TBlockRange64::WithLength(0, 5120));
            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(5120, blocks.BuffersSize());

            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 0),
                blocks.GetBuffers(0)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 0),
                blocks.GetBuffers(1023)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 1),
                blocks.GetBuffers(1024)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 1),
                blocks.GetBuffers(4095)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 0),
                blocks.GetBuffers(4096)
            );
            UNIT_ASSERT_VALUES_EQUAL(
                TString(DefaultBlockSize, 0),
                blocks.GetBuffers(5119)
            );
        }

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        auto& transportCounters =
            env.StorageStatsServiceState->Counters.Interconnect;
        UNIT_ASSERT_VALUES_EQUAL(
            transportCounters.WriteCount.Value,
            counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * 3072,
            counters.WriteBlocks.RequestBytes);
        UNIT_ASSERT_VALUES_EQUAL(
            transportCounters.WriteBytes.Value,
            counters.WriteBlocks.RequestBytes);
    }

    Y_UNIT_TEST(ShouldReadWriteZeroWithMonsterDisk)
    {
        TTestBasicRuntime runtime;

        TDevices devices;
        const ui64 blocksPerDevice = 93_GB / DefaultBlockSize;
        for (ui32 i = 0; i < 1024; ++i) {
            TTestEnv::AddDevice(
                runtime.GetNodeId(0),
                blocksPerDevice,
                Sprintf("vasya%u", i),
                devices
            );
        }

        TTestEnv env(runtime, {.Devices = std::move(devices)});

        TPartitionClient client(runtime, env.ActorId);

        auto range1 = TBlockRange64::WithLength(0, 1024);
        auto range2 = TBlockRange64::WithLength(blocksPerDevice * 511, 1024);
        auto range3 = TBlockRange64::WithLength(blocksPerDevice * 1023, 1024);

#define TEST_READ(range, data) {                                               \
            auto response = client.ReadBlocks(range);                          \
            const auto& blocks = response->Record.GetBlocks();                 \
                                                                               \
            UNIT_ASSERT_VALUES_EQUAL(1024, blocks.BuffersSize());              \
            UNIT_ASSERT_VALUES_EQUAL(                                          \
                DefaultBlockSize,                                              \
                blocks.GetBuffers(0).size()                                    \
            );                                                                 \
            UNIT_ASSERT_VALUES_EQUAL(                                          \
                data,                                                          \
                blocks.GetBuffers(0)                                           \
            );                                                                 \
                                                                               \
            UNIT_ASSERT_VALUES_EQUAL(                                          \
                DefaultBlockSize,                                              \
                blocks.GetBuffers(1023).size()                                 \
            );                                                                 \
            UNIT_ASSERT_VALUES_EQUAL(                                          \
                data,                                                          \
                blocks.GetBuffers(1023)                                        \
            );                                                                 \
        }

        TEST_READ(range1, TString(DefaultBlockSize, 0));
        TEST_READ(range2, TString(DefaultBlockSize, 0));
        TEST_READ(range3, TString(DefaultBlockSize, 0));

        client.WriteBlocks(range1, 1);
        client.WriteBlocks(range2, 2);
        client.WriteBlocks(range3, 3);

        TEST_READ(range1, TString(DefaultBlockSize, 1));
        TEST_READ(range2, TString(DefaultBlockSize, 2));
        TEST_READ(range3, TString(DefaultBlockSize, 3));

        client.ZeroBlocks(range2);

        TEST_READ(range1, TString(DefaultBlockSize, 1));
        TEST_READ(range2, TString(DefaultBlockSize, 0));
        TEST_READ(range3, TString(DefaultBlockSize, 3));
    }

    Y_UNIT_TEST(ShouldChecksumBlocks)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        const auto blockRange = TBlockRange64::WithLength(1024, 3072);
        client.WriteBlocksLocal(blockRange, TString(DefaultBlockSize, 'A'));

        TString data(blockRange.Size() * DefaultBlockSize, 'A');
        TBlockChecksum checksum;
        checksum.Extend(data.data(), data.size());

        {
            auto response = client.ChecksumBlocks(blockRange);
            UNIT_ASSERT_VALUES_EQUAL(checksum.GetValue(), response->Record.GetChecksum());
        }

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        UNIT_ASSERT_VALUES_EQUAL(1, counters.ChecksumBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * blockRange.Size(),
            counters.ChecksumBlocks.RequestBytes
        );
    }

    Y_UNIT_TEST(ShouldHandleUndeliveredIO)
    {
        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        env.KillDiskAgent();

        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));

            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("request timed out"));
        }

        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(1024, 3072),
                1);
            runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("request timed out"));
        }

        {
            client.SendZeroBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("request timed out"));
        }
    }

    void DoTestShouldHandleTimedoutIO(NProto::EStorageMediaKind mediaKind)
    {
        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime, {.MediaKind = mediaKind});
        env.DiskAgentState->ResponseDelay = TDuration::Max();

        auto& counters = env.StorageStatsServiceState->Counters.Simple;

        TPartitionClient client(runtime, env.ActorId);

        // cumulative = 0.0s
        // timeout = 1s (NonReplicatedMinRequestTimeoutSSD())
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // cumulative = 1.0s
        // timeout = 1.5s (cumulative + 0.5s)
        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(1024, 3072),
                1);
            runtime.DispatchEvents();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // cumulative = 2.5s (1.0 + 1.5)
        // timeout = 3s (cumulative + 0.5s)
        {
            client.SendZeroBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // cumulative = 5.5s (1.0 + 1.5 + 3.0)
        // timeout = 5s (limited by NonReplicatedMaxRequestTimeoutSSD())
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
        }

        // cumulative = 10.5 (1.0 + 1.5 + 3.0 + 5.0)
        // timeout = 5s (limited by NonReplicatedMaxRequestTimeoutSSD())
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
        }

        // cumulative not exceeds MaxTimedOutDeviceStateDuration(), and the disk
        // is not considered broken yet
        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(0, counters.HasBrokenDevice.Value);
        UNIT_ASSERT_VALUES_EQUAL(0, counters.HasBrokenDeviceSilent.Value);

        // cumulative = 15.5 (1.0 + 1.5 + 3.0 + 5.0 + 5.0)
        // timeout = 5s (limited by NonReplicatedMaxRequestTimeoutSSD())
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
        }

        // cumulative is now 20.5 and it exceeds MaxTimedOutDeviceStateDuration(), the disk
        // is considered as silently broken
        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(0, counters.HasBrokenDevice.Value);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HasBrokenDeviceSilent.Value);

        // the following attempts should get E_IO / E_IO_SILENT
        // since cumulative value exceeded MaxTimedOutDeviceStateDuration()
        // cumulative = 20.5 (1.0 + 1.5 + 3.0 + 5.0 + 5.0)
        // timeout = 5s (limited by NonReplicatedMaxRequestTimeoutSSD())
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO_SILENT, response->GetStatus());
        }

        // after a cooldown the error shouldn't be silent anymore and the disk
        // is considered as broken
        runtime.AdvanceCurrentTime(TDuration::Minutes(5));

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, counters.HasBrokenDevice.Value);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HasBrokenDeviceSilent.Value);

        // after a cooldown the error shouldn't be silent anymore
        {
            runtime.AdvanceCurrentTime(TDuration::Minutes(5));
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetStatus());
        }

        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(1024, 3072),
                1);
            runtime.DispatchEvents();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetStatus());
        }

        {
            client.SendZeroBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetStatus());
        }

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(1, counters.HasBrokenDevice.Value);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HasBrokenDeviceSilent.Value);
    }

    Y_UNIT_TEST(ShouldHandleTimedoutIOSSD)
    {
        DoTestShouldHandleTimedoutIO(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldHandleTimedoutIOHDD)
    {
        DoTestShouldHandleTimedoutIO(NProto::STORAGE_MEDIA_HDD_NONREPLICATED);
    }

    void DoTestShouldUseResponseTimeHistoryForTimeouts(NProto::EStorageMediaKind mediaKind)
    {
        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime, {.MediaKind = mediaKind});
        env.DiskAgentState->ResponseDelay = TDuration::MilliSeconds(1'200);

        TPartitionClient client(runtime, env.ActorId);

        // timeout = 1s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.AdvanceCurrentTime(env.DiskAgentState->ResponseDelay);
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // backoff = 0.5s
        // cumulative = 1.5s
        // timeout = 2s
        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(1024, 3072),
                1);
            runtime.AdvanceCurrentTime(env.DiskAgentState->ResponseDelay);
            runtime.DispatchEvents();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        env.DiskAgentState->ResponseDelay = TDuration::MilliSeconds(2'000);

        // timeout = 1s + 1.5s
        {
            client.SendZeroBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.AdvanceCurrentTime(env.DiskAgentState->ResponseDelay);
            runtime.DispatchEvents();
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        env.DiskAgentState->ResponseDelay = TDuration::MilliSeconds(2'500);

        // timeout = 1s + 2s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        env.DiskAgentState->ResponseDelay = TDuration::MilliSeconds(5'000);

        // timeout = 1s + 2.5s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.AdvanceCurrentTime(env.DiskAgentState->ResponseDelay);
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }
    }

    Y_UNIT_TEST(ShouldUseResponseTimeHistoryForTimeoutsSSD)
    {
        DoTestShouldUseResponseTimeHistoryForTimeouts(NProto::STORAGE_MEDIA_SSD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldUseResponseTimeHistoryForTimeoutsHDD)
    {
        DoTestShouldUseResponseTimeHistoryForTimeouts(NProto::STORAGE_MEDIA_HDD_NONREPLICATED);
    }

    Y_UNIT_TEST(ShouldNotReturnIOErrorUponTimeoutForBackgroundRequests)
    {
        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime);
        env.DiskAgentState->ResponseDelay = TDuration::Max();

        auto& counters = env.StorageStatsServiceState->Counters.Simple;

        TPartitionClient client(runtime, env.ActorId);

        for (ui32 i = 0; i < 10; ++i) {
            auto request = client.CreateReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            request->Record.MutableHeaders()->SetIsBackgroundRequest(true);
            client.SendRequest(client.GetActorId(), std::move(request));
            runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        // Background requests transfer the device into an error state, despite
        // the fact that they themselves receive timeouts.
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HasBrokenDevice.Value);
        UNIT_ASSERT_VALUES_EQUAL(1, counters.HasBrokenDeviceSilent.Value);

        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldRecoverFromShortTimeoutStreak)
    {
        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime);
        env.DiskAgentState->ResponseDelay = TDuration::Max();

        TPartitionClient client(runtime, env.ActorId);

        // timeout = 1s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // backoff = 0.5s
        // cumulative = 1.5s
        // timeout = 2s
        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(1024, 3072),
                1);
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // backoff = 1s
        // cumulative = 4.5s
        // timeout = 4s
        {
            client.SendZeroBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // backoff = 1.5s
        // cumulative = 10s
        // timeout = 5s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
        }

        env.DiskAgentState->ResponseDelay = TDuration::Zero();
        // backoff = 2s
        // cumulative = 17s
        // timeout = 0s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
        env.DiskAgentState->ResponseDelay = TDuration::Max();

        // timeout = 1s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // backoff = 0.5s
        // cumulative = 1.5s
        // timeout = 2s
        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(1024, 3072),
                1);
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // backoff = 1s
        // cumulative = 4.5s
        // timeout = 4s
        {
            client.SendZeroBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // backoff = 1.5s
        // cumulative = 10s
        // timeout = 5s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
        }

        // backoff = 2s
        // cumulative = 17s
        // timeout = 5s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
        }

        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            //runtime.AdvanceCurrentTime(TDuration::Minutes(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
        }

        // the following attempt should get E_IO / E_IO_SILENT
        // backoff = 2.5s
        // cumulative = 24.5s
        // timeout = 5s
        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO_SILENT, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldLimitIO)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);
        env.DiskAgentState->ResponseDelay = TDuration::Max();

        TPartitionClient client(runtime, env.ActorId);

        for (int i = 0; i != 1024; ++i) {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
        }

        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(response->GetErrorReason(), "Inflight limit reached");
        }

        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(1024, 3072),
                1);
            runtime.DispatchEvents();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(response->GetErrorReason(), "Inflight limit reached");
        }

        {
            client.SendZeroBlocksRequest(
                TBlockRange64::WithLength(1024, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL(response->GetErrorReason(), "Inflight limit reached");
        }
    }

    Y_UNIT_TEST(ShouldHandleInvalidSessionError)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        TActorId reacquireDiskRecipient;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvDiskAgent::EvReadDeviceBlocksRequest: {
                        auto response = std::make_unique<TEvDiskAgent::TEvReadDeviceBlocksResponse>(
                            MakeError(E_BS_INVALID_SESSION, "invalid session")
                        );

                        runtime.Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie
                        ), 0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    case TEvDiskAgent::EvWriteDeviceBlocksRequest: {
                        auto response = std::make_unique<TEvDiskAgent::TEvWriteDeviceBlocksResponse>(
                            MakeError(E_BS_INVALID_SESSION, "invalid session")
                        );

                        runtime.Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie
                        ), 0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    case TEvDiskAgent::EvZeroDeviceBlocksRequest: {
                        auto response = std::make_unique<TEvDiskAgent::TEvZeroDeviceBlocksResponse>(
                            MakeError(E_BS_INVALID_SESSION, "invalid session")
                        );

                        runtime.Send(new IEventHandle(
                            event->Sender,
                            event->Recipient,
                            response.release(),
                            0, // flags
                            event->Cookie
                        ), 0);

                        return TTestActorRuntime::EEventAction::DROP;
                    }

                    case TEvVolume::EvReacquireDisk: {
                        reacquireDiskRecipient = event->Recipient;

                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        {
            client.SendReadBlocksRequest(
                TBlockRange64::WithLength(0, 1024));
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(env.VolumeActorId, reacquireDiskRecipient);

        reacquireDiskRecipient = {};

        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(0, 1024),
                1);
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(env.VolumeActorId, reacquireDiskRecipient);

        reacquireDiskRecipient = {};

        {
            client.SendZeroBlocksRequest(
                TBlockRange64::WithLength(0, 1024));
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }

        UNIT_ASSERT_VALUES_EQUAL(env.VolumeActorId, reacquireDiskRecipient);
    }

    Y_UNIT_TEST(ShouldSupportReadOnlyMode)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime, {.IOMode = NProto::VOLUME_IO_ERROR_READ_ONLY});

        TPartitionClient client(runtime, env.ActorId);

        TString expectedBlockData(DefaultBlockSize, 0);

        auto readBlocks = [&]
        {
            auto response = client.ReadBlocks(
                TBlockRange64::WithLength(1024, 3072));
            const auto& blocks = response->Record.GetBlocks();

            UNIT_ASSERT_VALUES_EQUAL(3072, blocks.BuffersSize());
            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(0).size());
            UNIT_ASSERT_VALUES_EQUAL(expectedBlockData, blocks.GetBuffers(0));

            UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(3071).size());
            UNIT_ASSERT_VALUES_EQUAL(expectedBlockData, blocks.GetBuffers(3071));
        };

        readBlocks();

        {
            client.SendWriteBlocksRequest(
                TBlockRange64::WithLength(1024, 3072),
                1);
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetStatus());
        }
        {
            client.SendWriteBlocksRequest(
                TBlockRange64::MakeClosedInterval(1024, 4023),
                2);
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetStatus());
        }

        readBlocks();

        {
            client.SendZeroBlocksRequest(
                TBlockRange64::MakeClosedInterval(2024, 3023));
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_IO, response->GetStatus());
        }

        readBlocks();

        expectedBlockData = TString(DefaultBlockSize, 'A');
        {
            auto request = client.CreateWriteBlocksLocalRequest(
                TBlockRange64::WithLength(1024, 3072),
                expectedBlockData);
            request->Record.MutableHeaders()->SetIsBackgroundRequest(true);
            client.SendRequest(client.GetActorId(), std::move(request));
            auto response = client.RecvWriteBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        readBlocks();
    }

    Y_UNIT_TEST(ShouldHandleReadIOError)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime, {
            .IOMode = NProto::VOLUME_IO_ERROR_READ_ONLY,
            .MuteIOErrors = true
        });

        TPartitionClient client(runtime, env.ActorId);

        runtime.SetEventFilter(
            [&](auto&, TAutoPtr<IEventHandle>& event)
            {
                if (event->GetTypeRewrite() ==
                    TEvDiskAgent::EvReadDeviceBlocksResponse)
                {
                    auto response = std::make_unique<
                        TEvDiskAgent::TEvReadDeviceBlocksResponse>(MakeError(
                        MAKE_SYSTEM_ERROR(EIO),
                        "async IO operation failed"));

                    std::unique_ptr<IEventHandle> handle{new IEventHandle(
                        event->Recipient,
                        event->Sender,
                        response.release(),
                        0,
                        event->Cookie)};
                    event.Reset(handle.release());
                }

                return false;
            });

        client.SendReadBlocksRequest(
            TBlockRange64::MakeClosedInterval(0, 1024));

        auto response = client.RecvReadBlocksResponse();
        UNIT_ASSERT_C(
            HasProtoFlag(response->GetError().GetFlags(), NProto::EF_SILENT),
            FormatError(response->GetError()));
        UNIT_ASSERT_C(
            HasProtoFlag(
                response->GetError().GetFlags(),
                NProto::EF_HW_PROBLEMS_DETECTED),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldSendStatsToVolume)
    {
        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime);

        bool done = false;
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvStatsService::EvVolumePartCounters:
                        if (event->Recipient == MakeStorageStatsServiceId()) {
                            done = true;
                        }
                        break;
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        TPartitionClient client(runtime, env.ActorId);

        {
            runtime.AdvanceCurrentTime(TDuration::Seconds(15));
            runtime.DispatchEvents({}, TDuration::Seconds(1));
            UNIT_ASSERT_VALUES_EQUAL(true, done);
        }
    }

    Y_UNIT_TEST(ShouldRecoverWithAgentBackFromUnavailable)
    {
        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime);
        env.DiskAgentState->ResponseDelay = TDuration::Max();

        TPartitionClient client(runtime, env.ActorId);

        // wait for vasya
        for (;;) {
            client.SendReadBlocksRequest(
                TBlockRange64::MakeClosedInterval(0, 1024));
            runtime.AdvanceCurrentTime(TDuration::Seconds(10));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            if (response->GetStatus() == E_IO_SILENT) {
                break;
            }
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // read from petya
        {
            client.SendReadBlocksRequest(
                TBlockRange64::MakeClosedInterval(2048, 3072));
            runtime.DispatchEvents();
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // Get timeout from petya since device is not in an error state.
        {
            client.SendWriteBlocksRequest(
                TBlockRange64::MakeClosedInterval(2048, 3072),
                1);
            runtime.DispatchEvents();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_TIMEOUT, response->GetStatus());
            UNIT_ASSERT(response->GetErrorReason().Contains("timed out"));
        }

        // Back from unavailable
        env.DiskAgentState->ResponseDelay = TDuration::MilliSeconds(10);

        // Read OK.
        {
            auto response =
                client.ReadBlocks(TBlockRange64::WithLength(0, 1024));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
        {
            auto response =
                client.ReadBlocks(TBlockRange64::WithLength(2048, 1024));
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // Write OK.
        {
            auto response =
                client.WriteBlocks(TBlockRange64::WithLength(0, 1024), 1);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
        {
            auto response =
                client.WriteBlocks(TBlockRange64::WithLength(2048, 1024), 1);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldAccountTimeoutsFromParallelRequestsCorrectly)
    {
        constexpr size_t RequestCount = 10;

        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime);
        env.DiskAgentState->ResponseDelay = TDuration::Max();

        TPartitionClient client(runtime, env.ActorId);

        auto makeParallelRequests = [&](EWellKnownResultCodes expectedResponse)
        {
            for (size_t i = 0; i < RequestCount; ++i) {
                client.SendReadBlocksRequest(TBlockRange64::MakeOneBlock(i));
            }

            runtime.DispatchEvents();

            for (size_t i = 0; i < RequestCount; ++i) {
                auto response = client.RecvReadBlocksResponse();
                UNIT_ASSERT_VALUES_EQUAL(
                    expectedResponse,
                    response->GetStatus());
            }
        };

        // accumulated = 0.0, timeout = 1.0
        makeParallelRequests(E_TIMEOUT);
        // accumulated = 1.0, timeout = 1.5,
        makeParallelRequests(E_TIMEOUT);
        // accumulated = 2.5, timeout = 3.0,
        makeParallelRequests(E_TIMEOUT);
        // accumulated = 5.5, timeout = 5.0,
        makeParallelRequests(E_TIMEOUT);
        // accumulated = 10.5, timeout = 5.0,
        makeParallelRequests(E_TIMEOUT);
        // accumulated = 15.5, timeout = 5.0,
        makeParallelRequests(E_TIMEOUT);
        // accumulated = 20.5, timeout = 5.0,
        makeParallelRequests(E_IO_SILENT);
        // accumulated = 25.5, timeout = 5.0,
        makeParallelRequests(E_IO_SILENT);
        // advance time to skip cooldown period
        runtime.AdvanceCurrentTime(TDuration::Minutes(5));
        makeParallelRequests(E_IO);
    }

    Y_UNIT_TEST(ShouldUpdateStats)
    {
        TTestBasicRuntime runtime;

        runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        TTestEnv env(runtime);

        auto& counters = env.StorageStatsServiceState->Counters.Cumulative;

        TPartitionClient client(runtime, env.ActorId);

        client.WriteBlocks(TBlockRange64::WithLength(0, 1024), 1);
        client.ReadBlocks(TBlockRange64::WithLength(0, 512));
        client.ZeroBlocks(TBlockRange64::WithLength(0, 1024));

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        runtime.DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(2048 * 4096, counters.BytesWritten.Value);
        UNIT_ASSERT_VALUES_EQUAL(512 * 4096, counters.BytesRead.Value);
    }

    Y_UNIT_TEST(ShouldSetVolumeRequestIdForNonBackgroundRequests)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(runtime);

        TPartitionClient client(runtime, env.ActorId);

        TActorId reacquireDiskRecipient;

        ui64 interceptedVolumeRequestId = 0;
        auto takeVolumeRequestId = [&](TAutoPtr<IEventHandle>& event)
        {
            switch (event->GetTypeRewrite()) {
                case TEvDiskAgent::EvWriteDeviceBlocksRequest: {
                    using TEvent = TEvDiskAgent::TEvWriteDeviceBlocksRequest;
                    auto* msg = event->template Get<TEvent>();
                    interceptedVolumeRequestId =
                        msg->Record.GetVolumeRequestId();
                } break;
                case TEvDiskAgent::EvZeroDeviceBlocksRequest: {
                    using TEvent = TEvDiskAgent::TEvZeroDeviceBlocksRequest;
                    auto* msg = event->template Get<TEvent>();
                    interceptedVolumeRequestId =
                        msg->Record.GetVolumeRequestId();
                } break;
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime.SetObserverFunc(takeVolumeRequestId);

        {   // Check WriteBlocks
            auto doWriteBlocks = [&](bool isBackground, ui64 volumeRequestId)
            {
                interceptedVolumeRequestId = 0;
                auto request = client.CreateWriteBlocksRequest(
                    TBlockRange64::WithLength(1024, 3072),
                    'A');
                request->Record.MutableHeaders()->SetIsBackgroundRequest(
                    isBackground);
                client.SendRequest(
                    client.GetActorId(),
                    std::move(request),
                    volumeRequestId);
                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                runtime.DispatchEvents();
                auto response = client.RecvWriteBlocksResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            };

            doWriteBlocks(false, 101);
            UNIT_ASSERT_VALUES_EQUAL(101, interceptedVolumeRequestId);

            doWriteBlocks(true, 102);
            UNIT_ASSERT_VALUES_EQUAL(0, interceptedVolumeRequestId);
        }

        {   // Check WriteBlocksLocal
            auto doWriteBlocksLocal =
                [&](bool isBackground, ui64 volumeRequestId)
            {
                const TString data(DefaultBlockSize, 'B');
                interceptedVolumeRequestId = 0;
                auto request = client.CreateWriteBlocksLocalRequest(
                    TBlockRange64::WithLength(1024, 1024),
                    data);
                request->Record.MutableHeaders()->SetIsBackgroundRequest(
                    isBackground);
                client.SendRequest(
                    client.GetActorId(),
                    std::move(request),
                    volumeRequestId);
                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                runtime.DispatchEvents();
                auto response = client.RecvWriteBlocksLocalResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            };

            doWriteBlocksLocal(false, 101);
            UNIT_ASSERT_VALUES_EQUAL(101, interceptedVolumeRequestId);

            doWriteBlocksLocal(true, 102);
            UNIT_ASSERT_VALUES_EQUAL(0, interceptedVolumeRequestId);
        }

        {   // Check ZeroBlocks
            auto doZeroBlocks = [&](bool isBackground, ui64 volumeRequestId)
            {
                interceptedVolumeRequestId = 0;
                auto request = client.CreateZeroBlocksRequest(
                    TBlockRange64::WithLength(1024, 3072));
                request->Record.MutableHeaders()->SetIsBackgroundRequest(
                    isBackground);
                client.SendRequest(
                    client.GetActorId(),
                    std::move(request),
                    volumeRequestId);
                runtime.AdvanceCurrentTime(TDuration::Seconds(1));
                runtime.DispatchEvents();
                auto response = client.RecvZeroBlocksResponse();
                UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
            };

            doZeroBlocks(false, 101);
            UNIT_ASSERT_VALUES_EQUAL(101, interceptedVolumeRequestId);

            doZeroBlocks(true, 102);
            UNIT_ASSERT_VALUES_EQUAL(0, interceptedVolumeRequestId);
        }
    }

    Y_UNIT_TEST(ShouldReadVoidBuffers)
    {
        const ui32 blockCount = 16;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        // Write 3 blocks from 10 to 12.
        auto dirtyBlocks = TBlockRange64::WithLength(10, 3);
        client.WriteBlocks(dirtyBlocks, 100);

        // Read 16 blocks.
        ui32 voidBlockCount = 0;
        auto countVoidBlocks = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvReadDeviceBlocksResponse)
            {
                auto& msg =
                    *event->Get<TEvDiskAgent::TEvReadDeviceBlocksResponse>();
                voidBlockCount += CountVoidBuffers(msg.Record.GetBlocks());
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime.SetObserverFunc(countVoidBlocks);

        auto request = client.CreateReadBlocksRequest(
            TBlockRange64::WithLength(0, blockCount));
        request->Record.MutableHeaders()->SetOptimizeNetworkTransfer(
            NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS);
        client.SendRequest(client.GetActorId(), std::move(request));

        auto response = client.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            blockCount - dirtyBlocks.Size(),
            voidBlockCount);
        voidBlockCount = 0;

        const auto& blocks = response->Record.GetBlocks();
        UNIT_ASSERT_VALUES_EQUAL(blockCount, blocks.BuffersSize());
        UNIT_ASSERT_VALUES_EQUAL(DefaultBlockSize, blocks.GetBuffers(0).size());
        size_t i = 0;
        for (const auto& buffer: blocks.GetBuffers()) {
            if (dirtyBlocks.Contains(i)) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 100),
                    buffer);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 0), buffer);
            }
            ++i;
        }

        // Check statistics for requests with SKIP_VOID_BLOCKS.
        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>());
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            3 * DefaultBlockSize,
            counters.ReadBlocks.GetRequestNonVoidBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            13 * DefaultBlockSize,
            counters.ReadBlocks.GetRequestVoidBytes());

        // Check statistics for requests without SKIP_VOID_BLOCKS.
        auto secondResponse =
            client.ReadBlocks(TBlockRange64::WithLength(0, blockCount));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, secondResponse->GetError().GetCode());

        const auto& blocks2 = secondResponse->Record.GetBlocks();

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>());
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(0, voidBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters.ReadBlocks.GetRequestNonVoidBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.ReadBlocks.GetRequestVoidBytes());

        // Verify that the data read by the first and second requests are
        // identical.
        for (size_t i = 0; i < blockCount; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(
                blocks.GetBuffers(i),
                blocks2.GetBuffers(i));
        }
    }

    Y_UNIT_TEST(ShouldReadLocalVoidBuffers)
    {
        const ui32 blockCount = 16;
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        // Write 3 blocks from 10 to 12.
        auto dirtyBlocks = TBlockRange64::WithLength(10, 3);
        client.WriteBlocks(dirtyBlocks, 100);

        // Read 16 blocks.
        ui32 voidBlockCount = 0;
        auto countVoidBlocks = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvReadDeviceBlocksResponse)
            {
                auto& msg =
                    *event->Get<TEvDiskAgent::TEvReadDeviceBlocksResponse>();
                voidBlockCount += CountVoidBuffers(msg.Record.GetBlocks());
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime.SetObserverFunc(countVoidBlocks);

        // Create local buffer and fill with some data
        const size_t dataSize = blockCount * DefaultBlockSize;
        TString buffer(dataSize, 100);
        TSgList sgList{TBlockDataRef{buffer.data(), buffer.size()}};
        auto request = client.CreateReadBlocksLocalRequest(
            TBlockRange64::WithLength(0, blockCount),
            TGuardedSgList(sgList));

        request->Record.MutableHeaders()->SetOptimizeNetworkTransfer(
            NProto::EOptimizeNetworkTransfer::SKIP_VOID_BLOCKS);
        client.SendRequest(client.GetActorId(), std::move(request));

        auto response = client.RecvReadBlocksLocalResponse();

        UNIT_ASSERT_VALUES_EQUAL(
            blockCount - dirtyBlocks.Size(),
            voidBlockCount);
        voidBlockCount = 0;

        size_t i = 0;
        auto sgListOrError = SgListNormalize(sgList, DefaultBlockSize);
        UNIT_ASSERT(!HasError(sgListOrError));
        auto blocks = sgListOrError.ExtractResult();
        for (const auto& buffer: blocks) {
            if (dirtyBlocks.Contains(i)) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 100),
                    buffer.AsStringBuf());
            } else {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 0),
                    buffer.AsStringBuf());
            }
            ++i;
        }

        // Check statistics for requests with SKIP_VOID_BLOCKS.
        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>());
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(
            3 * DefaultBlockSize,
            counters.ReadBlocks.GetRequestNonVoidBytes());
        UNIT_ASSERT_VALUES_EQUAL(
            13 * DefaultBlockSize,
            counters.ReadBlocks.GetRequestVoidBytes());

        // Check statistics for requests without SKIP_VOID_BLOCKS.
        TString buffer2(dataSize, 100);
        TSgList sgList2{TBlockDataRef{buffer2.data(), buffer2.size()}};
        auto secondResponse = client.ReadBlocksLocal(
            TBlockRange64::WithLength(0, blockCount),
            TGuardedSgList(sgList2));
        UNIT_ASSERT_VALUES_EQUAL(S_OK, secondResponse->GetError().GetCode());

        client.SendRequest(
            env.ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>());
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(0, voidBlockCount);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            counters.ReadBlocks.GetRequestNonVoidBytes());
        UNIT_ASSERT_VALUES_EQUAL(0, counters.ReadBlocks.GetRequestVoidBytes());

        // Verify that the data read by the first and second requests are
        // identical.
        UNIT_ASSERT_EQUAL(buffer, buffer2);
    }

    Y_UNIT_TEST(ShouldHandleGetDeviceForRangeRequest)
    {
        using TEvGetDeviceForRangeRequest =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest;
        using TEvGetDeviceForRangeResponse =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse;
        using EPurpose = TEvGetDeviceForRangeRequest::EPurpose;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);
        TPartitionClient client(runtime, env.ActorId);

        {   // Request to first device
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(2040, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL("vasya", response->Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(2040, 8),
                response->DeviceBlockRange);
        }
        {
            // Request to second device
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(2048, 8)));
            auto response = client.RecvResponse<
                TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL("petya", response->Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(0, 8),
                response->DeviceBlockRange);
        }
        {   // Request on the border of two devices
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(2040, 16)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->Error.GetCode());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
