#include "resync_range.h"

#include "part_nonrepl.h"
#include "part_nonrepl_actor.h"
#include "part_nonrepl_events_private.h"
#include "ut_env.h"

#include <cloud/blockstore/libs/common/block_checksum.h>
#include <cloud/blockstore/libs/diagnostics/block_digest.h>
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
    TVector<TReplicaDescriptor> Replicas;
    TActorId VolumeActorId;
    TStorageStatsServiceStatePtr StorageStatsServiceState;
    TDiskAgentStatePtr DiskAgentState;
    IBlockDigestGeneratorPtr BlockDigestGenerator;

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
        AddDevice(nodeId, 3072, "vasya", devices);
        AddDevice(nodeId, 3072, "petya", devices);
        AddDevice(nodeId, 3072, "kolya", devices);

        return devices;
    }

    explicit TTestEnv(TTestActorRuntime& runtime)
        : TTestEnv(runtime, NProto::VOLUME_IO_OK)
    {}

    TTestEnv(
            TTestActorRuntime& runtime,
            NProto::EVolumeIOMode ioMode)
        : TTestEnv(runtime, ioMode, DefaultDevices(runtime.GetNodeId(0)))
    {}

    TTestEnv(
            TTestActorRuntime& runtime,
            NProto::EVolumeIOMode ioMode,
            TDevices devices)
        : Runtime(runtime)
        , VolumeActorId(0, "VVV")
        , StorageStatsServiceState(MakeIntrusive<TStorageStatsServiceState>())
        , DiskAgentState(std::make_shared<TDiskAgentState>())
        , BlockDigestGenerator(CreateTestBlockDigestGenerator())
    {
        SetupLogging();

        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetMaxTimedOutDeviceStateDuration(20'000);
        storageConfig.SetNonReplicatedMinRequestTimeoutSSD(1'000);
        storageConfig.SetNonReplicatedMaxRequestTimeoutSSD(5'000);

        auto config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        Runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        auto nodeId = Runtime.GetNodeId(0);

        Runtime.AddLocalService(
            MakeDiskAgentServiceId(nodeId),
            TActorSetupCmd(
                new TDiskAgentMock(devices, DiskAgentState),
                TMailboxType::Simple,
                0
            )
        );

        for (int i = 0; i < devices.size(); ++i) {
            TDevices replicaDevices;
            replicaDevices.Add()->CopyFrom(devices[i]);

            TString name = Sprintf("replica-%d", i);

            TNonreplicatedPartitionConfig::
                TNonreplicatedPartitionConfigInitParams params{
                    ToLogicalBlocks(replicaDevices, DefaultBlockSize),
                    TNonreplicatedPartitionConfig::TVolumeInfo{
                        Now(),
                        // only SSD/HDD distinction matters
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED},
                    name,
                    DefaultBlockSize,
                    VolumeActorId};
            params.IOMode = ioMode;
            auto partConfig = std::make_shared<TNonreplicatedPartitionConfig>(
                std::move(params));

            auto part = std::make_unique<TNonreplicatedPartitionActor>(
                config,
                CreateDiagnosticsConfig(),
                std::move(partConfig),
                VolumeActorId
            );

            TActorId actorId(0, Sprintf("YYY%d", i));
            Runtime.AddLocalService(
                actorId,
                TActorSetupCmd(part.release(), TMailboxType::Simple, 0)
            );
            Replicas.push_back({name, static_cast<ui32>(i), actorId});
        }

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

    std::unique_ptr<TEvNonreplPartitionPrivate::TEvRangeResynced> ResyncRange(
        ui64 start,
        ui64 end,
        TVector<int> idxs,
        NProto::EResyncPolicy resyncPolicy =
            NProto::EResyncPolicy::RESYNC_POLICY_MINOR_AND_MAJOR_4MB)
    {
        auto sender = Runtime.AllocateEdgeActor(0);

        auto requestInfo = CreateRequestInfo(
            sender,
            0,
            MakeIntrusive<TCallContext>()
        );

        TVector<TReplicaDescriptor> replicas;
        for (int idx: idxs) {
            replicas.push_back(Replicas[idx]);
        }

        std::unique_ptr<IActor> actor = MakeResyncRangeActor(
            std::move(requestInfo),
            DefaultBlockSize,
            TBlockRange64::MakeClosedInterval(start, end),
            std::move(replicas),
            "",   // rwClientId
            BlockDigestGenerator,
            resyncPolicy,
            EBlockRangeChecksumStatus::Unknown,
            VolumeActorId,
            false);

        Runtime.Register(actor.release(), 0);

        TAutoPtr<IEventHandle> handle;
        Runtime.GrabEdgeEventRethrow<
            TEvNonreplPartitionPrivate::TEvRangeResynced>(handle);
        UNIT_ASSERT(handle);

        return std::unique_ptr<TEvNonreplPartitionPrivate::TEvRangeResynced>(
            handle->Release<TEvNonreplPartitionPrivate::TEvRangeResynced>().Release());
    }

    TVector<TString> ReadReplica(int idx, ui64 start, ui64 end)
    {
        TPartitionClient client(Runtime, Replicas[idx].ActorId);

        auto range = TBlockRange64::MakeClosedInterval(start, end);
        TVector<TString> blocks;

        client.ReadBlocksLocal(
            range,
            TGuardedSgList(ResizeBlocks(
                blocks,
                range.Size(),
                TString(DefaultBlockSize, '\0')
            )));

        return blocks;
    }

    void WriteReplica(int idx, ui64 start, ui64 end, char fill)
    {
        TPartitionClient client(Runtime, Replicas[idx].ActorId);
        client.WriteBlocks(TBlockRange64::MakeClosedInterval(start, end), fill);
    }

    TPartitionDiskCounters GetReplicaCounters(int idx)
    {
        TPartitionClient client(Runtime, Replicas[idx].ActorId);

        client.SendRequest(
            Replicas[idx].ActorId,
            std::make_unique<TEvNonreplPartitionPrivate::TEvUpdateCounters>()
        );

        Runtime.DispatchEvents({}, TDuration::Seconds(1));

        return StorageStatsServiceState->Counters;
    }

    template <typename TEvent>
    void InjectError(ui32 errorCode, TString errorMessage)
    {
        Runtime.SetEventFilter([=] (auto& runtime, auto& event) {
            if (event->GetTypeRewrite() == TEvent::EventType) {
                auto response = std::make_unique<TEvent>(
                    MakeError(errorCode, errorMessage));

                runtime.Send(
                    new IEventHandle(
                        event->Recipient,
                        event->Sender,
                        response.release(),
                        0,
                        event->Cookie));

                return true;
            }

            return false;
        });
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TResyncRangeTest)
{
    Y_UNIT_TEST(ShouldNotReadWriteIfChecksumsMatch)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        auto response = env.ResyncRange(0, 3071, {0, 1});
        UNIT_ASSERT(!HasError(response->GetError()));

        for (int idx: {0, 1}) {
            auto counters = env.GetReplicaCounters(idx);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.RequestCounters.WriteBlocks.Count);
        }
    }

    Y_UNIT_TEST(ShouldResyncOneReplicaOfTwo)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 3071, 'A');
        env.WriteReplica(1, 0, 3071, 'B');

        auto response = env.ResyncRange(0, 3071, {0, 1});
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto counters = env.GetReplicaCounters(0);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.WriteBlocks.Count);

            auto blocks = env.ReadReplica(0, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 1
        {
            auto counters = env.GetReplicaCounters(1);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.RequestCounters.WriteBlocks.Count);

            auto blocks = env.ReadReplica(1, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldResyncOneReplicaOfThree)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 3071, 'A');
        env.WriteReplica(1, 0, 3071, 'B');
        env.WriteReplica(2, 0, 3071, 'A');

        auto response = env.ResyncRange(0, 3071, {0, 1, 2});
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto counters = env.GetReplicaCounters(0);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.WriteBlocks.Count);

            auto blocks = env.ReadReplica(0, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 1
        {
            auto counters = env.GetReplicaCounters(1);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.RequestCounters.WriteBlocks.Count);

            auto blocks = env.ReadReplica(1, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 2
        {
            auto counters = env.GetReplicaCounters(2);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.WriteBlocks.Count);

            auto blocks = env.ReadReplica(2, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldResyncTwoReplicasOfThree)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 3071, 'A');
        env.WriteReplica(1, 0, 3071, 'B');
        env.WriteReplica(2, 0, 3071, 'C');

        auto response = env.ResyncRange(0, 3071, {0, 1, 2});
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto counters = env.GetReplicaCounters(0);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.WriteBlocks.Count);

            auto blocks = env.ReadReplica(0, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 1
        {
            auto counters = env.GetReplicaCounters(1);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.RequestCounters.WriteBlocks.Count);

            auto blocks = env.ReadReplica(1, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 2
        {
            auto counters = env.GetReplicaCounters(2);
            UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ChecksumBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(0, counters.RequestCounters.ReadBlocks.Count);
            UNIT_ASSERT_VALUES_EQUAL(2, counters.RequestCounters.WriteBlocks.Count);

            auto blocks = env.ReadReplica(2, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldResyncOnlyRequestedRange)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 3071, 'A');
        env.WriteReplica(1, 0, 3071, 'B');
        env.WriteReplica(2, 0, 3071, 'A');

        auto response = env.ResyncRange(1024, 2047, {0, 1, 2});
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto blocks = env.ReadReplica(0, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 1
        {
            auto blocks = env.ReadReplica(1, 0, 3071);
            for (size_t i = 0; i < blocks.size(); ++i) {
                if (i >= 1024 && i <= 2047) {
                    UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), blocks[i]);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), blocks[i]);
                }
            }
        }

        // Check replica 2
        {
            auto blocks = env.ReadReplica(2, 0, 3071);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldProvideMetricsInResponse)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 3071, 'A');
        env.WriteReplica(1, 0, 3071, 'B');

        runtime.UpdateCurrentTime(TInstant::Seconds(10));

        runtime.SetEventFilter([&] (auto& runtime, auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvNonreplPartitionPrivate::EvChecksumBlocksCompleted:
                case TEvNonreplPartitionPrivate::EvReadBlocksCompleted:
                case TEvNonreplPartitionPrivate::EvWriteBlocksCompleted:
                    runtime.AdvanceCurrentTime(TDuration::Seconds(1));
            }

            return false;
        });

        auto response = env.ResyncRange(1024, 2047, {0, 1});
        UNIT_ASSERT(!HasError(response->GetError()));

        UNIT_ASSERT_VALUES_EQUAL(1024, response->Range.Start);
        UNIT_ASSERT_VALUES_EQUAL(2047, response->Range.End);

        UNIT_ASSERT_VALUES_EQUAL(10, response->ChecksumStartTs.Seconds());
        UNIT_ASSERT_VALUES_EQUAL(2, response->ChecksumDuration.Seconds());

        UNIT_ASSERT_VALUES_EQUAL(12, response->ReadStartTs.Seconds());
        UNIT_ASSERT_VALUES_EQUAL(1, response->ReadDuration.Seconds());

        UNIT_ASSERT_VALUES_EQUAL(13, response->WriteStartTs.Seconds());
        UNIT_ASSERT_VALUES_EQUAL(1, response->WriteDuration.Seconds());

        UNIT_ASSERT_VALUES_EQUAL(1024, response->AffectedBlockInfos.size());
        for (ui64 i = 0; i < response->AffectedBlockInfos.size(); ++i) {
            ui64 index = 1024 + i;
            TString data(DefaultBlockSize, 'A');
            ui64 digest = *env.BlockDigestGenerator->ComputeDigest(
                index, TBlockDataRef(data.data(), data.size()));

            UNIT_ASSERT_VALUES_EQUAL(index, response->AffectedBlockInfos[i].BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(digest, response->AffectedBlockInfos[i].Checksum);
        }
    }

    Y_UNIT_TEST(ShouldHandleRequestErrors)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 3071, 'A');
        env.WriteReplica(1, 0, 3071, 'B');

        {
            env.InjectError<TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse>(
                E_REJECTED, "checksum error");

            auto response = env.ResyncRange(0, 3071, {0, 1});
            UNIT_ASSERT(HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL("checksum error", response->GetError().GetMessage());
        }

        {
            env.InjectError<TEvService::TEvReadBlocksLocalResponse>(
                E_REJECTED, "read error");

            auto response = env.ResyncRange(0, 3071, {0, 1});
            UNIT_ASSERT(HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL("read error", response->GetError().GetMessage());
        }

        {
            env.InjectError<TEvService::TEvWriteBlocksLocalResponse>(
                E_REJECTED, "write error");

            auto response = env.ResyncRange(0, 3071, {0, 1});
            UNIT_ASSERT(HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL("write error", response->GetError().GetMessage());
        }
    }

    Y_UNIT_TEST(ShouldHealMinorBlocksMismatchBlockByBlock)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 1023, 'A');
        env.WriteReplica(1, 0, 1023, 'A');
        env.WriteReplica(2, 0, 1023, 'A');

        // Make one minor error in each replica.
        env.WriteReplica(0, 10, 10, 'x');
        env.WriteReplica(1, 11, 11, 'x');
        env.WriteReplica(2, 12, 12, 'x');

        auto response = env.ResyncRange(
            0,
            1023,
            {0, 1, 2},
            NProto::EResyncPolicy::RESYNC_POLICY_MINOR_BLOCK_BY_BLOCK);
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto blocks = env.ReadReplica(0, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 1
        {
            auto blocks = env.ReadReplica(1, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 2
        {
            auto blocks = env.ReadReplica(2, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    void DoShouldNotReplaceMajorBlocksMismatchAccordingToPolicy(
        NProto::EResyncPolicy resyncPolicy)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 1023, 'A');
        env.WriteReplica(1, 0, 1023, 'A');
        env.WriteReplica(2, 0, 1023, 'A');

        // Make major error.
        const ui64 blockWithMajorError = 10;
        env.WriteReplica(0, blockWithMajorError, blockWithMajorError, 'x');
        env.WriteReplica(1, blockWithMajorError, blockWithMajorError, 'y');

        auto response = env.ResyncRange(0, 1023, {0, 1, 2}, resyncPolicy);
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto blocks = env.ReadReplica(0, 0, 1023);
            int i = 0;
            for (const auto& block: blocks) {
                char expected = i == blockWithMajorError ? 'x' : 'A';
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, expected), block);
                ++i;
            }
        }

        // Check replica 1
        {
            auto blocks = env.ReadReplica(1, 0, 1023);
            int i = 0;
            for (const auto& block: blocks) {
                char expected = i == blockWithMajorError ? 'y' : 'A';
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, expected), block);
                ++i;
            }
        }

        // Check replica 2
        {
            auto blocks = env.ReadReplica(2, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldNotReplaceMajorBlocksMismatchAccordingToPolicy)
    {
        DoShouldNotReplaceMajorBlocksMismatchAccordingToPolicy(
            NProto::EResyncPolicy::RESYNC_POLICY_MINOR_4MB);
    }

    Y_UNIT_TEST(ShouldNotReplaceMajorBlocksMismatchAccordingToPolicyBlockByBlock)
    {
        DoShouldNotReplaceMajorBlocksMismatchAccordingToPolicy(
            NProto::EResyncPolicy::RESYNC_POLICY_MINOR_BLOCK_BY_BLOCK);
    }

    Y_UNIT_TEST(ShouldHealMinorErrorsAndSelectBestHealerForMajorSource)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 1023, 'A');
        env.WriteReplica(1, 0, 1023, 'A');
        env.WriteReplica(2, 0, 1023, 'A');

        // Add two minor errors for replica #0 and #1 each.
        // For replica #2 add only one minor error, it should be elected as
        // major source.
        env.WriteReplica(0, 11, 11, 'x');
        env.WriteReplica(0, 12, 12, 'x');
        env.WriteReplica(1, 13, 13, 'x');
        env.WriteReplica(1, 14, 14, 'x');
        env.WriteReplica(2, 15, 15, 'x');

        // Add major error
        env.WriteReplica(0, 20, 20, 'w');
        env.WriteReplica(1, 20, 20, 'z');

        auto response = env.ResyncRange(
            0,
            1023,
            {0, 1, 2},
            NProto::EResyncPolicy::RESYNC_POLICY_MINOR_AND_MAJOR_BLOCK_BY_BLOCK);
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto blocks = env.ReadReplica(0, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 1
        {
            auto blocks = env.ReadReplica(1, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 2
        {
            auto blocks = env.ReadReplica(2, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldSelectFirstReplicaAsBestHealerWhenAllHaveSameMinorErrors)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 1023, 'A');
        env.WriteReplica(1, 0, 1023, 'A');
        env.WriteReplica(2, 0, 1023, 'A');

        // Add two minor errors for each replica.
        env.WriteReplica(0, 11, 11, 'x');
        env.WriteReplica(0, 12, 12, 'x');
        env.WriteReplica(1, 13, 13, 'x');
        env.WriteReplica(1, 14, 14, 'x');
        env.WriteReplica(2, 15, 15, 'x');
        env.WriteReplica(2, 16, 16, 'x');

        // Add major error
        env.WriteReplica(1, 20, 20, 'w');
        env.WriteReplica(2, 20, 20, 'z');
        env.WriteReplica(1, 30, 40, 'w');
        env.WriteReplica(2, 30, 40, 'z');

        // Replica #0 should be used for fixing major error.
        auto response = env.ResyncRange(
            0,
            1023,
            {0, 1, 2},
            NProto::EResyncPolicy::RESYNC_POLICY_MINOR_AND_MAJOR_BLOCK_BY_BLOCK);
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto blocks = env.ReadReplica(0, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 1
        {
            auto blocks = env.ReadReplica(1, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 2
        {
            auto blocks = env.ReadReplica(2, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldNotMessReplicaWhenOnlyMinorErrorsHealed)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.WriteReplica(0, 0, 1023, 'A');
        env.WriteReplica(1, 0, 1023, 'A');
        env.WriteReplica(2, 0, 1023, 'A');

        // Add minor and major errors for replica #1 and #2.
        env.WriteReplica(1, 0, 0, 'y');   // block 0 - major
        env.WriteReplica(1, 1, 1, 'y');   // block 1 - minor
        env.WriteReplica(2, 0, 0, 'z');   // block 0 - major
        env.WriteReplica(2, 2, 2, 'z');   // block 2 - minor

        auto response = env.ResyncRange(
            0,
            1023,
            {0, 1, 2},
            NProto::EResyncPolicy::RESYNC_POLICY_MINOR_BLOCK_BY_BLOCK);
        UNIT_ASSERT(!HasError(response->GetError()));

        // Check replica 0
        {
            auto blocks = env.ReadReplica(0, 0, 1023);
            for (const auto& block: blocks) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check replica 1
        {
            auto blocks = env.ReadReplica(1, 0, 1023);
            size_t blockIndx = 0;
            for (const auto& block: blocks) {
                const char expected = (blockIndx == 0) ? 'y' : 'A';
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, expected),
                    block);
                ++blockIndx;
            }
        }

        // Check replica 2
        {
            size_t blockIndx = 0;
            auto blocks = env.ReadReplica(2, 0, 1023);
            for (const auto& block: blocks) {
                const char expected = (blockIndx == 0) ? 'z' : 'A';
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, expected),
                    block);
                ++blockIndx;
            }
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
