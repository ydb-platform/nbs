#include "part_mirror.h"
#include "part_mirror_actor.h"
#include "part_mirror_resync_actor.h"
#include "part_mirror_resync_util.h"
#include "part_nonrepl_actor.h"
#include "ut_env.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
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
using namespace NKikimr;
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TResyncController
{
    TTestActorRuntime& Runtime;

    enum EResyncState {
        INIT,
        RANGE_STARTED,
        RANGE_FINISHED,
        RESYNC_FINISHED,
    };

    bool ResyncFinished = false;
    TVector<TBlockRange64> ResyncedRanges;

    ui32 StopAfterResyncedRangeCount = 0xffffffffu;

    TResyncController(TTestActorRuntime& runtime)
        : Runtime(runtime)
    {
        runtime.SetReschedulingDelay(ResyncNextRangeInterval);

        runtime.SetObserverFunc([this] (auto& event) {
            switch (event->GetTypeRewrite()) {
                case TEvNonreplPartitionPrivate::EvChecksumBlocksRequest: {
                    if (ResyncedRanges.size() >= StopAfterResyncedRangeCount) {
                        return TTestActorRuntime::EEventAction::RESCHEDULE;
                    }

                    break;
                }

                case TEvVolume::EvResyncFinished: {
                    ResyncFinished = true;
                    Cerr << "resync finished" << Endl;

                    break;
                }

                case TEvNonreplPartitionPrivate::EvRangeResynced: {
                    using TEvent =
                        TEvNonreplPartitionPrivate::TEvRangeResynced;
                    auto* msg = event->template Get<TEvent>();
                    Cerr << "resynced range "
                        << DescribeRange(msg->Range) << Endl;
                    ResyncedRanges.push_back(msg->Range);

                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });
    }

    void SetStopAfterResyncedRangeCount(ui32 count)
    {
        StopAfterResyncedRangeCount = count;
    }

    void WaitForResyncedRangeCount(ui32 count)
    {
        TDispatchOptions options;
        options.CustomFinalCondition = [&]
        {
            return ResyncedRanges.size() >= count;
        };
        Runtime.DispatchEvents(options, ResyncNextRangeInterval);
    }

    void WaitForResyncFinished()
    {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvVolume::EvResyncFinished);
        Runtime.DispatchEvents(options, TDuration::Seconds(5));
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TTestEnv
{
    TTestActorRuntime& Runtime;
    const ui32 BlockSize;

    TActorId MirrorActorId;
    TActorId VolumeActorId;
    TStorageStatsServiceStatePtr StorageStatsServiceState;
    TDiskAgentStatePtr DiskAgentState;
    TVector<TDevices> Replicas;

    TStorageConfigPtr Config;
    TNonreplicatedPartitionConfigPtr PartConfig;
    TVector<TActorId> ReplicaActors;
    TActorId ActorId;
    ui64 RequestIdentityKey = 0;

    TResyncController ResyncController;

    TTestActorRuntime::TEventObserver PrevObs;
    TDeque<IEventHandlePtr> Caught;

    static void AddDevice(
        ui32 nodeId,
        ui32 blockCount,
        ui32 blockSize,
        TString name,
        TDevices& devices)
    {
        const auto k = blockSize / DefaultDeviceBlockSize;

        auto& device = *devices.Add();
        device.SetNodeId(nodeId);
        device.SetBlocksCount(blockCount * k);
        device.SetDeviceUUID(name);
        device.SetBlockSize(DefaultDeviceBlockSize);
    }

    static TDevices DefaultDevices(ui64 nodeId, ui32 blockSize)
    {
        TDevices devices;
        AddDevice(nodeId, 2048 * 4_KB / blockSize, blockSize, "vasya", devices);
        AddDevice(nodeId, 3072 * 4_KB / blockSize, blockSize, "petya", devices);
        //AddDevice(0, 1024 * 4_KB / blockSize, blockSize, "", devices);

        return devices;
    }

    static TDevices DefaultReplica(ui64 nodeId, ui32 blockSize, ui32 replicaId)
    {
        auto devices = DefaultDevices(nodeId, blockSize);
        for (auto& device: devices) {
            if (device.GetDeviceUUID()) {
                device.SetDeviceUUID(TStringBuilder() << device.GetDeviceUUID()
                    << "#" << replicaId);
            }
        }
        return devices;
    }

    explicit TTestEnv(TTestActorRuntime& runtime, THashSet<TString> freshDeviceIds = {})
        : TTestEnv(
            runtime,
            DefaultBlockSize,
            DefaultDevices(runtime.GetNodeId(0), DefaultBlockSize),
            TVector<TDevices>{
                DefaultReplica(runtime.GetNodeId(0), DefaultBlockSize, 1),
                DefaultReplica(runtime.GetNodeId(0), DefaultBlockSize, 2),
            },
            std::move(freshDeviceIds)
        )
    {
    }

    TTestEnv(TTestActorRuntime& runtime, ui32 blockSize)
        : TTestEnv(
            runtime,
            blockSize,
            DefaultDevices(runtime.GetNodeId(0), blockSize),
            TVector<TDevices>{
                DefaultReplica(runtime.GetNodeId(0), blockSize, 1),
                DefaultReplica(runtime.GetNodeId(0), blockSize, 2),
            },
            {}
        )
    {
    }

    TTestEnv(
            TTestActorRuntime& runtime,
            ui32 blockSize,
            TDevices devices,
            TVector<TDevices> replicas,
            THashSet<TString> freshDeviceIds)
        : Runtime(runtime)
        , BlockSize(blockSize)
        , MirrorActorId(0, "YYY")
        , VolumeActorId(0, "VVV")
        , StorageStatsServiceState(MakeIntrusive<TStorageStatsServiceState>())
        , DiskAgentState(std::make_shared<TDiskAgentState>())
        , Replicas(std::move(replicas))
        , ResyncController(runtime)
    {
        SetupLogging();

        Runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetMaxTimedOutDeviceStateDuration(20'000);
        storageConfig.SetNonReplicatedMinRequestTimeoutSSD(1'000);
        storageConfig.SetNonReplicatedMaxRequestTimeoutSSD(5'000);

        Config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        auto nodeId = Runtime.GetNodeId(0);

        TDevices allDevices;
        for (const auto& d: devices) {
            allDevices.Add()->CopyFrom(d);
        }
        for (const auto& r: Replicas) {
            for (const auto& d: r) {
                allDevices.Add()->CopyFrom(d);
            }
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
                ToLogicalBlocks(devices, BlockSize),
                TNonreplicatedPartitionConfig::TVolumeInfo{
                    Now(),
                    // only SSD/HDD distinction matters
                    NProto::STORAGE_MEDIA_SSD_MIRROR3},
                "test",
                BlockSize,
                VolumeActorId};
        params.FreshDeviceIds = std::move(freshDeviceIds);
        PartConfig =
            std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));

        for (auto& replica: Replicas) {
            replica = ToLogicalBlocks(replica, BlockSize);
        }

        auto mirror = std::make_unique<TMirrorPartitionActor>(
            Config,
            CreateDiagnosticsConfig(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            "", // rwClientId
            PartConfig,
            TMigrations(),
            Replicas,
            nullptr, // rdmaClient
            TActorId(), // statActorId
            // resync actor id should be set since mirror actor should be aware
            // of the fact that resync is in progress
            TActorId(0, "nobody") // resyncActorId
        );

        Runtime.AddLocalService(
            MirrorActorId,
            TActorSetupCmd(mirror.release(), TMailboxType::Simple, 0)
        );

        AddReplica(PartConfig->Fork(PartConfig->GetDevices()), "ZZZ");
        for (size_t i = 0; i < Replicas.size(); ++i) {
            AddReplica(PartConfig->Fork(Replicas[i]), Sprintf("ZZZ%zu", i));
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

        SetupTabletServices(Runtime);
    }

    void SetupLogging()
    {
        Runtime.AppendToLogSettings(
            TBlockStoreComponents::START,
            TBlockStoreComponents::END,
            GetComponentName);

        for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
           Runtime.SetLogPriority(i, NLog::PRI_INFO);
            Runtime.SetLogPriority(i, NLog::PRI_DEBUG);
        }
        Runtime.SetLogPriority(TBlockStoreComponents::PARTITION, NLog::PRI_DEBUG);
        // Runtime.SetLogPriority(NLog::InvalidComponent, NLog::PRI_DEBUG);
    }

    void StartResync(ui64 initialResyncIndex = 0)
    {
        auto actor = std::make_unique<TMirrorPartitionResyncActor>(
            Config,
            CreateDiagnosticsConfig(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            "", // rwClientId
            PartConfig,
            TMigrations(),
            Replicas,
            nullptr, // rdmaClient
            VolumeActorId,
            initialResyncIndex
        );

        actor->SetRequestIdentityKey(RequestIdentityKey);

        ActorId = Runtime.Register(actor.release(), 0);
        Runtime.DispatchEvents({}, TDuration::Seconds(1));
    }

    TVector<TString> ReadMirror(TBlockRange64 range)
    {
        return ReadActor(MirrorActorId, range);
    }

    void WriteMirror(TBlockRange64 range, char fill)
    {
        WriteActor(MirrorActorId, range, fill);
    }

    TVector<TString> ReadReplica(int idx, TBlockRange64 range)
    {
        return ReadActor(ReplicaActors[idx], range);
    }

    void WriteReplica(int idx, TBlockRange64 range, char fill)
    {
        WriteActor(ReplicaActors[idx], range, fill);
    }

    TVector<TString> ReadActor(TActorId actorId, TBlockRange64 range)
    {
        TPartitionClient client(Runtime, actorId, BlockSize);
        TVector<TString> blocks;

        client.ReadBlocksLocal(
            range,
            TGuardedSgList(ResizeBlocks(
                blocks,
                range.Size(),
                TString(BlockSize, '\0')
            )));

        return blocks;
    }

    void WriteActor(TActorId actorId, TBlockRange64 range, char fill)
    {
        TPartitionClient client(Runtime, actorId, BlockSize);
        client.WriteBlocks(range, fill);
    }

    TPartitionDiskCounters GetMirrorCounters()
    {
        Runtime.AdvanceCurrentTime(UpdateCountersInterval);
        Runtime.DispatchEvents({}, TDuration::Seconds(1));
        Runtime.AdvanceCurrentTime(UpdateCountersInterval);
        Runtime.DispatchEvents({}, TDuration::Seconds(1));
        Runtime.AdvanceCurrentTime(UpdateCountersInterval);
        Runtime.DispatchEvents({}, TDuration::Seconds(1));

        return StorageStatsServiceState->Counters;
    }

    TPartitionDiskCounters GetAggregatedMirrorCounters()
    {
        Runtime.AdvanceCurrentTime(UpdateCountersInterval);
        Runtime.DispatchEvents({}, 10ms);
        Runtime.AdvanceCurrentTime(UpdateCountersInterval);
        Runtime.DispatchEvents({}, 10ms);
        Runtime.AdvanceCurrentTime(UpdateCountersInterval);
        Runtime.DispatchEvents({}, 10ms);

        return StorageStatsServiceState->AggregatedCounters;
    }

    void AddReplica(TNonreplicatedPartitionConfigPtr partConfig, TString name)
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

    void CatchEvents(THashSet<ui32> eventTypes)
    {
        PrevObs = Runtime.SetObserverFunc([=] (auto& event) {
            if (eventTypes.contains(event->GetTypeRewrite())) {
                Caught.push_back(IEventHandlePtr(event.Release()));
                return TTestActorRuntime::EEventAction::DROP;
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });
    }

    void CatchEvents(ui32 eventType)
    {
        CatchEvents(THashSet<ui32>{eventType});
    }

    void ModifyEvents(
        ui32 eventType,
        NActors::TTestActorRuntime::TEventObserver eventModifier)
    {
        PrevObs = Runtime.SetObserverFunc(
            [=](auto& event)
            {
                if (event->GetTypeRewrite() == eventType) {
                    if (TTestActorRuntime::EEventAction::DROP ==
                        eventModifier(event))
                    {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });
    }

    void ReleaseEvents(bool restoreObs = true)
    {
        if (restoreObs) {
            Runtime.SetObserverFunc(std::move(PrevObs));
        }
        for (auto& ev: Caught) {
            Runtime.Send(ev.release());
        }
        Caught.clear();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMirrorPartitionResyncTest)
{
    Y_UNIT_TEST(ShouldForwardWriteZeroRequests)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto range = TBlockRange64::WithLength(1024, 1024);

        env.ResyncController.SetStopAfterResyncedRangeCount(0);
        env.StartResync();

        TPartitionClient resyncClient(runtime, env.ActorId);

        // Check WriteBlocks
        {
            const ui64 cookie = 11;
            auto request = resyncClient.CreateWriteBlocksRequest(range, 'A');
            resyncClient.SendRequest(
                resyncClient.GetActorId(),
                std::move(request),
                cookie);
            using TResponse = TEvService::TEvWriteBlocksResponse;
            auto response = resyncClient.RecvResponse<TResponse>(cookie);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        for (const auto& block: env.ReadMirror(range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
        }
        for (size_t i = 0; i < env.ReplicaActors.size(); ++i) {
            for (const auto& block: env.ReadReplica(i, range)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }

        // Check WriteBlocksLocal
        {
            const ui64 cookie = 22;
            const TString data(DefaultBlockSize, 'B');
            auto request = resyncClient.CreateWriteBlocksLocalRequest(
                range,
                data);
            resyncClient.SendRequest(
                resyncClient.GetActorId(),
                std::move(request),
                cookie);
            using TResponse = TEvService::TEvWriteBlocksLocalResponse;
            auto response = resyncClient.RecvResponse<TResponse>(cookie);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        for (const auto& block: env.ReadMirror(range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
        for (size_t i = 0; i < env.ReplicaActors.size(); ++i) {
            for (const auto& block: env.ReadReplica(i, range)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
            }
        }

        // Check ZeroBlocks
        {
            const ui64 cookie = 33;
            auto request = resyncClient.CreateZeroBlocksRequest(range);
            resyncClient.SendRequest(
                resyncClient.GetActorId(),
                std::move(request),
                cookie);
            using TResponse = TEvService::TEvZeroBlocksResponse;
            auto response = resyncClient.RecvResponse<TResponse>(cookie);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        for (const auto& block: env.ReadMirror(range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, '\0'), block);
        }
        for (size_t i = 0; i < env.ReplicaActors.size(); ++i) {
            for (const auto& block: env.ReadReplica(i, range)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, '\0'), block);
            }
        }

        // Check counters
        auto counters = env.GetMirrorCounters();

        UNIT_ASSERT_VALUES_EQUAL(3 * 2, counters.RequestCounters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            3 * 2 * DefaultBlockSize * range.Size(),
            counters.RequestCounters.WriteBlocks.RequestBytes);

        UNIT_ASSERT_VALUES_EQUAL(3 * 1, counters.RequestCounters.ZeroBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            3 * 1 * DefaultBlockSize * range.Size(),
            counters.RequestCounters.ZeroBlocks.RequestBytes);
    }

    Y_UNIT_TEST(ShouldSendStatisticsDuringResync)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto range = TBlockRange64::WithLength(0, 1024);

        env.ResyncController.SetStopAfterResyncedRangeCount(0);
        env.StartResync();

        TPartitionClient resyncClient(runtime, env.ActorId);

        {
            const ui64 cookie = 11;
            auto request = resyncClient.CreateReadBlocksRequest(range);
            resyncClient.SendRequest(
                resyncClient.GetActorId(),
                std::move(request),
                cookie);

            runtime.DispatchEvents({}, 1s);

            runtime.SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

            using TResponse = TEvService::TEvReadBlocksResponse;
            auto response = resyncClient.RecvResponse<TResponse>(cookie);
            UNIT_ASSERT_VALUES_EQUAL(S_OK, response->GetStatus());
        }

        // Check counters
        auto counters = env.GetAggregatedMirrorCounters();

        UNIT_ASSERT_VALUES_EQUAL(1, counters.RequestCounters.ReadBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            DefaultBlockSize * range.Size(),
            counters.RequestCounters.ReadBlocks.RequestBytes);
    }

    void DoTestShouldResyncWholeDisk(ui32 blockSize)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, blockSize);

        const auto range = TBlockRange64::WithLength(0, 5120 * 4_KB / blockSize);

        env.WriteMirror(range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'B');

        env.StartResync();
        env.ResyncController.WaitForResyncedRangeCount(5);
        UNIT_ASSERT(env.ResyncController.ResyncFinished);

        // Trigger sequential reading from different replicas
        for (const auto& block: env.ReadMirror(range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(blockSize, 'B'), block);
        }
        for (const auto& block: env.ReadMirror(range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(blockSize, 'B'), block);
        }
        for (const auto& block: env.ReadMirror(range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(blockSize, 'B'), block);
        }

        // Check individual replicas
        for (size_t i = 0; i < env.ReplicaActors.size(); ++i) {
            for (const auto& block: env.ReadReplica(i, range)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(blockSize, 'B'), block);
            }
        }

        const ui32 expectedResyncRangeSize = 4_MB;
        for (const auto& resyncRange: env.ResyncController.ResyncedRanges) {
            UNIT_ASSERT_VALUES_EQUAL(
                expectedResyncRangeSize,
                resyncRange.Size() * blockSize);
        }
        UNIT_ASSERT_VALUES_EQUAL(5, env.ResyncController.ResyncedRanges.size());
    }

    Y_UNIT_TEST(ShouldResyncWholeDisk)
    {
        DoTestShouldResyncWholeDisk(4_KB);
    }

    Y_UNIT_TEST(ShouldResyncWholeDiskWithLargeBlockSize)
    {
        DoTestShouldResyncWholeDisk(128_KB);
    }

    Y_UNIT_TEST(ShouldResyncFromStartIndex)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto diskRange = TBlockRange64::WithLength(0, 5120);
        const auto resyncRange = TBlockRange64::WithLength(1024, 4096);
        const ui64 startIndex = resyncRange.Start;

        env.WriteMirror(diskRange, 'A');
        env.WriteReplica(1, diskRange, 'B');
        env.WriteReplica(2, diskRange, 'B');

        env.StartResync(startIndex);
        env.ResyncController.WaitForResyncedRangeCount(4);
        UNIT_ASSERT(env.ResyncController.ResyncFinished);

        // This range should not be resynced
        for (const auto& block: env.ReadReplica(
                 0,
                 TBlockRange64::WithLength(0, startIndex)))
        {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
        }
        for (const auto& block: env.ReadReplica(
                 1,
                 TBlockRange64::WithLength(0, startIndex)))
        {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
        for (const auto& block: env.ReadReplica(
                 2,
                 TBlockRange64::WithLength(0, startIndex)))
        {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }

        // This range should be resynced
        for (const auto& block: env.ReadReplica(0, resyncRange)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
        for (const auto& block: env.ReadReplica(1, resyncRange)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
        for (const auto& block: env.ReadReplica(2, resyncRange)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
    }

    Y_UNIT_TEST(ShouldCorrectlyHandleWriteRequestCounter)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto range = TBlockRange64::WithLength(0, 1024);

        env.RequestIdentityKey = 42;
        env.StartResync();

        env.WriteActor(env.ActorId, range, 'A');

        // Check individual replicas
        for (size_t i = 0; i < env.ReplicaActors.size(); ++i) {
            for (const auto& block: env.ReadReplica(i, range)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldRejectIntersectingWriteZero)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.CatchEvents(TEvNonreplPartitionPrivate::EvChecksumBlocksRequest);

        env.StartResync();

        auto range = TBlockRange64::WithLength(100, 100);

        TPartitionClient client(runtime, env.ActorId);

        // Check WriteBlocks
        {
            client.SendWriteBlocksRequest(range, 'A');
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            UNIT_ASSERT_STRING_CONTAINS(response->GetErrorReason(), "intersects");
        }

        // Check WriteBlocksLocal
        {
            client.SendWriteBlocksLocalRequest(range, TString(DefaultBlockSize, 'A'));
            auto response = client.RecvWriteBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            UNIT_ASSERT_STRING_CONTAINS(response->GetErrorReason(), "intersects");
        }

        // Check ZeroBlocks
        {
            client.SendZeroBlocksRequest(range);
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
            UNIT_ASSERT_STRING_CONTAINS(response->GetErrorReason(), "intersects");
        }
    }

    Y_UNIT_TEST(ShouldPostponeResyncIfIntersectingWritePending)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto diskRange = TBlockRange64::WithLength(0, 5120);

        env.WriteReplica(0, diskRange, 'A');
        env.WriteReplica(1, diskRange, 'B');
        env.WriteReplica(2, diskRange, 'B');

        const auto range = TBlockRange64::WithLength(1100, 100);

        enum {
            INIT,
            FIRST_RANGE_RESYNCED,
            MIRROR_WRITE_STARTED,
            MIRROR_WRITE_COMPLETED,
        } state = INIT;

        ui32 writeDelay = 0;
        ui32 rangeCount = 0;

        runtime.SetReschedulingDelay(ResyncNextRangeInterval);

        runtime.SetObserverFunc([&state, &writeDelay, &rangeCount] (auto& event) {
            switch (state) {
                case INIT: {
                    if (event->GetTypeRewrite() == TEvNonreplPartitionPrivate::EvRangeResynced) {
                        state = FIRST_RANGE_RESYNCED;
                        rangeCount++;
                        return TTestActorRuntime::EEventAction::RESCHEDULE;
                    }
                    break;
                }

                case FIRST_RANGE_RESYNCED: {
                    if (event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest) {
                        state = MIRROR_WRITE_STARTED;
                    }
                    if (event->GetTypeRewrite() == TEvNonreplPartitionPrivate::EvRangeResynced) {
                        // Postpone second range resync until mirror write started
                        return TTestActorRuntime::EEventAction::RESCHEDULE;
                    }
                    break;
                }

                case MIRROR_WRITE_STARTED: {
                    if (event->GetTypeRewrite() == TEvNonreplPartitionPrivate::EvWriteOrZeroCompleted) {
                        state = MIRROR_WRITE_COMPLETED;
                    }
                    if (event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest) {
                        if (writeDelay++ < 100) {
                            // Delay mirror write to let resync actor read old data
                            return TTestActorRuntime::EEventAction::RESCHEDULE;
                        }
                    }
                    if (event->GetTypeRewrite() == TEvService::EvWriteBlocksLocalRequest) {
                        // Postpone resync write until mirror write completed
                        return TTestActorRuntime::EEventAction::RESCHEDULE;
                    }
                    break;
                }

                case MIRROR_WRITE_COMPLETED: {
                    if (event->GetTypeRewrite() == TEvNonreplPartitionPrivate::EvRangeResynced) {
                        rangeCount++;
                    }
                    break;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        env.StartResync();
        env.WriteActor(env.ActorId, range, 'C');

        while (rangeCount < 5) {
            runtime.DispatchEvents({}, ResyncNextRangeInterval);
        }

        for (size_t i = 0; i < env.ReplicaActors.size(); ++i) {
            auto blocks = env.ReadReplica(i, diskRange);
            for (size_t index = 0; index < blocks.size(); ++index) {
                if (range.Contains(index)) {
                    UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'C'), blocks[index]);
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), blocks[index]);
                }
            }
        }
    }

    Y_UNIT_TEST(ShouldForwardReadIfResyncFinished)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto diskRange = TBlockRange64::WithLength(0, 5120);
        const auto range = TBlockRange64::WithLength(100, 100);

        env.WriteReplica(0, diskRange, 'A');
        env.WriteReplica(1, diskRange, 'B');
        env.WriteReplica(2, diskRange, 'B');

        env.StartResync();
        env.ResyncController.WaitForResyncedRangeCount(5);
        UNIT_ASSERT(env.ResyncController.ResyncFinished);

        // Trigger sequential reading from different replicas
        for (const auto& block: env.ReadActor(env.ActorId, range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
        for (const auto& block: env.ReadActor(env.ActorId, range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
        for (const auto& block: env.ReadActor(env.ActorId, range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
    }

    Y_UNIT_TEST(ShouldForwardReadIfRangeResynced)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto diskRange = TBlockRange64::WithLength(0, 5120);
        const auto range = TBlockRange64::WithLength(100, 100);

        env.WriteReplica(0, diskRange, 'A');
        env.WriteReplica(1, diskRange, 'B');
        env.WriteReplica(2, diskRange, 'B');

        env.ResyncController.SetStopAfterResyncedRangeCount(1);

        env.StartResync();

        env.ResyncController.WaitForResyncedRangeCount(1);
        UNIT_ASSERT(!env.ResyncController.ResyncFinished);

        // Trigger sequential reading from different replicas
        for (const auto& block: env.ReadActor(env.ActorId, range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
        for (const auto& block: env.ReadActor(env.ActorId, range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }
        for (const auto& block: env.ReadActor(env.ActorId, range)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
        }

        // Check that after resync request to first replica get correct response
        {
            TPartitionClient client(runtime, env.ActorId);

            auto response = client.ReadBlocks(range, 1);
            const auto& blocks = response->Record.GetBlocks();
            UNIT_ASSERT_VALUES_EQUAL(100, blocks.BuffersSize());
            for (ui32 i = 0; i < 100; ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 'B'),
                    blocks.GetBuffers(i));
            }
        }

        // Make sure resync process is stopped after first range
        for (const auto& block: env.ReadReplica(
                 0,
                 TBlockRange64::MakeClosedInterval(1024, diskRange.End)))
        {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
        }
    }

#define TEST_NO_EVENT(runtime, evType) {                                       \
    runtime.DispatchEvents({}, TInstant::Zero());                              \
    auto evList = runtime.CaptureEvents();                                     \
    for (auto& ev: evList) {                                                   \
        UNIT_ASSERT(ev->GetTypeRewrite() != evType);                           \
    }                                                                          \
    runtime.PushEventsFront(evList);                                           \
} // TEST_NO_EVENT

    Y_UNIT_TEST(ShouldPostponeReadIfRangeNotResynced)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.CatchEvents(TEvNonreplPartitionPrivate::EvChecksumBlocksRequest);

        env.StartResync();

        auto range = TBlockRange64::WithLength(100, 100);

        TPartitionClient client(runtime, env.ActorId);
        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'B');

        // Check ReadBlocks

        {
            client.SendReadBlocksRequest(range);
            TEST_NO_EVENT(runtime, TEvService::EvReadBlocksResponse);

            env.ReleaseEvents();
            env.ResyncController.WaitForResyncedRangeCount(1);

            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            for (ui32 i = 0; i < range.Size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 'B'),
                    response->Record.GetBlocks().GetBuffers(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldPostponeReadFromOneReplicaIfRangeNotResynced)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.CatchEvents(TEvNonreplPartitionPrivate::EvChecksumBlocksRequest);

        env.StartResync();

        auto range = TBlockRange64::WithLength(100, 100);

        TPartitionClient client(runtime, env.ActorId);
        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'B');

        // Check ReadBlocks from first replica

        {
            client.SendReadBlocksRequest(range, 1);
            TEST_NO_EVENT(runtime, TEvService::EvReadBlocksResponse);

            env.ReleaseEvents();
            env.ResyncController.WaitForResyncedRangeCount(1);

            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            for (ui32 i = 0; i < range.Size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 'B'),
                    response->Record.GetBlocks().GetBuffers(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldPostponeReadFromAllReplicaIfRangeNotResynced)
    {
        constexpr ui32 replicaCount = 3;

        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.CatchEvents(TEvNonreplPartitionPrivate::EvChecksumBlocksRequest);

        env.StartResync();

        auto range = TBlockRange64::WithLength(100, 100);

        TPartitionClient client(runtime, env.ActorId);
        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'C');
        env.WriteReplica(2, range, 'C');

        {
            client.SendReadBlocksRequest(range, 0, replicaCount);
            TEST_NO_EVENT(runtime, TEvService::EvReadBlocksResponse);

            env.ReleaseEvents();
            env.ResyncController.WaitForResyncedRangeCount(1);

            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            for (ui32 i = 0; i < range.Size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 'C'),
                    response->Record.GetBlocks().GetBuffers(i));
            }
        }
    }

    Y_UNIT_TEST(ShouldRejectPostponedReadIfRangeResyncFailed)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.CatchEvents({
            TEvNonreplPartitionPrivate::EvChecksumBlocksResponse,
            TEvNonreplPartitionPrivate::EvRangeResynced,
        });

        env.StartResync();

        auto range = TBlockRange64::WithLength(100, 100);

        TPartitionClient client(runtime, env.ActorId);
        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'B');

        // Check ReadBlocks

        {
            client.SendReadBlocksRequest(range);
            // no response - EvChecksumBlocksResponse-s are suspended
            TEST_NO_EVENT(runtime, TEvService::EvReadBlocksResponse);

            // changing response code for EvChecksumBlocksResponse-s to E_IO
            // affects both fast path and resync since they started in parallel
            for (auto& ev: env.Caught) {
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<int>(TEvNonreplPartitionPrivate::EvChecksumBlocksResponse),
                    static_cast<int>(ev->GetTypeRewrite()));
                using TEvent =
                    TEvNonreplPartitionPrivate::TEvChecksumBlocksResponse;
                auto* msg = ev->template Get<TEvent>();
                *msg->Record.MutableError() = MakeError(E_IO, "request failed");
            }

            UNIT_ASSERT(env.Caught.size() > 0);

            // sending env.Caught - fast path request should fail, we should
            // transition to slow path, which should also fail due to resync failure
            env.ReleaseEvents(false);
            while (env.Caught.empty()) {
                runtime.DispatchEvents({}, ResyncNextRangeInterval);
            }
            for (auto& ev: env.Caught) {
                UNIT_ASSERT_VALUES_EQUAL(
                    static_cast<int>(TEvNonreplPartitionPrivate::EvRangeResynced),
                    static_cast<int>(ev->GetTypeRewrite()));
            }
            UNIT_ASSERT(env.Caught.size() > 0);

            // unblocking EvRangeResynced
            env.ReleaseEvents(true);

            // read fails because both fast path and resync failed
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());

            // the next read should succeed because the next resync succeeds
            client.SendReadBlocksRequest(range);
            env.ResyncController.WaitForResyncedRangeCount(1);
            response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }
    }

    Y_UNIT_TEST(ShouldPostponeLocalReadIfRangeNotResynced)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        env.CatchEvents(TEvNonreplPartitionPrivate::EvChecksumBlocksRequest);

        env.StartResync();

        auto range = TBlockRange64::WithLength(100, 100);

        TPartitionClient client(runtime, env.ActorId);
        env.WriteReplica(0, range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'B');

        // Check ReadBlocksLocal
        {
            TVector<TString> blocks;

            client.SendReadBlocksLocalRequest(
                range,
                TGuardedSgList(ResizeBlocks(
                    blocks,
                    range.Size(),
                    TString(DefaultBlockSize, '\0')
                )));
            TEST_NO_EVENT(runtime, TEvService::EvReadBlocksResponse);

            env.ReleaseEvents();
            env.ResyncController.WaitForResyncedRangeCount(1);

            auto response = client.RecvReadBlocksLocalResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            for (ui32 i = 0; i < range.Size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 'B'),
                    blocks[i]);
            }
        }
    }

    Y_UNIT_TEST(ShouldResyncRangeOnReadRequest)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto diskRange = TBlockRange64::WithLength(0, 5120);
        const auto range = TBlockRange64::WithLength(2100, 100);

        env.WriteReplica(0, diskRange, 'A');
        env.WriteReplica(1, diskRange, 'B');
        env.WriteReplica(2, diskRange, 'B');

        env.ResyncController.SetStopAfterResyncedRangeCount(0);
        env.StartResync();

        // Trigger resync on read
        TPartitionClient client(runtime, env.ActorId);
        client.SendReadBlocksRequest(range);
        TEST_NO_EVENT(runtime, TEvService::EvReadBlocksResponse);

        // Consecutive reads should be rejected as well
        client.SendReadBlocksRequest(range);
        TEST_NO_EVENT(runtime, TEvService::EvReadBlocksResponse);

        // Let resync continue
        env.ResyncController.SetStopAfterResyncedRangeCount(2);
        env.ResyncController.WaitForResyncedRangeCount(2);
        UNIT_ASSERT(!env.ResyncController.ResyncFinished);

        // Now our reads should succeed
        for (ui32 i = 0; i < 2; ++i) {
            auto response = client.RecvReadBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(
                range.Size(),
                response->Record.GetBlocks().BuffersSize());
            for (const auto& buffer: response->Record.GetBlocks().GetBuffers()) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), buffer);
            }
        }

        // Check individual replicas
        for (size_t i = 0; i < env.ReplicaActors.size(); ++i) {
            for (const auto& block: env.ReadReplica(i, range)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
            }
        }

        // Check that only the read range and the first range were resynced
        auto blocks = env.ReadReplica(0, diskRange);
        for (ui32 index = 0; index < blocks.size(); ++index) {
            if ((index >= 1024 && index < 2048) || index >= 3072) {
                UNIT_ASSERT_VALUES_EQUAL(
                    TString(DefaultBlockSize, 'A'),
                    blocks[index]);
            }
        }

        // Let resync finish
        env.ResyncController.SetStopAfterResyncedRangeCount(5);
        env.ResyncController.WaitForResyncedRangeCount(5);
        env.ResyncController.WaitForResyncFinished();
        UNIT_ASSERT(env.ResyncController.ResyncFinished);

        // Check that the whole disk is resynced
        for (size_t i = 0; i < env.ReplicaActors.size(); ++i) {
            for (const auto& block: env.ReadReplica(i, diskRange)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldReadFastPathOnReadRequestDuringResync)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        const auto diskRange = TBlockRange64::WithLength(0, 5120);
        // This range should have identical content even before resync
        const auto fastPathRange = TBlockRange64::WithLength(2100, 100);
        // This range has default content, which is different before resync
        const auto slowPathRange = TBlockRange64::WithLength(3100, 200);

        env.WriteReplica(0, diskRange, 'A');
        env.WriteReplica(1, diskRange, 'B');
        env.WriteReplica(2, diskRange, 'B');

        env.WriteReplica(0, fastPathRange, 'A');
        env.WriteReplica(1, fastPathRange, 'A');
        env.WriteReplica(2, fastPathRange, 'A');

        env.ResyncController.SetStopAfterResyncedRangeCount(0);
        env.StartResync();

        TPartitionClient client(runtime, env.ActorId);

        //////////////////////////////////////////////////////////////////////
        // Fast path read attempts use one checksum request per mirror
        // Here we're going to make 2 reqs, and count from 0
        env.ResyncController.SetStopAfterResyncedRangeCount(4);
        // This read succeeds immediately
        client.SendReadBlocksRequest(fastPathRange);
        auto response = client.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(
            fastPathRange.Size(),
            response->Record.GetBlocks().BuffersSize());

        for (const auto& buffer: response->Record.GetBlocks().GetBuffers()) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), buffer);
        }

        //////////////////////////////////////////////////////////////////////
        // This read should not complete until resync of the range is finished
        client.SendReadBlocksRequest(slowPathRange);
        TEST_NO_EVENT(runtime, TEvService::EvReadBlocksResponse);

        //////////////////////////////////////////////////////////////////////
        // Let resync continue
        env.ResyncController.SetStopAfterResyncedRangeCount(6);
        env.ResyncController.WaitForResyncedRangeCount(1);
        UNIT_ASSERT(!env.ResyncController.ResyncFinished);

        // Now second read sould succeed as well
        response = client.RecvReadBlocksResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL(
            slowPathRange.Size(),
            response->Record.GetBlocks().BuffersSize());

        for (const auto& buffer: response->Record.GetBlocks().GetBuffers()) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), buffer);
        }
    }

#undef TEST_NO_EVENT

    Y_UNIT_TEST(ShouldSaveResyncIndex)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime);

        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetResyncIndexCachingInterval(2048);

        env.Config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        const auto range = TBlockRange64::WithLength(0, 5120);

        env.WriteMirror(range, 'A');
        env.WriteReplica(1, range, 'B');
        env.WriteReplica(2, range, 'B');

        ui32 resyncIndex = 0;

        runtime.SetObserverFunc([&resyncIndex] (auto& event) {
            using TEvent = TEvVolume::TEvUpdateResyncState;

            if (event->GetTypeRewrite() == TEvent::EventType) {
                auto* msg = event->template Get<TEvent>();
                resyncIndex = msg->ResyncIndex;
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        });

        env.StartResync();

        while (resyncIndex == 0) {
            runtime.DispatchEvents({}, ResyncNextRangeInterval);
        }

        UNIT_ASSERT_VALUES_EQUAL(3072, resyncIndex);
    }

    void DoTestShouldTreatFreshDevicesProperly(bool afterResync)
    {
        TTestBasicRuntime runtime;
        TTestEnv env(runtime, {"vasya#1", "vasya#2"});

        const auto range1 = TBlockRange64::WithLength(0, 2048);
        const auto range2 = TBlockRange64::WithLength(2048, 3072);

        env.WriteMirror(range1, 'A');
        env.WriteReplica(1, range1, 'B');
        env.WriteReplica(2, range1, 'B');
        env.WriteMirror(range2, 'C');
        env.WriteReplica(1, range2, 'D');
        env.WriteReplica(2, range2, 'D');

        env.CatchEvents(TEvNonreplPartitionPrivate::EvResyncNextRange);
        env.StartResync();

        auto read = [&] (TBlockRange64 range) {
            return afterResync
                ? env.ReadMirror(range)
                : env.ReadActor(env.ActorId, range);
        };

        // Read a range which doesn't really require a resync
        // Devices in replicas #1 and #2 have different data but those devices
        // are fresh so they shouldn't be taken into account
        for (const auto& block: read(range1)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
        }
        for (const auto& block: read(range1)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
        }
        for (const auto& block: read(range1)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
        }

        // unblock resync
        // XXX may trigger immediate resync and not actually test the
        // slow path that happens during resync
        env.ReleaseEvents();

        if (afterResync) {
            env.ResyncController.WaitForResyncedRangeCount(5);
            env.ResyncController.WaitForResyncFinished();
            UNIT_ASSERT(env.ResyncController.ResyncFinished);
        }

        // Trigger sequential reading from different replicas
        for (const auto& block: read(range2)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'D'), block);
        }
        for (const auto& block: read(range2)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'D'), block);
        }
        for (const auto& block: read(range2)) {
            UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'D'), block);
        }

        if (afterResync) {
            // Check individual replicas
            for (const auto& block: env.ReadReplica(0, range1)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'A'), block);
            }
            for (const auto& block: env.ReadReplica(1, range1)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
            }
            for (const auto& block: env.ReadReplica(2, range1)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'B'), block);
            }
            for (const auto& block: env.ReadReplica(0, range2)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'D'), block);
            }
            for (const auto& block: env.ReadReplica(1, range2)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'D'), block);
            }
            for (const auto& block: env.ReadReplica(2, range2)) {
                UNIT_ASSERT_VALUES_EQUAL(TString(DefaultBlockSize, 'D'), block);
            }
        }
    }

    Y_UNIT_TEST(ShouldTreatFreshDevicesProperlyAfterResync)
    {
        DoTestShouldTreatFreshDevicesProperly(true);
    }

    Y_UNIT_TEST(ShouldTreatFreshDevicesProperlyDuringResync)
    {
        DoTestShouldTreatFreshDevicesProperly(false);
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

        // Resync first range [0..1023]
        env.CatchEvents(TEvNonreplPartitionPrivate::EvResyncNextRange);
        env.StartResync();
        env.ReleaseEvents();
        env.ResyncController.WaitForResyncedRangeCount(1);

        {   // Request to first device over an already resynchronized range
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(0, 10)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            UNIT_ASSERT_STRING_CONTAINS(
                response->Device.GetDeviceUUID(),
                "vasya");
        }
        {   // Request over not synced range
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(1024, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->Error.GetCode());
        }

        // Resync second range [1024..2047].
        env.ResyncController.WaitForResyncedRangeCount(2);

        {
            // Request to first device over an already resynchronized range
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
        }

        // Resync range [2048..3095].
        env.ResyncController.WaitForResyncedRangeCount(3);

        {   // Request to second device
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
        {   // Request with write purpose
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(0, 16)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL(E_ABORTED, response->Error.GetCode());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
