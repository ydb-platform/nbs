#include "part_nonrepl_migration.h"
#include "part_nonrepl_migration_actor.h"
#include "ut_env.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/rdma_test/client_test.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/core/proto_helpers.h>
#include <cloud/blockstore/libs/storage/disk_agent/actors/direct_copy_actor.h>
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
using namespace std::chrono_literals;

namespace {

////////////////////////////////////////////////////////////////////////////////

// The block count migrated at a time.
constexpr ui64 ProcessingBlockCount = MigrationRangeSize / DefaultBlockSize;

auto MakeLeaderFollowerFilter(
    TActorId* leaderPartition,
    TActorId* followerPartition)
{
    using TEvGetDeviceForRangeRequest =
        TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest;
    using EPurpose =
        TEvNonreplPartitionPrivate::TGetDeviceForRangeRequest::EPurpose;

    auto findLeaderAndFollower = [leaderPartition, followerPartition](
                                     TTestActorRuntimeBase& runtime,
                                     TAutoPtr<IEventHandle>& event) -> bool
    {
        Y_UNUSED(runtime);

        switch (event->GetTypeRewrite()) {
            case TEvService::EvReadBlocksRequest: {
                *leaderPartition = event->Recipient;
            } break;

            case TEvService::EvWriteBlocksRequest: {
                *followerPartition = event->Recipient;
            } break;

            case TEvNonreplPartitionPrivate::EvGetDeviceForRangeRequest: {
                auto& msg = *event->Get<TEvGetDeviceForRangeRequest>();
                switch (msg.Purpose) {
                    case EPurpose::ForReading:
                        *leaderPartition = event->Recipient;
                        break;
                    case EPurpose::ForWriting:
                        *followerPartition = event->Recipient;
                        break;
                }
            } break;
        }

        return false;
    };
    return findLeaderAndFollower;
}

struct TTestEnv
{
    TTestActorRuntime& Runtime;
    TActorId ActorId;
    TActorId VolumeActorId;
    TStorageStatsServiceStatePtr StorageStatsServiceState;
    TDiskAgentStatePtr DiskAgentState;
    NRdma::IClientPtr RdmaClient;

    static void InitDevice(
        ui32 nodeId,
        ui32 blockCount,
        TString name,
        NProto::TDeviceConfig* device)
    {
        const auto k = DefaultBlockSize / DefaultDeviceBlockSize;

        device->SetNodeId(nodeId);
        device->SetBlocksCount(blockCount * k);
        device->SetDeviceUUID(name);
        device->SetBlockSize(DefaultDeviceBlockSize);
        device->SetAgentId(Sprintf("agent-%u", nodeId));
    }

    static TDevices DefaultDevices(ui64 nodeId)
    {
        TDevices devices;
        InitDevice(nodeId, 2048, "vasya", devices.Add());
        InitDevice(nodeId, 3072, "petya", devices.Add());

        return devices;
    }

    static TMigrations DefaultMigrations(ui64 nodeId)
    {
        TMigrations migrations;
        auto* m = migrations.Add();
        m->SetSourceDeviceId("petya");
        auto* d = m->MutableTargetDevice();
        InitDevice(nodeId, 3072, "petya_migration", d);

        return migrations;
    }

    explicit TTestEnv(
            TTestActorRuntime& runtime,
            TDevices devices,
            TMigrations migrations,
            NProto::EVolumeIOMode ioMode,
            bool useRdma,
            TMigrationStatePtr migrationState,
            const NProto::TStorageServiceConfig& storageConfigPatch)
        : Runtime(runtime)
        , ActorId(0, "YYY")
        , VolumeActorId(0, "VVV")
        , StorageStatsServiceState(MakeIntrusive<TStorageStatsServiceState>())
        , DiskAgentState(std::make_shared<TDiskAgentState>())
    {
        if (useRdma) {
            RdmaClient = std::make_shared<TRdmaClientTest>();
        }

        SetupLogging();

        DiskAgentState->CreateDirectCopyActorFunc =
            [](const TEvDiskAgent::TEvDirectCopyBlocksRequest::TPtr& ev,
               const NActors::TActorContext& ctx,
               NActors::TActorId owner)
        {
            auto* msg = ev->Get();
            auto& record = msg->Record;
            NCloud::Register<TDirectCopyActor>(
                ctx,
                owner,
                CreateRequestInfo(ev->Sender, ev->Cookie, msg->CallContext),
                std::move(record),
                1000);
        };

        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetMaxTimedOutDeviceStateDuration(20'000);
        storageConfig.SetNonReplicatedMinRequestTimeoutSSD(1'000);
        storageConfig.SetNonReplicatedMaxRequestTimeoutSSD(5'000);
        storageConfig.SetMaxMigrationBandwidth(500);
        storageConfig.SetInitialRetryDelayForServiceRequests(10);
        storageConfig.SetMigrationIndexCachingInterval(1024);
        storageConfig.MergeFrom(storageConfigPatch);

        auto config = std::make_shared<TStorageConfig>(
            std::move(storageConfig),
            std::make_shared<NFeatures::TFeaturesConfig>(
                NCloud::NProto::TFeaturesConfig())
        );

        auto nodeId = Runtime.GetNodeId(0);

        TDevices agentDevices = devices;
        for (auto& m: migrations) {
            if (m.GetTargetDevice().GetDeviceUUID()) {
                *agentDevices.Add() = m.GetTargetDevice();

                ToLogicalBlocks(*m.MutableTargetDevice(), DefaultBlockSize);
            }
        }

        Runtime.SetRegistrationObserverFunc(
            [] (auto& runtime, const auto& parentId, const auto& actorId)
        {
            Y_UNUSED(parentId);
            runtime.EnableScheduleForActor(actorId);
        });

        Runtime.AddLocalService(
            MakeDiskAgentServiceId(nodeId),
            TActorSetupCmd(
                new TDiskAgentMock(agentDevices, DiskAgentState),
                TMailboxType::Simple,
                0
            )
        );

        TNonreplicatedPartitionConfig::TNonreplicatedPartitionConfigInitParams
            params{
                ToLogicalBlocks(devices, DefaultBlockSize),
                TNonreplicatedPartitionConfig::TVolumeInfo{
                    .CreationTs = Now(),
                    // only SSD/HDD distinction matters
                    .MediaKind = NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                    .EncryptionMode = NProto::NO_ENCRYPTION},
                "test",
                DefaultBlockSize,
                VolumeActorId};
        params.IOMode = ioMode;
        params.UseSimpleMigrationBandwidthLimiter = false;
        auto partConfig =
            std::make_shared<TNonreplicatedPartitionConfig>(std::move(params));

        auto part = CreateNonreplicatedPartitionMigration(
            std::move(config),
            CreateDiagnosticsConfig(),
            CreateProfileLogStub(),
            CreateBlockDigestGeneratorStub(),
            0,
            "", // rwClientId
            std::move(partConfig),
            std::move(migrations),
            RdmaClient,
            VolumeActorId // statActorId
        );

        Runtime.AddLocalService(
            ActorId,
            TActorSetupCmd(part.release(), TMailboxType::Simple, 0)
        );

        auto dummy = std::make_unique<TDummyActor>(std::move(migrationState));

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

        for (ui32 i = TBlockStoreComponents::START; i < TBlockStoreComponents::END; ++i) {
            Runtime.SetLogPriority(i, NLog::PRI_INFO);
        }
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

    TRdmaClientTest& Rdma()
    {
        return static_cast<TRdmaClientTest&>(*RdmaClient);
    }
};

struct TMigrationMessageHandler
{
    TVector<ui64> VolumeRequestIds;
    ui64 MigrationRequestCount = 0;

    template <typename TEv>
    void Handle(TAutoPtr<IEventHandle>& event)
    {
        if (event->GetTypeRewrite() == TEv::EventType) {
            MigrationRequestCount += 1;
            auto* ev = static_cast<TEv*>(event->GetBase());
            VolumeRequestIds.emplace_back(GetVolumeRequestId(*ev));
        }
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNonreplicatedPartitionMigrationTest)
{
    void DoShouldMirrorRequestsAfterAllDataIsMigrated(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            nullptr,   // no migration state
            storageConfigPatch);

        TPartitionClient client(runtime, env.ActorId);

        // petya should be migrated => 3 ranges
        WaitForMigrations(runtime, 3);

        const auto blockRange = TBlockRange64::MakeOneBlock(2048);
        client.WriteBlocksLocal(blockRange, TString(DefaultBlockSize, 'A'));

        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        const size_t migrationCopyBlocks = useDirectCopy ? 3 : 0;
        UNIT_ASSERT_VALUES_EQUAL(migrationCopyBlocks, counters.CopyBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            migrationCopyBlocks * MigrationRangeSize,
            counters.CopyBlocks.RequestBytes);

        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        const size_t migrationWrites = useDirectCopy ? 0 : 3;
        UNIT_ASSERT_VALUES_EQUAL(
            2 + migrationWrites,
            counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            (2 + migrationWrites * ProcessingBlockCount) * DefaultBlockSize,
            counters.WriteBlocks.RequestBytes);
    }

    Y_UNIT_TEST(ShouldMirrorRequestsAfterAllDataIsMigrated)
    {
        DoShouldMirrorRequestsAfterAllDataIsMigrated(false);
    }

    Y_UNIT_TEST(ShouldMirrorRequestsAfterAllDataIsMigratedDirectCopy)
    {
        DoShouldMirrorRequestsAfterAllDataIsMigrated(true);
    }

    void DoShouldMirrorRequestsDuringMigration(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            nullptr,   // no migration state
            storageConfigPatch);

        TPartitionClient client(runtime, env.ActorId);

        // petya should be migrated => 3 ranges
        WaitForMigrations(runtime, 3);

        const auto blockRange = TBlockRange64::WithLength(2047, 2);
        const auto buffer1 = TString(DefaultBlockSize, 'A');
        const auto buffer2 = TString(DefaultBlockSize, 'B');

        size_t writeDeviceRequestCount = 0;
        auto checkWriteDeviceRequest =
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);

            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvWriteDeviceBlocksRequest)
            {
                auto* msg =
                    event->Get<TEvDiskAgent::TEvWriteDeviceBlocksRequest>();

                writeDeviceRequestCount++;

                if (msg->Record.GetStartIndex() == 2047) {
                    const auto& blocks = msg->Record.GetBlocks().GetBuffers();
                    TBlockDataRef src{buffer1.data(), buffer1.size()};
                    TBlockDataRef dst{blocks[0].data(), DefaultBlockSize};
                    UNIT_ASSERT_VALUES_EQUAL(
                        src.AsStringBuf(),
                        dst.AsStringBuf());
                } else if (msg->Record.GetStartIndex() == 0) {
                    const auto& blocks = msg->Record.GetBlocks().GetBuffers();
                    TBlockDataRef src{buffer2.data(), buffer2.size()};
                    TBlockDataRef dst{blocks[0].data(), DefaultBlockSize};
                    UNIT_ASSERT_VALUES_EQUAL(
                        src.AsStringBuf(),
                        dst.AsStringBuf());
                } else {
                    UNIT_ASSERT(false);
                }
            }
            return false;
        };

        runtime.SetEventFilter(checkWriteDeviceRequest);

        auto writeRequest =
            std::make_unique<TEvService::TEvWriteBlocksRequest>();
        writeRequest->Record.SetStartIndex(blockRange.Start);
        writeRequest->Record.MutableBlocks()->AddBuffers(buffer1);
        writeRequest->Record.MutableBlocks()->AddBuffers(buffer2);
        client.SendRequest(client.GetActorId(), std::move(writeRequest));
        auto response = client.RecvWriteBlocksResponse();
        UNIT_ASSERT(SUCCEEDED(response->GetStatus()));
        UNIT_ASSERT_VALUES_EQUAL(3, writeDeviceRequestCount);
    }

    Y_UNIT_TEST(ShouldMirrorRequestsDuringMigration)
    {
        DoShouldMirrorRequestsDuringMigration(false);
    }

    Y_UNIT_TEST(ShouldMirrorRequestsDuringMigrationDirectCopy)
    {
        DoShouldMirrorRequestsDuringMigration(true);
    }

    void DoShouldUpdateMigrationState(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;
        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            nullptr,   // no migration state
            storageConfigPatch);

        TPartitionClient client(runtime, env.ActorId);

        ui32 migrationUpdates = 0;
        ui64 lastMigrationIndex = 0;
        ui32 lastBlockCountToMigrate = 0;
        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event) -> bool
            {
                Y_UNUSED(runtime);

                if (event->GetTypeRewrite() ==
                    TEvVolume::EvUpdateMigrationState) {
                    auto* msg =
                        event->Get<TEvVolume::TEvUpdateMigrationState>();
                    migrationUpdates++;
                    lastMigrationIndex = msg->MigrationIndex;
                    lastBlockCountToMigrate = msg->BlockCountToMigrate;
                }
                return false;
            });

        // Migrate the first range.
        {
            WaitForMigrations(runtime, 1);
            if (migrationUpdates != 1) {
                NActors::TDispatchOptions options;
                options.CustomFinalCondition = [&]()
                {
                    return migrationUpdates == 1;
                };
                runtime.DispatchEvents(options);
            }
        }

        // We should see "lastMigrationIndex" equal to 3072, because the first
        // device has 2048 blocks and is not migrating.
        UNIT_ASSERT_VALUES_EQUAL(1, migrationUpdates);
        UNIT_ASSERT_VALUES_EQUAL(3072, lastMigrationIndex);
        UNIT_ASSERT_VALUES_EQUAL(2048, lastBlockCountToMigrate);

        {
            NActors::TDispatchOptions options;
            options.CustomFinalCondition = [&]()
            {
                return migrationUpdates == 2;
            };
            runtime.DispatchEvents(options);
        }

        // The migration of the second range increases the index by 1024 blocks.
        UNIT_ASSERT_VALUES_EQUAL(2, migrationUpdates);
        UNIT_ASSERT_VALUES_EQUAL(4096, lastMigrationIndex);
        UNIT_ASSERT_VALUES_EQUAL(1024, lastBlockCountToMigrate);
    }

    Y_UNIT_TEST(ShouldUpdateMigrationState)
    {
        DoShouldUpdateMigrationState(false);
    }

    Y_UNIT_TEST(ShouldUpdateMigrationStateDirectCopy)
    {
        DoShouldUpdateMigrationState(true);
    }

    void DoShouldNotFinishMigrationWhenMigrationIsDisabled(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;
        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        storageConfigPatch.SetNonReplicatedVolumeMigrationDisabled(true);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            nullptr,   // no migration state
            storageConfigPatch);

        TPartitionClient client(runtime, env.ActorId);

        runtime.SetEventFilter(
            [&](TTestActorRuntimeBase&, TAutoPtr<IEventHandle>& event) -> bool
            {
                switch (event->GetTypeRewrite()) {
                    case TEvNonreplPartitionPrivate::EvRangeMigrated: {
                        UNIT_ASSERT_C(
                            false,
                            "EvRangeMigrated should not be received");
                    }
                    case TEvDiskRegistry::EvFinishMigrationRequest: {
                        UNIT_ASSERT_C(
                            false,
                            "EvFinishMigrationRequest should not be received");
                    }
                }

                return false;
            });

        runtime.DispatchEvents({}, TDuration::Seconds(1));
    }

    Y_UNIT_TEST(ShouldNotFinishMigrationWhenMigrationIsDisabled)
    {
        DoShouldNotFinishMigrationWhenMigrationIsDisabled(false);
    }

    Y_UNIT_TEST(ShouldNotFinishMigrationWhenMigrationIsDisabledDirectCopy)
    {
        DoShouldNotFinishMigrationWhenMigrationIsDisabled(true);
    }

    void DoShouldDoMigrationViaRdma(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            true,
            nullptr,   // no migration state,
            storageConfigPatch);

        env.Rdma().InitAllEndpoints();

        // petya should be migrated => 3 ranges
        WaitForMigrations(runtime, 3);
    }

    Y_UNIT_TEST(ShouldDoMigrationViaRdma)
    {
        DoShouldDoMigrationViaRdma(false);
    }

    Y_UNIT_TEST(ShouldDoMigrationViaRdmaDirectCopy)
    {
        DoShouldDoMigrationViaRdma(true);
    }

    void DoShouldDoMigrationEvenInReadOnlyMode(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_ERROR_READ_ONLY,
            false,
            nullptr,   // no migration state
            storageConfigPatch);

        // petya should be migrated => 3 ranges
        WaitForMigrations(runtime, 3);
    }

    Y_UNIT_TEST(ShouldDoMigrationEvenInReadOnlyMode)
    {
        DoShouldDoMigrationEvenInReadOnlyMode(false);
    }

    Y_UNIT_TEST(ShouldDoMigrationEvenInReadOnlyModeDirectCopy)
    {
        DoShouldDoMigrationEvenInReadOnlyMode(true);
    }

    void DoShouldReportSimpleCounters(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            nullptr,   // no migration state
            storageConfigPatch);

        TPartitionClient client(runtime, env.ActorId);

        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.Simple;
        UNIT_ASSERT_VALUES_EQUAL(
            5 * ProcessingBlockCount * DefaultBlockSize,
            counters.BytesCount.Value);
    }

    Y_UNIT_TEST(ShouldReportSimpleCounters)
    {
        DoShouldReportSimpleCounters(false);
    }

    Y_UNIT_TEST(ShouldReportSimpleCountersDirectCopy)
    {
        DoShouldReportSimpleCounters(true);
    }

    void DoShouldDelayMigration(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);

        WaitForNoMigrations(runtime, TDuration::Seconds(5));

        migrationState->IsMigrationAllowed = true;
        WaitForMigrations(runtime, 3);
    }

    Y_UNIT_TEST(ShouldDelayMigration)
    {
        DoShouldDelayMigration(false);
    }

    Y_UNIT_TEST(ShouldDelayMigrationDirectCopy)
    {
        DoShouldDelayMigration(true);
    }

    void DoShouldRegisterTrafficSource(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);

        WaitForNoMigrations(runtime, TDuration::Seconds(5));

        ui32 registerSourceCounter = 0;
        auto countRegisterTrafficSourceRequests =
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);

            if (event->GetTypeRewrite() ==
                TEvStatsServicePrivate::EvRegisterTrafficSourceRequest)
            {
                auto* msg = event->Get<
                    TEvStatsServicePrivate::TEvRegisterTrafficSourceRequest>();
                ++registerSourceCounter;
                UNIT_ASSERT_VALUES_EQUAL("test", msg->SourceId);
                UNIT_ASSERT_VALUES_EQUAL(500, msg->BandwidthMiBs);
            }
            return false;
        };
        runtime.SetEventFilter(countRegisterTrafficSourceRequests);

        migrationState->IsMigrationAllowed = true;
        WaitForMigrations(runtime, 1);

        // Expect that the registration of the background bandwidth source has
        // occurred.
        UNIT_ASSERT_VALUES_EQUAL(1, registerSourceCounter);

        // The background bandwidth source should be re-registered at intervals
        // of once per second.
        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));
        UNIT_ASSERT_VALUES_EQUAL(2, registerSourceCounter);
    }

    Y_UNIT_TEST(ShouldRegisterTrafficSource)
    {
        DoShouldRegisterTrafficSource(false);
    }

    Y_UNIT_TEST(ShouldRegisterTrafficSourceDirectCopy)
    {
        DoShouldRegisterTrafficSource(true);
    }

    void DoShouldNotFailRequestOnFollowerNonRetriableError(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);

        // Find the ActorIDs of the leader and follower partitions.
        TActorId leaderPartition;
        TActorId followerPartition;
        auto findLeaderAndFollower =
            MakeLeaderFollowerFilter(&leaderPartition, &followerPartition);
        runtime.SetEventFilter(findLeaderAndFollower);

        migrationState->IsMigrationAllowed = true;
        WaitForMigrations(runtime, 3);
        UNIT_ASSERT(leaderPartition);
        UNIT_ASSERT(followerPartition);

        // Now we will fail requests to the follower partition with a
        // non-retriable error. Expect that the client's requests will be
        // executed successfully, since the leader partition responds S_OK, but
        // the migration will be stopped due to errors of follower partition.
        size_t failedPartitionRequestCount = 0;
        bool failLeaderRequest = false;

        TAutoPtr<IEventHandle> leaderWriteRequest;
        TAutoPtr<IEventHandle> followerWriteRequest;
        TAutoPtr<IEventHandle> followerZeroRequest;

        auto failFollowerRequests = [&](TTestActorRuntimeBase& runtime,
                                        TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);

            if (event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest) {
                if (failLeaderRequest && event->Recipient == leaderPartition) {
                    leaderWriteRequest = event;
                    ++failedPartitionRequestCount;
                    return true;
                }

                if (event->Recipient == followerPartition) {
                    followerWriteRequest = event;
                    ++failedPartitionRequestCount;
                    return true;
                }
            }

            if (event->GetTypeRewrite() == TEvService::EvZeroBlocksRequest) {
                if (event->Recipient == followerPartition) {
                    followerZeroRequest = event;
                    ++failedPartitionRequestCount;
                    return true;
                }
            }

            return false;
        };
        runtime.SetEventFilter(failFollowerRequests);

        auto replyToLeaderWrite = [&] () {
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            UNIT_ASSERT(leaderWriteRequest);

            runtime.Send(new IEventHandle(
                leaderWriteRequest->Sender,
                leaderWriteRequest->Recipient,
                new TEvService::TEvWriteBlocksResponse(MakeError(E_REJECTED)),
                0,
                leaderWriteRequest->Cookie,
                nullptr));

            leaderWriteRequest.Reset();
        };

        auto replyToFollowerWrite = [&] () {
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            UNIT_ASSERT(followerWriteRequest);

            runtime.Send(new IEventHandle(
                followerWriteRequest->Sender,
                followerWriteRequest->Recipient,
                new TEvService::TEvWriteBlocksResponse(MakeError(E_ARGUMENT)),
                0,
                followerWriteRequest->Cookie,
                nullptr));

            followerWriteRequest.Reset();
        };

        auto replyToFollowerZero = [&] () {
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            UNIT_ASSERT(followerZeroRequest);

            runtime.Send(new IEventHandle(
                followerZeroRequest->Sender,
                followerZeroRequest->Recipient,
                new TEvService::TEvZeroBlocksResponse(MakeError(E_IO)),
                0,
                followerZeroRequest->Cookie,
                nullptr));

            followerZeroRequest.Reset();
        };

        TPartitionClient client(runtime, env.ActorId);
        {
            // Executed successfully
            client.SendZeroBlocksRequest(TBlockRange64::MakeOneBlock(0));
            replyToFollowerZero();
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(1, failedPartitionRequestCount);
            failedPartitionRequestCount = 0;
        }
        {
            // Executed successfully
            client.SendWriteBlocksRequest(TBlockRange64::MakeOneBlock(0), 'A');
            replyToFollowerWrite();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(1, failedPartitionRequestCount);
            failedPartitionRequestCount = 0;
        }
        {
            // Fail with error from leader partition
            failLeaderRequest = true;
            client.SendWriteBlocksRequest(TBlockRange64::MakeOneBlock(0), 'A');
            replyToLeaderWrite();
            replyToFollowerWrite();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(2, failedPartitionRequestCount);
            failedPartitionRequestCount = 0;
        }
    }

    Y_UNIT_TEST(ShouldNotFailRequestOnFollowerNonRetriableError)
    {
        DoShouldNotFailRequestOnFollowerNonRetriableError(false);
    }

    Y_UNIT_TEST(ShouldNotFailRequestOnFollowerNonRetriableErrorDirectCopy)
    {
        DoShouldNotFailRequestOnFollowerNonRetriableError(true);
    }

    void DoShouldFailRequestOnFollowerRetriableError(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);

        // Find the ActorIDs of the leader and follower partitions.
        TActorId leaderPartition;
        TActorId followerPartition;
        auto findLeaderAndFollower =
            MakeLeaderFollowerFilter(&leaderPartition, &followerPartition);
        runtime.SetEventFilter(findLeaderAndFollower);

        migrationState->IsMigrationAllowed = true;
        WaitForMigrations(runtime, 3);
        UNIT_ASSERT(leaderPartition);
        UNIT_ASSERT(followerPartition);

        // Now we will fail requests to the follower partition with an
        // retriable error. We expect that client requests will fail, since we
        // do not want to stop the migration.
        size_t failedPartitionRequestCount = 0;
        TAutoPtr<IEventHandle> writeRequest;
        TAutoPtr<IEventHandle> zeroRequest;
        auto failFollowerRequests = [&](TTestActorRuntimeBase& runtime,
                                        TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);

            if (event->GetTypeRewrite() == TEvService::EvWriteBlocksRequest) {
                if (event->Recipient == followerPartition) {
                    writeRequest = event;
                    ++failedPartitionRequestCount;
                    return true;
                }
            }

            if (event->GetTypeRewrite() == TEvService::EvZeroBlocksRequest) {
                if (event->Recipient == followerPartition) {
                    zeroRequest = event;
                    ++failedPartitionRequestCount;
                    return true;
                }
            }

            return false;
        };
        runtime.SetEventFilter(failFollowerRequests);

        auto replyToWrite = [&] () {
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            UNIT_ASSERT(writeRequest);

            runtime.Send(new IEventHandle(
                writeRequest->Sender,
                writeRequest->Recipient,
                new TEvService::TEvWriteBlocksResponse(MakeError(E_REJECTED)),
                0,
                writeRequest->Cookie,
                nullptr));

            writeRequest.Reset();
        };

        auto replyToZero = [&] () {
            runtime.DispatchEvents({}, TDuration::MilliSeconds(100));
            UNIT_ASSERT(zeroRequest);

            runtime.Send(new IEventHandle(
                zeroRequest->Sender,
                zeroRequest->Recipient,
                new TEvService::TEvZeroBlocksResponse(MakeError(E_TIMEOUT)),
                0,
                zeroRequest->Cookie,
                nullptr));

            zeroRequest.Reset();
        };

        TPartitionClient client(runtime, env.ActorId);
        {
            client.SendZeroBlocksRequest(TBlockRange64::MakeOneBlock(0));
            replyToZero();
            auto response = client.RecvZeroBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_TIMEOUT,
                response->GetStatus(),
                response->GetErrorReason());
        }
        {
            client.SendWriteBlocksRequest(TBlockRange64::MakeOneBlock(0), 'A');
            replyToWrite();
            auto response = client.RecvWriteBlocksResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        UNIT_ASSERT_VALUES_EQUAL(2, failedPartitionRequestCount);
    }

    Y_UNIT_TEST(ShouldFailRequestOnFollowerRetriableError)
    {
        DoShouldFailRequestOnFollowerRetriableError(false);
    }

    Y_UNIT_TEST(ShouldFailRequestOnFollowerRetriableErrorDirectCopy)
    {
        DoShouldFailRequestOnFollowerRetriableError(true);
    }

    Y_UNIT_TEST(ShouldHandleGetDeviceForRangeRequest)
    {
        using TEvGetDeviceForRangeRequest =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest;
        using TEvGetDeviceForRangeResponse =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse;
        using EPurpose = TEvGetDeviceForRangeRequest::EPurpose;

        TTestBasicRuntime runtime;

        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(true);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);
        TPartitionClient client(runtime, env.ActorId);

        migrationState->IsMigrationAllowed = true;

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

            // Request with write should fail
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(2040, 8)));
            response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        WaitForMigrations(runtime, 3);

        {
            // Request to second device with read should success
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(2048, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL("petya", response->Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(0, 8),
                response->DeviceBlockRange);

            // with write should fail.
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(2048, 8)));
            response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                response->GetStatus(),
                response->GetErrorReason());
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
    }

    Y_UNIT_TEST(ShouldFailOnDirectCopyRequestTimeout)
    {
        TTestBasicRuntime runtime;

        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(true);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);

        // We will steal a EvDirectCopyBlocksResponse this will cause the
        // TEvDirectCopyBlocksRequest to hang and the range migration timeout.
        bool gotTimeout = false;
        bool directCopyBlocksResponseStolen = false;

        auto stoleTargetWriteRequest =
            [&](TTestActorRuntimeBase& runtime,
                TAutoPtr<IEventHandle>& event) -> bool
        {
            Y_UNUSED(runtime);

            if (event->GetTypeRewrite() ==
                TEvDiskAgent::EvDirectCopyBlocksResponse)
            {
                directCopyBlocksResponseStolen = true;
                return true;
            }

            if (event->GetTypeRewrite() ==
                TEvNonreplPartitionPrivate::EvRangeMigrated)
            {
                auto* msg =
                    event->Get<TEvNonreplPartitionPrivate::TEvRangeMigrated>();
                gotTimeout = msg->GetError().GetCode() == E_TIMEOUT;
            }
            return false;
        };
        runtime.SetEventFilter(stoleTargetWriteRequest);

        migrationState->IsMigrationAllowed = true;

        NActors::TDispatchOptions options;
        options.CustomFinalCondition = [&]()
        {
            return directCopyBlocksResponseStolen;
        };
        runtime.DispatchEvents(options);
        UNIT_ASSERT_VALUES_EQUAL(true, directCopyBlocksResponseStolen);

        options.CustomFinalCondition = [&]()
        {
            return gotTimeout;
        };
        runtime.AdvanceCurrentTime(TDuration::Seconds(10));
        runtime.DispatchEvents(options, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(true, gotTimeout);
    }

    Y_UNIT_TEST(ShouldFallbackFromDirectCopy)
    {
        using TEvGetDeviceForRangeResponse =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse;

        const size_t migratedRangeCount = 3;

        TTestBasicRuntime runtime;

        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(true);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);
        TPartitionClient client(runtime, env.ActorId);

        // Abort TEvGetDeviceForRangeRequest requests. In this case, the
        // migrator will fallback to normal reading and writing.
        auto abortGetDeviceForRange = [&](TTestActorRuntimeBase& runtime,
                                          TAutoPtr<IEventHandle>& event) -> bool
        {
            if (event->GetTypeRewrite() ==
                TEvNonreplPartitionPrivate::EvGetDeviceForRangeRequest)
            {
                runtime.Schedule(
                    new IEventHandle(
                        event->Sender,
                        event->Recipient,
                        new TEvGetDeviceForRangeResponse(MakeError(E_ABORTED)),
                        0,
                        event->Cookie,
                        nullptr),
                    TDuration::MilliSeconds(1));

                return true;
            }
            return false;
        };
        runtime.SetEventFilter(abortGetDeviceForRange);

        migrationState->IsMigrationAllowed = true;
        WaitForMigrations(runtime, migratedRangeCount);

        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));
        runtime.AdvanceCurrentTime(UpdateCountersInterval);
        runtime.DispatchEvents({}, TDuration::Seconds(1));

        auto& counters = env.StorageStatsServiceState->Counters.RequestCounters;
        UNIT_ASSERT_VALUES_EQUAL(
            migratedRangeCount,
            counters.WriteBlocks.Count);
        UNIT_ASSERT_VALUES_EQUAL(
            (migratedRangeCount * ProcessingBlockCount) * DefaultBlockSize,
            counters.WriteBlocks.RequestBytes);
    }

    Y_UNIT_TEST(ShouldUseRecommendedBandwidth)
    {
        using TEvGetDeviceForRangeRequest =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeRequest;
        using TEvGetDeviceForRangeResponse =
            TEvNonreplPartitionPrivate::TEvGetDeviceForRangeResponse;
        using EPurpose = TEvGetDeviceForRangeRequest::EPurpose;

        TTestBasicRuntime runtime;

        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(true);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);
        TPartitionClient client(runtime, env.ActorId);

        migrationState->IsMigrationAllowed = true;

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

        WaitForMigrations(runtime, 3);

        {
            // Request to second device (migration target) with read
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForReading,
                    TBlockRange64::WithLength(2048, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_C(
                SUCCEEDED(response->GetStatus()),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL("petya", response->Device.GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                TBlockRange64::WithLength(0, 8),
                response->DeviceBlockRange);

            // Request to second device (migration target) with write
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(2048, 8)));
            response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ABORTED,
                response->GetStatus(),
                response->GetErrorReason());
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
    }

    void ShouldCopyRangeWithCorrectVolumeRequestId(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(useDirectCopy);
        storageConfigPatch.SetAssignIdToWriteAndZeroRequestsEnabled(true);
        storageConfigPatch.SetRejectLateRequestsAtDiskAgentEnabled(true);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            nullptr,   // no migration state
            storageConfigPatch);

        size_t volumeRequestId = 12345;

        for (size_t i = 0; i < 3; ++i) {
            TMigrationMessageHandler handler;
            std::unique_ptr<IEventHandle> stollenTakeVolumeRequestIdEvent;
            runtime.SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (useDirectCopy) {
                        handler
                            .Handle<TEvDiskAgent::TEvDirectCopyBlocksRequest>(
                                event);
                    } else {
                        handler.Handle<TEvService::TEvWriteBlocksRequest>(
                            event);
                        handler.Handle<TEvService::TEvZeroBlocksRequest>(event);
                    }

                    switch (event->GetTypeRewrite()) {
                        case TEvVolumePrivate::EvTakeVolumeRequestIdRequest:
                            stollenTakeVolumeRequestIdEvent.reset(
                                event.Release());
                            return TTestActorRuntimeBase::EEventAction::DROP;

                        default:
                            break;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });

            TPartitionClient client(runtime, env.ActorId);

            runtime.AdvanceCurrentTime(5s);
            runtime.DispatchEvents({}, 10ms);

            UNIT_ASSERT_VALUES_EQUAL(0, handler.MigrationRequestCount);
            UNIT_ASSERT(stollenTakeVolumeRequestIdEvent);
            UNIT_ASSERT_VALUES_EQUAL(
                env.VolumeActorId,
                stollenTakeVolumeRequestIdEvent->Recipient);

            runtime.Send(
                stollenTakeVolumeRequestIdEvent->Sender,
                env.VolumeActorId,
                std::make_unique<
                    TEvVolumePrivate::TEvTakeVolumeRequestIdResponse>(
                    volumeRequestId)
                    .release());

            NActors::TDispatchOptions options;
            options.FinalEvents = {
                NActors::TDispatchOptions::TFinalEventCondition(
                    TEvNonreplPartitionPrivate::EvRangeMigrated)};

            runtime.DispatchEvents(options);

            UNIT_ASSERT_VALUES_EQUAL(1, handler.MigrationRequestCount);
            UNIT_ASSERT_VALUES_EQUAL(1, handler.VolumeRequestIds.size());
            UNIT_ASSERT_VALUES_EQUAL(
                volumeRequestId,
                handler.VolumeRequestIds[0]);
            volumeRequestId += 12345;
        }
    }

    Y_UNIT_TEST(ShouldCopyRangeWithCorrectVolumeRequestId)
    {
        ShouldCopyRangeWithCorrectVolumeRequestId(false);
    }

    Y_UNIT_TEST(ShouldCopyRangeWithCorrectVolumeRequestIdDirectCopy)
    {
        ShouldCopyRangeWithCorrectVolumeRequestId(true);
    }

    Y_UNIT_TEST(ShouldRetryIfRangeMigrationFail)
    {
        TTestBasicRuntime runtime;
        auto migrationState = std::make_shared<TMigrationState>();
        migrationState->IsMigrationAllowed = false;

        NProto::TStorageServiceConfig storageConfigPatch;
        storageConfigPatch.SetUseDirectCopyRange(true);
        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            storageConfigPatch);

        bool isRejected = false;
        bool seenRetry = false;
        TBlockRange64 rejectedRange;

        auto filter = [&](TTestActorRuntimeBase& runtime,
                          TAutoPtr<IEventHandle>& event) -> bool
        {
            if (event->GetTypeRewrite() ==
                TEvNonreplPartitionPrivate::EvRangeMigrated)
            {
                auto* msg =
                    event->Get<TEvNonreplPartitionPrivate::TEvRangeMigrated>();

                if (isRejected && msg->Range == rejectedRange) {
                    seenRetry = true;
                    return false;
                }

                if (isRejected) {
                    return false;
                }

                isRejected = true;
                rejectedRange = msg->Range;
                runtime.Send(
                    event->Recipient,
                    event->Sender,
                    new TEvNonreplPartitionPrivate::TEvRangeMigrated(
                        MakeError(E_REJECTED),
                        *msg));

                return true;
            }

            return false;
        };

        runtime.SetEventFilter(filter);

        migrationState->IsMigrationAllowed = true;
        runtime.AdvanceCurrentTime(TDuration::Seconds(10));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT(isRejected);

        runtime.AdvanceCurrentTime(TDuration::Seconds(1));
        runtime.DispatchEvents({}, TDuration::MilliSeconds(10));

        UNIT_ASSERT(seenRetry);
    }
}

}   // namespace NCloud::NBlockStore::NStorage
