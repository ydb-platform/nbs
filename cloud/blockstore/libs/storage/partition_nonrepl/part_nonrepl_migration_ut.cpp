#include "part_nonrepl_migration_actor.h"
#include "ut_env.h"

#include <cloud/blockstore/libs/diagnostics/block_digest.h>
#include <cloud/blockstore/libs/diagnostics/profile_log.h>
#include <cloud/blockstore/libs/rdma_test/client_test.h>
#include <cloud/blockstore/libs/storage/api/disk_agent.h>
#include <cloud/blockstore/libs/storage/api/volume.h>
#include <cloud/blockstore/libs/storage/core/config.h>
#include <cloud/blockstore/libs/storage/disk_agent/actors/direct_copy_actor.h>
#include <cloud/blockstore/libs/storage/protos/disk.pb.h>
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

// The block count migrated at a time.
constexpr ui64 ProcessingBlockCount = ProcessingRangeSize / DefaultBlockSize;

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
            bool useDirectCopy)
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
                std::move(record));
        };

        NProto::TStorageServiceConfig storageConfig;
        storageConfig.SetMaxTimedOutDeviceStateDuration(20'000);
        storageConfig.SetNonReplicatedMinRequestTimeoutSSD(1'000);
        storageConfig.SetNonReplicatedMaxRequestTimeoutSSD(5'000);
        storageConfig.SetMaxMigrationBandwidth(500);
        storageConfig.SetUseDirectCopyRange(useDirectCopy);

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

        auto partConfig = std::make_shared<TNonreplicatedPartitionConfig>(
            ToLogicalBlocks(devices, DefaultBlockSize),
            ioMode,
            "test",
            DefaultBlockSize,
            TNonreplicatedPartitionConfig::TVolumeInfo{
                Now(),
                // only SSD/HDD distinction matters
                NProto::STORAGE_MEDIA_SSD_NONREPLICATED},
            VolumeActorId,
            false, // muteIOErrors
            THashSet<TString>(), // freshDeviceIds
            TDuration::Zero(), // maxTimedOutDeviceStateDuration
            false, // maxTimedOutDeviceStateDurationOverridden
            false
        );

        auto part = std::make_unique<TNonreplicatedPartitionMigrationActor>(
            std::move(config),
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNonreplicatedPartitionMigrationTest)
{
    void DoShouldMirrorRequestsAfterAllDataIsMigrated(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            nullptr,   // no migration state
            useDirectCopy);

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
            migrationCopyBlocks * ProcessingRangeSize,
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

    Y_UNIT_TEST(ShouldMirrorRequestsAfterAllDataIsMigrated) {
        DoShouldMirrorRequestsAfterAllDataIsMigrated(false);
    }

    Y_UNIT_TEST(ShouldMirrorRequestsAfterAllDataIsMigratedDirectCopy) {
        DoShouldMirrorRequestsAfterAllDataIsMigrated(true);
    }

    void DoShouldDoMigrationViaRdma(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            true,
            nullptr,   // no migration state,
            useDirectCopy);

        env.Rdma().InitAllEndpoints();

        // petya should be migrated => 3 ranges
        WaitForMigrations(runtime, 3);
    }

    Y_UNIT_TEST(DoShouldDoMigrationViaRdma) {
        DoShouldDoMigrationViaRdma(false);
    }

    Y_UNIT_TEST(DoShouldDoMigrationViaRdmaDirectCopy) {
        DoShouldDoMigrationViaRdma(true);
    }

    void DoShouldDoMigrationEvenInReadOnlyMode(bool useDirectCopy)
    {
        TTestBasicRuntime runtime;

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_ERROR_READ_ONLY,
            false,
            nullptr,   // no migration state
            useDirectCopy);

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

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            nullptr,   // no migration state
            useDirectCopy);

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

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            useDirectCopy);

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

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            useDirectCopy);

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

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            useDirectCopy);

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

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            useDirectCopy);

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

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            true);
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
            // Request to second device
            client.SendRequest(
                env.ActorId,
                std::make_unique<TEvGetDeviceForRangeRequest>(
                    EPurpose::ForWriting,
                    TBlockRange64::WithLength(2048, 8)));
            auto response = client.RecvResponse<TEvGetDeviceForRangeResponse>();
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

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            true);

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

        TTestEnv env(
            runtime,
            TTestEnv::DefaultDevices(runtime.GetNodeId(0)),
            TTestEnv::DefaultMigrations(runtime.GetNodeId(0)),
            NProto::VOLUME_IO_OK,
            false,
            migrationState,
            true);
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
}

}   // namespace NCloud::NBlockStore::NStorage
