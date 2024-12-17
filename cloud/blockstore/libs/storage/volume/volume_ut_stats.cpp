#include "volume_ut.h"

#include <cloud/blockstore/libs/storage/disk_agent/actors/direct_copy_actor.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NTestVolume;

using namespace NTestVolumeHelpers;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeStatsTest)
{
    Y_UNIT_TEST(ShouldSendPartitionStatsForNonreplicatedVolume)
    {
        auto state = MakeIntrusive<TDiskRegistryState>();
        state->MigrationMode = EMigrationMode::InProgress;
        NProto::TStorageServiceConfig config;
        // XXX
        // disabling migration index caching - migration preempts metrics-related
        // code in test actor runtime
        config.SetMigrationIndexCachingInterval(999999);
        config.SetMaxMigrationBandwidth(999'999'999);
        auto runtime = PrepareTestActorRuntime(config, state);

        const auto expectedBlockCount =
            DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;
        const auto expectedDeviceCount = 3;

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            expectedDeviceCount * expectedBlockCount,
            "vol0");

        volume.WaitReady();

        ui64 bytesCount = 0;
        ui32 partStatsSaved = 0;

        auto obs = [&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvVolumePrivate::EvPartStatsSaved) {
                ++partStatsSaved;
            } else if (event->Recipient == MakeStorageStatsServiceId()
                    && event->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters)
            {
                auto* msg = event->Get<TEvStatsService::TEvVolumePartCounters>();

                bytesCount = msg->DiskCounters->Simple.BytesCount.Value;

                UNIT_ASSERT_VALUES_EQUAL(
                    msg->DiskCounters->RequestCounters.ReadBlocks
                        .GetRequestBytes(),
                    msg->DiskCounters->Interconnect.ReadBytes.Value);
                UNIT_ASSERT_VALUES_EQUAL(
                    msg->DiskCounters->RequestCounters.WriteBlocks
                        .GetRequestBytes(),
                    msg->DiskCounters->Interconnect.WriteBytes.Value);
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        runtime->SetObserverFunc(obs);

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            1);
        volume.WriteBlocks(
            TBlockRange64::WithLength(1024, 512),
            clientInfo.GetClientId(),
            2);

        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT(partStatsSaved >= 2);
        UNIT_ASSERT_VALUES_EQUAL(
            expectedDeviceCount * expectedBlockCount * DefaultBlockSize,
            bytesCount);

        volume.RebootTablet();
        bytesCount = 0;

        runtime->SetObserverFunc(obs);
        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvUpdateCounters>()
        );
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(
            expectedDeviceCount * expectedBlockCount * DefaultBlockSize,
            bytesCount);
    }

    Y_UNIT_TEST(ShouldSendCachedValuesWhenPartitionIsOffline)
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
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            1024,       // block count per partition
            "vol0",
            "cloud",
            "folder",
            1,          // partition count
            2
        );

        CheckVolumeSendsStatsEvenIfPartitionsAreDead(
            std::move(runtime),
            volume,
            4_MB,
            true);
    }

    Y_UNIT_TEST(BytesCountShouldNeverBeZero)
    {
        NProto::TStorageServiceConfig config;
        config.SetMinChannelCount(4);
        auto runtime = PrepareTestActorRuntime(config);

        TVolumeClient volume(*runtime);
        const ui64 blockCount = 1024;
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            blockCount,
            "vol0",
            "cloud",
            "folder",
            1,          // partition count
            2
        );

        volume.WaitReady();

        ui64 expectedBytesCount = blockCount * DefaultBlockSize;
        ui64 bytesCount = 0;
        ui64 vbytesCount = 0;

        auto obs = [&] (TAutoPtr<IEventHandle>& event) {
            if (event->Recipient == MakeStorageStatsServiceId()
                    && event->GetTypeRewrite()
                        == TEvStatsService::EvVolumePartCounters)
            {
                auto* msg = event->Get<TEvStatsService::TEvVolumePartCounters>();

                bytesCount = msg->DiskCounters->Simple.BytesCount.Value;
            }

            if (event->Recipient == MakeStorageStatsServiceId()
                    && event->GetTypeRewrite()
                        == TEvStatsService::EvVolumeSelfCounters)
            {
                auto* msg = event->Get<TEvStatsService::TEvVolumeSelfCounters>();

                vbytesCount = msg->VolumeSelfCounters->Simple.VBytesCount.Value;
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

        UNIT_ASSERT_VALUES_EQUAL(expectedBytesCount, bytesCount);
        UNIT_ASSERT_VALUES_EQUAL(expectedBytesCount, vbytesCount);
    }

    Y_UNIT_TEST(ShouldSendCachedValuesWhenNonReplPartitionIsOffline)
    {
        NProto::TStorageServiceConfig config;
        config.SetMinChannelCount(4);
        // XXX
        // disabling migration index caching - migration preempts metrics-related
        // code in test actor runtime
        config.SetMigrationIndexCachingInterval(999999);

        auto state = MakeIntrusive<TDiskRegistryState>();
        state->MigrationMode = EMigrationMode::InProgress;
        auto runtime = PrepareTestActorRuntime(config, state);

        const auto expectedBlockCount =
            DefaultDeviceBlockSize * DefaultDeviceBlockCount / DefaultBlockSize;
        const auto expectedDeviceCount = 3;

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            expectedDeviceCount * expectedBlockCount,
            "vol0");

        CheckVolumeSendsStatsEvenIfPartitionsAreDead(
            std::move(runtime),
            volume,
            expectedBlockCount * DefaultBlockSize * expectedDeviceCount,
            false);
    }

    Y_UNIT_TEST(ShouldAggregateAndCachePartitionStats)
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
            NCloud::NProto::STORAGE_MEDIA_HYBRID,
            1024,       // block count per partition
            "vol0",
            "cloud",
            "folder",
            2,          // partition count
            2
        );

        volume.WaitReady();

        ui64 bytesCount = 0;
        ui64 usedBytesCount = 0;
        ui32 partStatsSaved = 0;
        ui64 channelHistorySize = 0;
        ui32 partitionCount = 0;
        ui64 loadTime = 0;
        ui64 startTime = 0;

        auto obs = [&] (TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvVolumePrivate::EvPartStatsSaved) {
                ++partStatsSaved;
            } else if (event->Recipient == MakeStorageStatsServiceId()
                    && event->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters)
            {
                auto* msg = event->Get<TEvStatsService::TEvVolumePartCounters>();

                bytesCount = msg->DiskCounters->Simple.BytesCount.Value;
                usedBytesCount = msg->DiskCounters->Simple.UsedBytesCount.Value;
                channelHistorySize = msg->DiskCounters->Simple.ChannelHistorySize.Value;
            } else if (event->Recipient == MakeStorageStatsServiceId()
                    && event->GetTypeRewrite() == TEvStatsService::EvVolumeSelfCounters)
            {
                auto* msg = event->Get<TEvStatsService::TEvVolumeSelfCounters>();

                partitionCount =
                    msg->VolumeSelfCounters->Simple.PartitionCount.Value;
                loadTime = msg->VolumeSelfCounters->Simple.LastVolumeLoadTime.Value;
                startTime = msg->VolumeSelfCounters->Simple.LastVolumeStartTime.Value;
            }


            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        runtime->SetObserverFunc(obs);

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        volume.WriteBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId(),
            1);
        volume.WriteBlocks(
            TBlockRange64::WithLength(1024, 512),
            clientInfo.GetClientId(),
            2);

        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT(partStatsSaved >= 2);
        UNIT_ASSERT_VALUES_EQUAL(8_MB, bytesCount);
        UNIT_ASSERT_VALUES_EQUAL(6_MB, usedBytesCount);
        UNIT_ASSERT_VALUES_UNEQUAL(0, channelHistorySize);
        UNIT_ASSERT_VALUES_UNEQUAL(0, loadTime);
        UNIT_ASSERT_VALUES_UNEQUAL(0, startTime);
        UNIT_ASSERT_VALUES_EQUAL(2, partitionCount);

        volume.RebootTablet();
        bytesCount = 0;
        usedBytesCount = 0;
        channelHistorySize = 0;
        loadTime = 0;
        startTime = 0;
        partitionCount = 0;

        runtime->SetObserverFunc(obs);
        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvUpdateCounters>()
        );
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(8_MB, bytesCount);
        UNIT_ASSERT_VALUES_EQUAL(6_MB, usedBytesCount);
        UNIT_ASSERT_VALUES_UNEQUAL(0, channelHistorySize);
        UNIT_ASSERT_VALUES_UNEQUAL(0, loadTime);
        UNIT_ASSERT_VALUES_EQUAL(0, startTime);
        UNIT_ASSERT_VALUES_EQUAL(2, partitionCount);

        // partition stats should be sent not just once
        bytesCount = 0;
        usedBytesCount = 0;
        channelHistorySize = 0;
        loadTime = 0;
        startTime = 0;
        partitionCount = 0;

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvUpdateCounters>()
        );
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(8_MB, bytesCount);
        UNIT_ASSERT_VALUES_EQUAL(6_MB, usedBytesCount);
        UNIT_ASSERT_VALUES_UNEQUAL(0, channelHistorySize);
        UNIT_ASSERT_VALUES_UNEQUAL(0, loadTime);
        UNIT_ASSERT_VALUES_EQUAL(0, startTime);
        UNIT_ASSERT_VALUES_EQUAL(2, partitionCount);
    }

    Y_UNIT_TEST(ShouldReportCpuConsumptionAndNetUtilizationForNrdPartitions)
    {
        NProto::TStorageServiceConfig storageServiceConfig;

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        // Enable Schedule for all actors!!!
        runtime->SetRegistrationObserverFunc(
            [](auto& runtime, const auto& parentId, const auto& actorId)
            {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            1024
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        ui64 network = 0;
        TDuration cpu;
        ui64 nonEmptyReports = 0;
        auto observer = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                    TEvVolume::EvDiskRegistryBasedPartitionCounters)
            {
                auto* msg =
                    event->Get<TEvVolume::TEvDiskRegistryBasedPartitionCounters>();
                if (msg->NetworkBytes || msg->CpuUsage) {
                    network = std::max(network, msg->NetworkBytes);
                    cpu = std::max(cpu, msg->CpuUsage);
                    ++nonEmptyReports;
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        runtime->SetObserverFunc(observer);

        volume.ReadBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId());

        // Wait for EvDiskRegistryBasedPartitionCounters arrived.
        runtime->AdvanceCurrentTime(TDuration::Seconds(60));
        runtime->DispatchEvents({ .CustomFinalCondition = [&] {
            return nonEmptyReports == 1;
        }}, TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_UNEQUAL(cpu, TDuration());
        UNIT_ASSERT_VALUES_EQUAL(network, 1024 * 4096);
    }

    Y_UNIT_TEST(ShouldReportCpuConsumptionAndNetUtilizationForMirroredPartitions)
    {
        NProto::TStorageServiceConfig config;
        config.SetAcquireNonReplicatedDevices(true);
        auto state = MakeIntrusive<TDiskRegistryState>();
        auto runtime = PrepareTestActorRuntime(config, state);

        state->ReplicaCount = 2;

        // Enable Schedule for all actors!!!
        runtime->SetRegistrationObserverFunc(
            [](auto& runtime, const auto& parentId, const auto& actorId)
            {
                Y_UNUSED(parentId);
                runtime.EnableScheduleForActor(actorId);
            });

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3,
            1024
        );
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        ui32 nonEmptyReports = 0;
        auto observer = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->GetTypeRewrite() ==
                    TEvVolume::EvDiskRegistryBasedPartitionCounters)
            {
                auto* msg =
                    event->Get<TEvVolume::TEvDiskRegistryBasedPartitionCounters>();
                if (msg->NetworkBytes || msg->CpuUsage) {
                    UNIT_ASSERT_VALUES_UNEQUAL(msg->CpuUsage, TDuration());
                    UNIT_ASSERT_VALUES_EQUAL(msg->NetworkBytes, 1024 * 4096);
                }
            }

            return TTestActorRuntime::DefaultObserverFunc(event);
        };

        runtime->SetObserverFunc(observer);

        volume.ReadBlocks(
            TBlockRange64::WithLength(0, 1024),
            clientInfo.GetClientId());

        runtime->AdvanceCurrentTime(TDuration::Seconds(15));
        runtime->DispatchEvents({ .CustomFinalCondition = [&] {
            return nonEmptyReports == 3;
        }}, TDuration::Seconds(1));
    }

    void DoShouldSendPartitionStatsForShadowDisk(bool useDirectCopy)
    {
        constexpr ui64 DiskBlockCount = 32768;
        constexpr ui64 DiskBlockSize = 4096;
        constexpr ui64 DiskByteCount = DiskBlockCount * DiskBlockSize;
        constexpr ui32 WriteBlockCount = 2;
        constexpr ui32 ReadFromSourceBlockCount = 5;
        constexpr ui32 ReadFromCheckpointBlockCount = 10;

        NProto::TStorageServiceConfig config;
        config.SetUseShadowDisksForNonreplDiskCheckpoints(true);
        config.SetUseDirectCopyRange(useDirectCopy);

        TDiskAgentStatePtr diskAgentState = std::make_shared<TDiskAgentState>();
        diskAgentState->CreateDirectCopyActorFunc =
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

        auto runtime =
            PrepareTestActorRuntime(config, {}, {}, {}, diskAgentState);

        struct TReadAndWriteByteCount
        {
            ui64 ReadByteCount = 0;
            ui64 WriteByteCount = 0;
            ui64 DirectCopyByteCount = 0;
        };
        TMap<TString, TReadAndWriteByteCount> statsForDisks;
        auto statEventInterceptor = [&](TAutoPtr<IEventHandle>& event)
        {
            if (event->Recipient == MakeStorageStatsServiceId() &&
                event->GetTypeRewrite() ==
                    TEvStatsService::EvVolumePartCounters)
            {
                auto* msg =
                    event->Get<TEvStatsService::TEvVolumePartCounters>();
                statsForDisks[msg->DiskId].ReadByteCount +=
                    msg->DiskCounters->RequestCounters.ReadBlocks.RequestBytes;
                statsForDisks[msg->DiskId].WriteByteCount +=
                    msg->DiskCounters->RequestCounters.WriteBlocks.RequestBytes;
                statsForDisks[msg->DiskId].DirectCopyByteCount +=
                    msg->DiskCounters->RequestCounters.CopyBlocks.RequestBytes;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        };
        runtime->SetObserverFunc(statEventInterceptor);

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig(
            0,
            0,
            0,
            0,
            false,
            1,
            NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
            DiskBlockCount,
            "vol0");

        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        // Create checkpoint.
        volume.CreateCheckpoint("c1");

        // Reconnect pipe since partition has restarted.
        volume.ReconnectPipe();
        runtime->DispatchEvents({}, TDuration::MilliSeconds(10));

        // Write to the source disk. These writes will be mirrored to the shadow
        // disk too.
        volume.WriteBlocks(
            TBlockRange64::WithLength(
                DiskBlockCount - WriteBlockCount - 1,
                WriteBlockCount),
            clientInfo.GetClientId(),
            GetBlockContent(2));

        // Read from the source disk.
        volume.ReadBlocks(
            TBlockRange64::WithLength(0, ReadFromSourceBlockCount),
            clientInfo.GetClientId());

        // Wait for checkpoint get ready.
        for (;;) {
            auto status = volume.GetCheckpointStatus("c1");
            if (status->Record.GetCheckpointStatus() ==
                NProto::ECheckpointStatus::READY)
            {
                break;
            }
            runtime->DispatchEvents({}, TDuration::MilliSeconds(10));
        }

        // Read from checkpoint.
        volume.ReadBlocks(
            TBlockRange64::WithLength(0, ReadFromCheckpointBlockCount),
            clientInfo.GetClientId(),
            "c1");

        // Wait for stats send to StorageStatsService.
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));
        runtime->AdvanceCurrentTime(UpdateCountersInterval);
        runtime->DispatchEvents({}, TDuration::Seconds(1));

        // Validate bytes count for source and shadow disks.
        auto shadowActorBytes = useDirectCopy ? 0 :DiskByteCount;
        auto directCopyBytes = useDirectCopy ? DiskByteCount : 0;

        UNIT_ASSERT_VALUES_EQUAL(2, statsForDisks.size());
        UNIT_ASSERT_VALUES_EQUAL(
            shadowActorBytes + ReadFromSourceBlockCount * DiskBlockSize,
            statsForDisks["vol0"].ReadByteCount);
        UNIT_ASSERT_VALUES_EQUAL(
            WriteBlockCount * DiskBlockSize,
            statsForDisks["vol0"].WriteByteCount);

        UNIT_ASSERT_VALUES_EQUAL(
            ReadFromCheckpointBlockCount * DiskBlockSize,
            statsForDisks["vol0-c1"].ReadByteCount);
        UNIT_ASSERT_VALUES_EQUAL(
            shadowActorBytes + WriteBlockCount * DiskBlockSize,
            statsForDisks["vol0-c1"].WriteByteCount);

        UNIT_ASSERT_VALUES_EQUAL(
            directCopyBytes,
            statsForDisks["vol0"].DirectCopyByteCount);
    }

    Y_UNIT_TEST(ShouldSendPartitionStatsForShadowDisk)
    {
        DoShouldSendPartitionStatsForShadowDisk(false);
    }

    Y_UNIT_TEST(ShouldSendPartitionStatsForShadowDiskDirectCopy)
    {
        DoShouldSendPartitionStatsForShadowDisk(true);
    }

    Y_UNIT_TEST(ShouldTryToCutVolumeHistoryAtStartup)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetVolumeHistoryDuration(1000);
        storageServiceConfig.SetVolumeHistoryCleanupItemCount(20);

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig();
        volume.WaitReady();

        auto clientInfo = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_LOCAL,
            0);
        volume.AddClient(clientInfo);

        for (ui32 i = 0; i < 39; ++i) {
            auto clientInfo = CreateVolumeClientInfo(
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                0);
            volume.SendAddClientRequest(clientInfo);
            auto response = volume.RecvAddClientResponse();
            UNIT_ASSERT_C(
                FAILED(response->GetStatus()),
                "Unexpected successful result");
        }

        runtime->AdvanceCurrentTime(TDuration::Minutes(1));
        volume.RebootTablet();
        volume.WaitReady();

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvReadHistoryRequest>(
                runtime->GetCurrentTime(),
                TInstant::Seconds(0),
                Max<size_t>()
            ));
        {
            auto response = volume.RecvResponse<TEvVolumePrivate::TEvReadHistoryResponse>();
            UNIT_ASSERT_VALUES_EQUAL(40, response->History.size());
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvReadHistoryRequest>(
                runtime->GetCurrentTime(),
                TInstant::Seconds(0),
                Max<size_t>()
            ));
        {
            auto response = volume.RecvResponse<TEvVolumePrivate::TEvReadHistoryResponse>();
            UNIT_ASSERT_VALUES_EQUAL(20, response->History.size());
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvReadHistoryRequest>(
                runtime->GetCurrentTime(),
                TInstant::Seconds(0),
                Max<size_t>()
            ));
        {
            auto response = volume.RecvResponse<TEvVolumePrivate::TEvReadHistoryResponse>();
            UNIT_ASSERT_VALUES_EQUAL(0, response->History.size());
        }

        // no history records should remain, just check that nothing crashes
        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }
    }

    Y_UNIT_TEST(ShouldNotTruncateHistoryRecordsNewerThanVolumeHistoryDuration)
    {
        NProto::TStorageServiceConfig storageServiceConfig;
        storageServiceConfig.SetVolumeHistoryDuration(1000);
        storageServiceConfig.SetVolumeHistoryCleanupItemCount(100);

        auto runtime = PrepareTestActorRuntime(std::move(storageServiceConfig));

        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig();
        volume.WaitReady();

        {
            auto clientInfo = CreateVolumeClientInfo(
                NProto::VOLUME_ACCESS_READ_WRITE,
                NProto::VOLUME_MOUNT_LOCAL,
                0);
            volume.AddClient(clientInfo);
        }

        runtime->AdvanceCurrentTime(TDuration::Minutes(2));

        {
            auto clientInfo = CreateVolumeClientInfo(
                NProto::VOLUME_ACCESS_READ_ONLY,
                NProto::VOLUME_MOUNT_REMOTE,
                0);
            volume.AddClient(clientInfo);
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvReadHistoryRequest>(
                runtime->GetCurrentTime(),
                TInstant::Seconds(0),
                Max<size_t>()
            ));
        {
            auto response = volume.RecvResponse<TEvVolumePrivate::TEvReadHistoryResponse>();
            UNIT_ASSERT_VALUES_EQUAL(2, response->History.size());
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvReadHistoryRequest>(
                runtime->GetCurrentTime(),
                TInstant::Seconds(0),
                Max<size_t>()
            ));
        {
            auto response = volume.RecvResponse<TEvVolumePrivate::TEvReadHistoryResponse>();
            UNIT_ASSERT_VALUES_EQUAL(1, response->History.size());
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvUpdateCounters>());
        {
            TDispatchOptions options;
            options.FinalEvents.emplace_back(
                TEvStatsService::EvVolumePartCounters);
            runtime->DispatchEvents(options);
        }

        volume.SendToPipe(
            std::make_unique<TEvVolumePrivate::TEvReadHistoryRequest>(
                runtime->GetCurrentTime(),
                TInstant::Seconds(0),
                Max<size_t>()
            ));
        {
            auto response = volume.RecvResponse<TEvVolumePrivate::TEvReadHistoryResponse>();
            UNIT_ASSERT_VALUES_EQUAL(1, response->History.size());
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
