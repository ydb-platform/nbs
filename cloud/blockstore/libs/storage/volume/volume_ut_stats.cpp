#include "volume_ut.h"

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

        auto obs = [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvVolumePrivate::EvPartStatsSaved) {
                ++partStatsSaved;
            } else if (event->Recipient == MakeStorageStatsServiceId()
                    && event->GetTypeRewrite() == TEvStatsService::EvVolumePartCounters)
            {
                auto* msg = event->Get<TEvStatsService::TEvVolumePartCounters>();

                bytesCount = msg->DiskCounters->Simple.BytesCount.Value;
            }

            return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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

        auto obs = [&] (auto& runtime, TAutoPtr<IEventHandle>& event) {
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


            return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
        auto observer = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
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

            return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
        auto observer = [&](auto& runtime, TAutoPtr<IEventHandle>& event)
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

            return TTestActorRuntime::DefaultObserverFunc(runtime, event);
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
}

}   // namespace NCloud::NBlockStore::NStorage
