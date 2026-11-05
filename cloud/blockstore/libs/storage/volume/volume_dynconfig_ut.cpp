#include "volume_ut.h"

#include <cloud/blockstore/libs/storage/api/bootstrapper.h>
#include <cloud/blockstore/libs/storage/partition_nonrepl/part_nonrepl_events_private.h>

#include <cloud/blockstore/libs/config/blockstore_config_holder.h>
#include <cloud/blockstore/libs/config/blockstore_config_management.h>
#include <cloud/blockstore/libs/configs_manager/events.h>
#include <cloud/blockstore/libs/configs_manager/public.h>
#include <cloud/blockstore/libs/configs_manager/testlib/dynconfig.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;
using namespace NTestVolume;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeDynamicConfigTest)
{
    // Verify that Volume starts without a subscription response and updates
    // client-conflict handling without restarting its running partition.
    Y_UNIT_TEST(ShouldUpdateVolumeWithoutWaitingForSubscription)
    {
        // Capture the subscription and the actor serving configuration reads.
        NProto::TStorageServiceConfig config;
        config.SetSendErrorOnAddClientConflict(false);
        auto runtime = PrepareTestActorRuntime(config);
        const auto manager = runtime->AllocateEdgeActor();
        runtime->RegisterService(MakeConfigsManagerServiceId(), manager);
        bool subscribed = false;
        TActorId volumeActor;
        ui32 partitionStarts = 0;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() ==
                TEvConfigsManager::EvSetConfigSubscriptionRequest)
            {
                subscribed = true;
            }
            if (event->GetTypeRewrite() == TEvVolume::EvGetStorageConfigRequest) {
                volumeActor = event->GetRecipientRewrite();
            }
            if (event->GetTypeRewrite() == TEvPartition::EvWaitReadyResponse) {
                ++partitionStarts;
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig();
        volume.WaitReady();
        volume.GetStorageConfig();
        UNIT_ASSERT(subscribed);
        UNIT_ASSERT(volumeActor);

        // Preserve an existing RW client when the conflict policy changes.
        auto client = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            0);
        volume.AddClient(client);
        volume.WaitReady();
        auto readOnlyClient = client;
        readOnlyClient.SetVolumeAccessMode(NProto::VOLUME_ACCESS_READ_ONLY);
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            volume.AddClient(readOnlyClient)->GetStatus());
        const auto startsBeforeUpdate = partitionStarts;

        // Apply the initial subscription response after Volume is already ready.
        NProto::TBlockstoreConfig publication;
        *publication.MutableStorageService() = config;
        publication.MutableStorageService()->SetSendErrorOnAddClientConflict(true);
        auto provider =
            std::make_shared<NTestDynconfig::TTestBlockstoreConfigProvider>(
                publication);
        runtime->Send(new IEventHandle(
            volumeActor,
            manager,
            new TEvConfigsManager::TEvSetConfigSubscriptionResponse(provider)));
        UNIT_ASSERT(
            volume.GetStorageConfig()
                ->Record.GetStorageConfig()
                .GetSendErrorOnAddClientConflict());
        volume.SendAddClientRequest(readOnlyClient);
        UNIT_ASSERT_VALUES_EQUAL(
            E_INVALID_STATE,
            volume.RecvAddClientResponse()->GetStatus());

        // Restore the original policy through a later notification.
        provider->Config.MutableStorageService()
            ->SetSendErrorOnAddClientConflict(false);
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(*runtime, volumeActor, provider);
        UNIT_ASSERT(
            !volume.GetStorageConfig()
                 ->Record.GetStorageConfig()
                 .GetSendErrorOnAddClientConflict());
        UNIT_ASSERT_VALUES_EQUAL(
            S_ALREADY,
            volume.AddClient(readOnlyClient)->GetStatus());
        UNIT_ASSERT_VALUES_EQUAL(startsBeforeUpdate, partitionStarts);
        volume.RemoveClient(client.GetClientId());
    }

    // Verify that an absent ConfigsManager does not block startup or I/O.
    Y_UNIT_TEST(ShouldServeIoWithoutConfigsManager)
    {
        // Start the static path without registering a subscription recipient.
        auto runtime = PrepareTestActorRuntime();
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig();
        volume.WaitReady();
        auto client = CreateVolumeClientInfo(
            NProto::VOLUME_ACCESS_READ_WRITE,
            NProto::VOLUME_MOUNT_REMOTE,
            0);
        volume.AddClient(client);

        // Complete a write and read while no subscription response can arrive.
        const auto range = TBlockRange64::MakeOneBlock(0);
        volume.WriteBlocks(range, client.GetClientId(), 'x');
        const auto response = volume.ReadBlocks(range, client.GetClientId());
        UNIT_ASSERT_VALUES_EQUAL(
            GetBlockContent('x'),
            response->Record.GetBlocks().GetBuffers(0));
    }

    // Verify that later global publications and fallback retain the disk patch.
    Y_UNIT_TEST(ShouldKeepPatchAcrossGlobalUpdatesAndFallback)
    {
        // Capture the ready Volume and persist a distinct per-volume override.
        NProto::TStorageServiceConfig config;
        config.SetMaxMigrationIoDepth(4);
        config.SetMaxMigrationBandwidth(100);
        auto runtime = PrepareTestActorRuntime(config);
        TActorId volumeActor;
        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& event) {
            if (event->GetTypeRewrite() == TEvVolume::EvGetStorageConfigRequest) {
                volumeActor = event->GetRecipientRewrite();
            }
            return TTestActorRuntime::DefaultObserverFunc(event);
        });
        TVolumeClient volume(*runtime);
        volume.UpdateVolumeConfig();
        volume.WaitReady();
        volume.GetStorageConfig();
        NProto::TStorageServiceConfig patch;
        patch.SetMaxMigrationIoDepth(7);
        volume.ChangeStorageConfig(patch);
        volume.WaitReady();

        // Mask the patched field while applying an unpatched global change.
        NProto::TBlockstoreConfig publication;
        *publication.MutableStorageService() = config;
        publication.MutableStorageService()->SetMaxMigrationIoDepth(20);
        publication.MutableStorageService()->SetMaxMigrationBandwidth(200);
        auto provider =
            std::make_shared<NTestDynconfig::TTestBlockstoreConfigProvider>(
                publication);
        NTestDynconfig::SendDynconfigTo(*runtime, volumeActor, provider);
        auto response = volume.GetStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(
            7, response->Record.GetStorageConfig().GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(
            200, response->Record.GetStorageConfig().GetMaxMigrationBandwidth());

        // Restore the static global source without removing the saved patch.
        *provider->Config.MutableStorageService() = config;
        provider->Publish();
        NTestDynconfig::SendDynconfigTo(*runtime, volumeActor, provider);
        response = volume.GetStorageConfig();
        UNIT_ASSERT_VALUES_EQUAL(
            7, response->Record.GetStorageConfig().GetMaxMigrationIoDepth());
        UNIT_ASSERT_VALUES_EQUAL(
            100, response->Record.GetStorageConfig().GetMaxMigrationBandwidth());
    }

    // Verify that a publication during partition boot preserves the selected
    // writer topology and reaches the partition after its delayed STARTED
    // event.
    Y_UNIT_TEST(ShouldKeepWriterModeDuringPartitionBoot)
    {
        for (const bool startupWriter: {false, true}) {
            // Hold the partition startup notification after its actor is
            // created.
            NProto::TStorageServiceConfig config;
            config.SetFreshBlocksWriterEnabled(startupWriter);
            config.SetFreshChannelWriteRequestsEnabled(true);
            config.SetMaxReadWriteRangeSize(2 * DefaultBlockSize);
            auto runtime = PrepareTestActorRuntime(config);
            TActorId volumeActor;
            TAutoPtr<IEventHandle> started;
            bool holdStarted = true;
            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvHiveProxy::EvBootExternalResponse)
                    {
                        const auto* msg =
                            event->Get<TEvHiveProxy::TEvBootExternalResponse>();
                        const_cast<NKikimr::TTabletStorageInfo*>(
                            msg->StorageInfo.Get())
                            ->TabletType =
                            NKikimr::TTabletTypes::BlockStorePartition;
                    }
                    if (event->GetTypeRewrite() ==
                        TEvVolume::EvGetStorageConfigRequest)
                    {
                        volumeActor = event->GetRecipientRewrite();
                    }
                    if (holdStarted &&
                        event->GetTypeRewrite() == TEvBootstrapper::EvStatus &&
                        event->Get<TEvBootstrapper::TEvStatus>()->Status ==
                            TEvBootstrapper::STARTED)
                    {
                        started = event.Release();
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });
            TVolumeClient volume(*runtime);
            volume.UpdateVolumeConfig();
            volume.GetStorageConfig();
            volume.SendWaitReadyRequest();
            TDispatchOptions options;
            options.CustomFinalCondition = [&]
            {
                return bool(started);
            };
            runtime->DispatchEvents(options);
            UNIT_ASSERT(started);

            // Request the opposite topology before Volume handles STARTED.
            NProto::TBlockstoreConfig publication;
            *publication.MutableStorageService() = config;
            publication.MutableStorageService()->SetFreshBlocksWriterEnabled(
                !startupWriter);
            publication.MutableStorageService()->SetMaxReadWriteRangeSize(
                DefaultBlockSize);
            const auto holder =
                std::make_shared<TBlockstoreConfigHolder>(MakeBlockstoreConfig(
                    publication, std::make_shared<TStorageConfigControls>()));
            runtime->Send(new IEventHandle(
                volumeActor, runtime->AllocateEdgeActor(),
                new TEvConfigsManager::TEvConfigChanged(holder)));
            UNIT_ASSERT_VALUES_EQUAL(
                !startupWriter,
                volume.GetStorageConfig()
                    ->Record.GetStorageConfig()
                    .GetFreshBlocksWriterEnabled());

            // Complete the original boot and serve I/O through its selected
            // mode.
            holdStarted = false;
            runtime->Send(started.Release());
            volume.RecvWaitReadyResponse();
            const auto client =
                CreateVolumeClientInfo(NProto::VOLUME_ACCESS_READ_WRITE,
                                       NProto::VOLUME_MOUNT_REMOTE, 0);
            volume.AddClient(client);
            const auto range = TBlockRange64::MakeOneBlock(0);
            volume.WriteBlocks(range, client.GetClientId(), 'x');
            const auto response =
                volume.ReadBlocks(range, client.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('x'),
                response->Record.GetBlocks().GetBuffers(0));

            // Apply the publication to a child created before the update.
            volume.SendReadBlocksRequest(TBlockRange64::WithLength(0, 2),
                                         client.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT, volume.RecvReadBlocksResponse()->GetStatus());
        }
    }

    // Verify that Volume follows the lagging-device mode of the running
    // partition after the desired configuration changes in either direction.
    Y_UNIT_TEST(ShouldKeepRunningLaggingDeviceMode)
    {
        for (const bool startupAllowed: {false, true}) {
            // Create a mirror with one device in each of its three replicas.
            auto registry = MakeIntrusive<TDiskRegistryState>();
            registry->Devices = MakeDeviceList(3, 3);
            registry->AllocateDiskReplicasOnDifferentNodes = true;
            registry->ReplicaCount = 2;
            NProto::TStorageServiceConfig config;
            config.SetLaggingDevicesForMirror3DisksEnabled(startupAllowed);
            TVector<TDiskAgentStatePtr> agents(3);
            auto runtime =
                PrepareTestActorRuntime(config, registry, {}, {}, agents);
            TActorId volumeActor;
            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvVolume::EvGetStorageConfigRequest)
                    {
                        volumeActor = event->GetRecipientRewrite();
                    }
                    if (event->GetTypeRewrite() ==
                        TEvNonreplPartitionPrivate::EvAddLaggingAgentRequest)
                    {
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });
            TVolumeClient volume(*runtime);
            volume.UpdateVolumeConfig(
                0,
                0,
                0,
                0,
                false,
                1,
                NProto::STORAGE_MEDIA_SSD_MIRROR3,
                DefaultDeviceBlockCount * DefaultDeviceBlockSize /
                    DefaultBlockSize);
            volume.WaitReady();
            volume.GetStorageConfig();

            // Change the desired mode without rebuilding the running mirror.
            NProto::TBlockstoreConfig publication;
            *publication.MutableStorageService() = config;
            publication.MutableStorageService()
                ->SetLaggingDevicesForMirror3DisksEnabled(!startupAllowed);
            const auto holder =
                std::make_shared<TBlockstoreConfigHolder>(MakeBlockstoreConfig(
                    publication, std::make_shared<TStorageConfigControls>()));
            runtime->Send(new IEventHandle(
                volumeActor, runtime->AllocateEdgeActor(),
                new TEvConfigsManager::TEvConfigChanged(holder)));
            volume.GetStorageConfig();

            // Keep accepting or rejecting the protocol used by that partition.
            volume.SendDeviceTimedOutRequest("uuid-2.0");
            const auto response = volume.RecvDeviceTimedOutResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                startupAllowed ? S_OK : E_PRECONDITION_FAILED,
                response->GetError().GetCode());
        }
    }

    // Verify delivery to both tablet implementations and the fresh-writer
    // wrapper by changing the accepted I/O range without rebooting the
    // partition.
    Y_UNIT_TEST(ShouldUpdateRunningPartitionRangeLimit)
    {
        for (const auto tabletType:
             {NKikimr::TTabletTypes::BlockStorePartition,
              NKikimr::TTabletTypes::BlockStorePartition2})
        {
            // Start a mounted partition with a range larger than one block.
            NProto::TStorageServiceConfig config;
            config.SetMaxReadWriteRangeSize(8 * DefaultBlockSize);
            config.SetFreshBlocksWriterEnabled(true);
            config.SetFreshChannelWriteRequestsEnabled(true);
            auto runtime = PrepareTestActorRuntime(config);
            TActorId volumeActor;
            ui32 partitionStarts = 0;
            runtime->SetObserverFunc(
                [&](TAutoPtr<IEventHandle>& event)
                {
                    if (event->GetTypeRewrite() ==
                        TEvHiveProxy::EvBootExternalResponse)
                    {
                        const auto* msg =
                            event->Get<TEvHiveProxy::TEvBootExternalResponse>();
                        const_cast<NKikimr::TTabletStorageInfo*>(
                            msg->StorageInfo.Get())
                            ->TabletType = tabletType;
                    }
                    if (event->GetTypeRewrite() ==
                        TEvVolume::EvGetStorageConfigRequest)
                    {
                        volumeActor = event->GetRecipientRewrite();
                    }
                    if (event->GetTypeRewrite() ==
                        TEvPartition::EvWaitReadyResponse)
                    {
                        ++partitionStarts;
                    }
                    return TTestActorRuntime::DefaultObserverFunc(event);
                });
            TVolumeClient volume(*runtime);
            volume.UpdateVolumeConfig();
            volume.WaitReady();
            const auto client =
                CreateVolumeClientInfo(NProto::VOLUME_ACCESS_READ_WRITE,
                                       NProto::VOLUME_MOUNT_REMOTE, 0);
            volume.AddClient(client);
            const auto range = TBlockRange64::WithLength(0, 2);
            volume.WriteBlocks(range, client.GetClientId(), 'x');
            volume.ReadBlocks(range, client.GetClientId());
            volume.GetStorageConfig();
            const auto startsBeforeUpdate = partitionStarts;

            // Publish a smaller range limit to the existing actor graph.
            NProto::TBlockstoreConfig publication;
            *publication.MutableStorageService() = config;
            publication.MutableStorageService()->SetMaxReadWriteRangeSize(
                DefaultBlockSize);
            const auto controls = std::make_shared<TStorageConfigControls>();
            const auto holder = std::make_shared<TBlockstoreConfigHolder>(
                MakeBlockstoreConfig(publication, controls));
            runtime->Send(new IEventHandle(
                volumeActor, runtime->AllocateEdgeActor(),
                new TEvConfigsManager::TEvConfigChanged(holder)));
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultBlockSize,
                volume.GetStorageConfig()
                    ->Record.GetStorageConfig()
                    .GetMaxReadWriteRangeSize());

            // Observe the new partition decision and keep the existing data.
            volume.SendReadBlocksRequest(range, client.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT, volume.RecvReadBlocksResponse()->GetStatus());
            volume.SendWriteBlocksRequest(range, client.GetClientId(), 'y');
            UNIT_ASSERT_VALUES_EQUAL(
                E_ARGUMENT, volume.RecvWriteBlocksResponse()->GetStatus());
            const auto response = volume.ReadBlocks(
                TBlockRange64::MakeOneBlock(0), client.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                GetBlockContent('x'),
                response->Record.GetBlocks().GetBuffers(0));
            UNIT_ASSERT_VALUES_EQUAL(startsBeforeUpdate, partitionStarts);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
