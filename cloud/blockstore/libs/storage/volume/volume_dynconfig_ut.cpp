#include "volume_ut.h"

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
}

}   // namespace NCloud::NBlockStore::NStorage
