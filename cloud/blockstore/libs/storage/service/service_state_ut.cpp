#include "service_state.h"

#include <cloud/blockstore/libs/storage/core/manually_preempted_volumes.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NActors;

namespace {

////////////////////////////////////////////////////////////////////////////////

TVolumeInfoPtr CreateVolumeInfo(TString diskId)
{
    return std::make_shared<TVolumeInfo>(std::move(diskId));
}

TSharedServiceCountersPtr CreateSharedCounters(TStorageConfigPtr cfg)
{
    return MakeIntrusive<TSharedServiceCounters>(std::move(cfg));
}

TStorageConfigPtr CreateStorageConfig(ui32 limit)
{
    NProto::TStorageServiceConfig cfg;
    cfg.SetMaxLocalVolumes(limit);
    return std::make_shared<TStorageConfig>(
        cfg,
        std::make_shared<NFeatures::TFeaturesConfig>());
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TServiceStateTest)
{
    Y_UNIT_TEST(ShouldReturnCorrentBindingType)
    {
        {
            // remote mount should always return NProto::BINDING_REMOTE binding
            auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));
            auto volume = CreateVolumeInfo("remote1");

            auto bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);

            auto* clientInfo = volume->AddClientInfo("xxx");
            clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;
            volume->PreemptionSource = NProto::SOURCE_INITIAL_MOUNT;

            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);

            volume->PreemptionSource = NProto::SOURCE_NONE;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_REMOTE);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        }

        {
            // local mount should return NProto::BINDING_LOCAL binding
            // always if volume was not preempted manually or by balancer
            auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));
            auto volume = CreateVolumeInfo("remote1");

            auto bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);

            volume->PreemptionSource = NProto::SOURCE_INITIAL_MOUNT;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);

            volume->PreemptionSource = NProto::SOURCE_BALANCER;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);

            volume->PreemptionSource = NProto::SOURCE_MANUAL;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);

            volume->PreemptionSource = NProto::SOURCE_NONE;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);

            auto* clientInfo = volume->AddClientInfo("xxx");
            clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);

            volume->PreemptionSource = NProto::SOURCE_INITIAL_MOUNT;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_NONE,
                NProto::BINDING_NOT_SET,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        }

        {
            // balancer should not pull manually peempted volmes or
            // volumes that don't have completed local mount
            auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));
            auto volume = CreateVolumeInfo("remote1");

            auto* clientInfo = volume->AddClientInfo("xxx");
            clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;
            volume->PreemptionSource = NProto::SOURCE_INITIAL_MOUNT;

            auto bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_BALANCER,
                NProto::BINDING_LOCAL,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);

            volume->PreemptionSource = NProto::SOURCE_MANUAL;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_BALANCER,
                NProto::BINDING_LOCAL,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        }

        {
            // should not be able to manually pull volume if it did not
            // complete local mount
            auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));
            auto volume = CreateVolumeInfo("remote1");

            auto* clientInfo = volume->AddClientInfo("xxx");
            clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;
            volume->PreemptionSource = NProto::SOURCE_INITIAL_MOUNT;

            auto bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_MANUAL,
                NProto::BINDING_LOCAL,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);

            volume->PreemptionSource = NProto::SOURCE_BALANCER;
            bindingType = volume->CalcVolumeBinding(
                NProto::SOURCE_MANUAL,
                NProto::BINDING_LOCAL,
                NProto::VOLUME_MOUNT_LOCAL);
            UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        }
    }

    Y_UNIT_TEST(ShouldNotIncreaseLocalVolumeCountIfRemoteMount)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto remoteVolume = CreateVolumeInfo("remote1");

        auto bindingType = remoteVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_REMOTE,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            remoteVolume->SharedCountersLockAcquired);

        auto* clientInfo = remoteVolume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_REMOTE;

        remoteVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            remoteVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_REMOTE,
            remoteVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            remoteVolume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldKeepSourceInitialMount)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto volume1 = CreateVolumeInfo("v1");
        auto volume2 = CreateVolumeInfo("v2");

        auto bindingType = volume1->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume1->SharedCountersLockAcquired);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            volume1->PreemptionSource);

        auto* clientInfo = volume1->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume1->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            volume1->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume1->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume1->SharedCountersLockAcquired);

        bindingType = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            volume2->PreemptionSource);

        clientInfo = volume2->AddClientInfo("yyy");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        bindingType = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_BALANCER,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            volume2->PreemptionSource);

        clientInfo = volume2->AddClientInfo("yyy");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_BALANCER,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldIncreaseLocalVolumeCountIfLocalMount)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto localVolume = CreateVolumeInfo("local1");

        auto bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        auto* clientInfo = localVolume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldHandleRemountFromRemoteToLocal)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto remoteVolume = CreateVolumeInfo("remote1");

        auto bindingType = remoteVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_REMOTE,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            remoteVolume->SharedCountersLockAcquired);

        auto* clientInfo = remoteVolume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_REMOTE;

        remoteVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            remoteVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_REMOTE,
            remoteVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            remoteVolume->SharedCountersLockAcquired);

        bindingType = remoteVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            remoteVolume->SharedCountersLockAcquired);

        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        remoteVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            remoteVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            remoteVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            remoteVolume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldHandleRemountFromLocalToRemote)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto localVolume = CreateVolumeInfo("local1");

        auto bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        auto* clientInfo = localVolume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_REMOTE,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_REMOTE;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_REMOTE,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            localVolume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(
        ShouldNotIncreaseLocalVolumeCountIfLocalMountForSubsequentRemounts)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto localVolume = CreateVolumeInfo("local1");

        auto bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        auto* clientInfo = localVolume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldNotIncreaseLocalVolumeCountIfAnotherRemoteClientJoins)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto localVolume = CreateVolumeInfo("local1");

        auto bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        auto* clientInfo = localVolume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_REMOTE,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        clientInfo = localVolume->AddClientInfo("yyy");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_REMOTE;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldNotDecreaseLocalVolumeCountIfRemoteMounterRemoved)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto localVolume = CreateVolumeInfo("local1");

        auto bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        auto* clientInfo = localVolume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_REMOTE,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        clientInfo = localVolume->AddClientInfo("yyy");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_REMOTE;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        localVolume->RemoveClientInfo(clientInfo);
        localVolume->OnClientRemoved(*sharedCounters);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
    }

    Y_UNIT_TEST(ShouldNotDecreaseLocalVolumeCountIfLocalMounterRemoved)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto localVolume = CreateVolumeInfo("local1");

        auto bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        auto* localClientInfo = localVolume->AddClientInfo("xxx");
        localClientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_REMOTE,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        auto* remoteClientInfo = localVolume->AddClientInfo("yyy");
        remoteClientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_REMOTE;

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_LOCAL,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        localVolume->RemoveClientInfo(localClientInfo);
        localVolume->OnClientRemoved(*sharedCounters);
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_REMOTE,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
    }

    Y_UNIT_TEST(ShouldRequestRemoteMountIfLimitOfLocalVolumesIsReached)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto v1 = CreateVolumeInfo("local1");
        auto v2 = CreateVolumeInfo("local2");

        auto b1 = v1->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, b1);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, v1->SharedCountersLockAcquired);

        auto b2 = v2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, b2);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            v2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, v2->SharedCountersLockAcquired);

        v2->OnMountFinished(*sharedCounters, NProto::SOURCE_NONE, b2, {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            v2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, v2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(false, v2->SharedCountersLockAcquired);

        auto* localClientInfo = v1->AddClientInfo("xxx");
        localClientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        v1->OnMountFinished(*sharedCounters, NProto::SOURCE_NONE, b1, {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, v1->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, v1->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, v1->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldRunLocalMountIfLocalMountsLimitIsNotReached)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto v1 = CreateVolumeInfo("local1");
        auto v2 = CreateVolumeInfo("local2");

        auto b1 = v1->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, b1);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, v1->SharedCountersLockAcquired);

        auto b2 = v2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, b2);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            v2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, v2->SharedCountersLockAcquired);

        auto* v2Client = v2->AddClientInfo("yyy");
        v2Client->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        v2->OnMountFinished(*sharedCounters, NProto::SOURCE_NONE, b2, {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            v2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, v2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(false, v2->SharedCountersLockAcquired);

        auto* v1Client = v1->AddClientInfo("xxx");
        v1Client->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        v1->OnMountFinished(*sharedCounters, NProto::SOURCE_NONE, b1, {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, v1->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, v1->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, v1->SharedCountersLockAcquired);

        v1->RemoveClientInfo(v1Client);
        v1->OnClientRemoved(*sharedCounters);
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, v1->SharedCountersLockAcquired);

        auto b3 = v2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, b3);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            v2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, v2->SharedCountersLockAcquired);

        v2->OnMountFinished(*sharedCounters, NProto::SOURCE_NONE, b3, {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, v2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, v2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, v2->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldDecreaseLocalVolumeCountIfLocalMountFailed)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto localVolume = CreateVolumeInfo("local1");

        auto bindingType = localVolume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, localVolume->SharedCountersLockAcquired);

        localVolume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            MakeError(E_REJECTED, ""));
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            localVolume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::BINDING_REMOTE,
            localVolume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            localVolume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldAllowToOverrideBindingFromMonitoring)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto volume = CreateVolumeInfo("local1");

        auto bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        auto* clientInfo = volume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_REMOTE,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_MANUAL,
            volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        // check that following mounts won't change binding

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_MANUAL,
            volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        // check that if we "return back" volume it cleans up PreemptionSource

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_LOCAL,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldAllowToOverrideBindingFromBalancer)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto volume = CreateVolumeInfo("local1");

        auto bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        auto* clientInfo = volume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_BALANCER,
            NProto::BINDING_REMOTE,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_BALANCER,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_BALANCER,
            volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        // check that following mounts won't change binding

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_BALANCER,
            volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        // check that if we "return back" volume it cleans up PreemptionSource

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_BALANCER,
            NProto::BINDING_LOCAL,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldChangeLocalVolumeCountIfNotRequested)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto volume = CreateVolumeInfo("v1");

        auto bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            false);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume->SharedCountersLockAcquired);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, volume->PreemptionSource);

        auto* clientInfo = volume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(false, volume->SharedCountersLockAcquired);

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_REMOTE,
            NProto::VOLUME_MOUNT_LOCAL,
            false);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_MANUAL,
            volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(false, volume->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldCheckLimitsIfVolumeIsPulledFromControl)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        // allocate the only available slot for local mounts
        auto volume1 = CreateVolumeInfo("local1");

        auto bindingType1 = volume1->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType1);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume1->SharedCountersLockAcquired);

        auto* clientInfo1 = volume1->AddClientInfo("xxx");
        clientInfo1->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume1->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType1,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume1->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            volume1->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume1->SharedCountersLockAcquired);

        // mount another disk and make sure it mounted remotely
        // because there are not available slots for local mounts
        auto volume2 = CreateVolumeInfo("remote1");

        auto bindingType2 = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType2);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        auto* clientInfo2 = volume2->AddClientInfo("xxx");
        clientInfo2->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType2,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        // pin volume to control
        auto bindingType3 = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_LOCAL,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType3);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            bindingType3,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_MANUAL,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        // manualy return it back
        auto bindingType4 = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_LOCAL,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType4);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            bindingType4,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        // unmount the first volume to free slot for local mounts
        volume1->RemoveClientInfo(clientInfo1);
        volume1->OnClientRemoved(*sharedCounters);
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume1->SharedCountersLockAcquired);

        // mount second volume. now it should be able to bind locally
        auto bindingType5 = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_LOCAL,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType5);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume2->SharedCountersLockAcquired);

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType5,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume2->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldTryToAcquireLocalVolumeAsSoonAsReturnFromControl)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        // allocate the only available slot for local mounts
        auto volume1 = CreateVolumeInfo("local1");

        auto bindingType1 = volume1->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType1);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume1->SharedCountersLockAcquired);

        auto* clientInfo1 = volume1->AddClientInfo("xxx");
        clientInfo1->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume1->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType1,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume1->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            volume1->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume1->SharedCountersLockAcquired);

        // mount another disk and make sure it mounted remotely
        // because there are not available slots for local mounts
        auto volume2 = CreateVolumeInfo("remote1");

        auto bindingType2 = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType2);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        auto* clientInfo2 = volume2->AddClientInfo("xxx");
        clientInfo2->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType2,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_INITIAL_MOUNT,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        // pin volume to control
        auto bindingType3 = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_LOCAL,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType3);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            bindingType3,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_MANUAL,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume2->SharedCountersLockAcquired);

        // unmount the first volume to free slot for local mounts
        volume1->RemoveClientInfo(clientInfo1);
        volume1->OnClientRemoved(*sharedCounters);
        UNIT_ASSERT_VALUES_EQUAL(0, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(false, volume1->SharedCountersLockAcquired);

        // manualy return it back
        auto bindingType4 = volume2->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_LOCAL,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType4);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume2->SharedCountersLockAcquired);

        volume2->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            bindingType4,
            {});

        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume2->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_NONE,
            volume2->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume2->SharedCountersLockAcquired);
    }

    Y_UNIT_TEST(ShouldSetProperPreemptionAndBindingForManuallyPreemptedVolumes)
    {
        auto preemptedVolumes = CreateManuallyPreemptedVolumes();
        preemptedVolumes->AddVolume("disk1", TInstant::Now());

        TServiceState state(preemptedVolumes);

        {
            auto volume = state.GetOrAddVolume("disk0");

            UNIT_ASSERT_VALUES_UNEQUAL(
                NProto::SOURCE_MANUAL,
                volume->PreemptionSource);
        }

        {
            auto volume = state.GetOrAddVolume("disk1");

            UNIT_ASSERT_VALUES_EQUAL(
                NProto::SOURCE_MANUAL,
                volume->PreemptionSource);

            UNIT_ASSERT_VALUES_EQUAL(
                NProto::BINDING_REMOTE,
                volume->BindingType);
        }
    }

    Y_UNIT_TEST(ShouldDetectIfPreemptionSourceChanged)
    {
        auto sharedCounters = CreateSharedCounters(CreateStorageConfig(1));

        auto volume = CreateVolumeInfo("local1");

        auto bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        auto* clientInfo = volume->AddClientInfo("xxx");
        clientInfo->VolumeMountMode = NProto::VOLUME_MOUNT_LOCAL;

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            volume->ShouldSyncManuallyPreemptedVolumes());

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_REMOTE,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_MANUAL,
            volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            volume->ShouldSyncManuallyPreemptedVolumes());

        // check that following mounts won't change binding

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_NONE,
            NProto::BINDING_NOT_SET,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(
            NProto::SOURCE_MANUAL,
            volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_REMOTE, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);
        UNIT_ASSERT_VALUES_EQUAL(
            false,
            volume->ShouldSyncManuallyPreemptedVolumes());

        // check that if we "return back" volume it cleans up PreemptionSource

        bindingType = volume->OnMountStarted(
            *sharedCounters,
            NProto::SOURCE_MANUAL,
            NProto::BINDING_LOCAL,
            NProto::VOLUME_MOUNT_LOCAL,
            true);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, bindingType);
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);

        volume->OnMountFinished(
            *sharedCounters,
            NProto::SOURCE_NONE,
            bindingType,
            {});
        UNIT_ASSERT_VALUES_EQUAL(1, sharedCounters->LocalVolumeCount);
        UNIT_ASSERT_VALUES_EQUAL(NProto::SOURCE_NONE, volume->PreemptionSource);
        UNIT_ASSERT_VALUES_EQUAL(NProto::BINDING_LOCAL, volume->BindingType);
        UNIT_ASSERT_VALUES_EQUAL(true, volume->SharedCountersLockAcquired);
        UNIT_ASSERT_VALUES_EQUAL(
            true,
            volume->ShouldSyncManuallyPreemptedVolumes());
    }
}

}   // namespace NCloud::NBlockStore::NStorage

template <>
inline void Out<NCloud::NBlockStore::NProto::EVolumeBinding>(
    IOutputStream& o,
    const NCloud::NBlockStore::NProto::EVolumeBinding e)
{
    switch (e) {
        case NCloud::NBlockStore::NProto::BINDING_NOT_SET: {
            o << "BINDING_NOT_SET";
            break;
        }
        case NCloud::NBlockStore::NProto::BINDING_LOCAL: {
            o << "BINDING_LOCAL";
            break;
        }
        case NCloud::NBlockStore::NProto::BINDING_REMOTE: {
            o << "BINDING_REMOTE";
            break;
        }
        default: {
            o << "BINDING_UNKNOWN";
        }
    }
}

template <>
inline void Out<NCloud::NBlockStore::NProto::EPreemptionSource>(
    IOutputStream& o,
    const NCloud::NBlockStore::NProto::EPreemptionSource e)
{
    switch (e) {
        case NCloud::NBlockStore::NProto::SOURCE_NONE: {
            o << "SOURCE_NONE";
            break;
        }
        case NCloud::NBlockStore::NProto::SOURCE_BALANCER: {
            o << "SOURCE_BALANCER";
            break;
        }
        case NCloud::NBlockStore::NProto::SOURCE_MANUAL: {
            o << "SOURCE_MANUAL";
            break;
        }
        case NCloud::NBlockStore::NProto::SOURCE_INITIAL_MOUNT: {
            o << "SOURCE_INITIAL_MOUNT";
            break;
        }
        default: {
            o << "SOURCE_UNKNOWN";
        }
    }
}
