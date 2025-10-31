#include "restore_validator_actor.h"

#include <ydb/library/actors/testlib/test_runtime.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

static NActors::TActorId EdgeActor;

NActors::TActorId MakeStorageServiceId()
{
    return EdgeActor;
}

NActors::TActorId MakeSSProxyServiceId()
{
    return EdgeActor;
}

NActors::TActorId MakeVolumeProxyServiceId()
{
    return EdgeActor;
}

namespace NDiskRegistry {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRestoreValidatorActorTest)
{
    struct TSetupEnvironment
        : public TCurrentTestCase
    {
        NActors::TTestActorRuntimeBase ActorSystem;

        void SetUp(NUnitTest::TTestContext&) override
        {
            ActorSystem.Initialize();
            ActorSystem.SetDispatchTimeout(TDuration::Seconds(5));

            EdgeActor = ActorSystem.AllocateEdgeActor();
        }
    };

    Y_UNIT_TEST_F(CheckEmptyBackup, TSetupEnvironment)
    {
        ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                {}
            ));

        auto response = ActorSystem.GrabEdgeEvent<
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();

        UNIT_ASSERT(HasError(response->GetError()));
    }

    Y_UNIT_TEST_F(CheckSingleDiskId, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        backup.Disks.emplace_back().SetDiskId("Disk 1");
        backup.Disks.emplace_back().SetDiskId("Disk 1");

        ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        auto response = ActorSystem.GrabEdgeEvent<
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();

        UNIT_ASSERT(HasError(response->GetError()));
    }

    Y_UNIT_TEST_F(CheckSingleGroupId, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        backup.PlacementGroups.emplace_back().SetGroupId("Group 1");
        backup.PlacementGroups.emplace_back().SetGroupId("Group 1");

        ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        auto response = ActorSystem.GrabEdgeEvent<
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();

        UNIT_ASSERT(HasError(response->GetError()));
    }

    Y_UNIT_TEST_F(CheckSingleAgentId, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        backup.Agents.emplace_back().SetAgentId("Agent 1");
        backup.Agents.emplace_back().SetAgentId("Agent 1");

        ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        auto response = ActorSystem.GrabEdgeEvent<
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();

        UNIT_ASSERT(HasError(response->GetError()));
    }

    Y_UNIT_TEST_F(CheckNewDisksInBackup, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        backup.Disks.emplace_back().SetDiskId("Disk 1");
        backup.Disks.emplace_back().SetDiskId("Disk 3");
        backup.Disks.emplace_back().SetDiskId("Disk 2");

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 3");
        volumeListResponse->Record.AddVolumes("Disk 1");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<
                TEvSSProxy::TEvDescribeVolumeRequest>()->DiskId,
            "Disk 1");
        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<
                TEvSSProxy::TEvDescribeVolumeRequest>()->DiskId,
            "Disk 3");
    }

    Y_UNIT_TEST_F(CheckNewDisksInSS, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        backup.Disks.emplace_back().SetDiskId("Disk 3");
        backup.Disks.emplace_back().SetDiskId("Disk 1");

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 3");
        volumeListResponse->Record.AddVolumes("Disk 1");
        volumeListResponse->Record.AddVolumes("Disk 2");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>()->DiskId,
            "Disk 1");
        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>()->DiskId,
            "Disk 2");
        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>()->DiskId,
            "Disk 3");

        auto sendDiskId = [this, &validatorId](const TString& diskId) {
            NKikimrSchemeOp::TPathDescription description;
            description.MutableBlockStoreVolumeDescription()
                ->MutableVolumeConfig()->SetDiskId(diskId);
             auto describeVolumeResponse
                = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                    "", std::move(description));

            ActorSystem.Send(new NActors::IEventHandle(
                validatorId,
                EdgeActor,
                describeVolumeResponse.release()));
        };

        sendDiskId("Disk 1");
        sendDiskId("Disk 2");
        sendDiskId("Disk 3");

        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                ->Record.GetDiskId(),
            "Disk 1");
        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                ->Record.GetDiskId(),
            "Disk 2");
        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                ->Record.GetDiskId(),
            "Disk 3");
    }

    Y_UNIT_TEST_F(CheckAnotherFolder, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        auto& config = backup.Disks.emplace_back();
        config.SetDiskId("Disk 1");
        config.SetFolderId("Folder 1");

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 1");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();

        NKikimrSchemeOp::TPathDescription description;
        auto* mutableVolumeConfig = description.MutableBlockStoreVolumeDescription()
            ->MutableVolumeConfig();
        *mutableVolumeConfig->MutableFolderId() = "Folder 2";
        mutableVolumeConfig->SetDiskId("Disk 1");
        auto describeVolumeResponse
            = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>("", std::move(description));

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            describeVolumeResponse.release()));

        UNIT_ASSERT(
            ActorSystem.GrabEdgeEvent<
                TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>()
                    ->LoadDBState.Disks.empty());
    }

    Y_UNIT_TEST_F(CheckAnotherCloud, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        auto& config = backup.Disks.emplace_back();
        config.SetDiskId("Disk 1");
        config.SetCloudId("Cloud 1");

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 1");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();

        NKikimrSchemeOp::TPathDescription description;
        auto* mutableVolumeConfig = description.MutableBlockStoreVolumeDescription()
            ->MutableVolumeConfig();
        *mutableVolumeConfig->MutableCloudId() = "Cloud 2";
        mutableVolumeConfig->SetDiskId("Disk 1");
        auto describeVolumeResponse
            = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>("", std::move(description));

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            describeVolumeResponse.release()));

        UNIT_ASSERT(
            ActorSystem.GrabEdgeEvent<
                TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>()
                    ->LoadDBState.Disks.empty());
    }

    Y_UNIT_TEST_F(CheckAnotherBlockSize, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        auto& config = backup.Disks.emplace_back();
        config.SetDiskId("Disk 1");
        config.SetBlockSize(42);

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 1");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();

        NKikimrSchemeOp::TPathDescription description;
        auto* mutableVolumeConfig = description.MutableBlockStoreVolumeDescription()
            ->MutableVolumeConfig();
        mutableVolumeConfig->SetBlockSize(43);
        mutableVolumeConfig->SetDiskId("Disk 1");
        auto describeVolumeResponse
            = std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>("", std::move(description));

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            describeVolumeResponse.release()));

        UNIT_ASSERT(
            ActorSystem.GrabEdgeEvent<
                TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>()
                    ->LoadDBState.Disks.empty());
    }

    Y_UNIT_TEST_F(CheckPlacementGroups, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        backup.Disks.emplace_back().SetDiskId("Disk 1");
        backup.Disks.emplace_back().SetDiskId("Disk 2");

        auto& placementGroup1 = backup.PlacementGroups.emplace_back();
        placementGroup1.SetGroupId("Group 1");
        placementGroup1.AddDisks()->SetDiskId("Disk 1");
        placementGroup1.AddDisks()->SetDiskId("Disk 2");

        auto& placementGroup2 = backup.PlacementGroups.emplace_back();
        placementGroup2.SetGroupId("Group 2");
        placementGroup2.AddDisks()->SetDiskId("Disk 2");

        auto& placementGroup3 = backup.PlacementGroups.emplace_back();
        placementGroup3.SetGroupId("Group 3");

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 1");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();

        NKikimrSchemeOp::TPathDescription description;
        auto* mutableVolumeConfig = description.MutableBlockStoreVolumeDescription()
            ->MutableVolumeConfig();
        mutableVolumeConfig->SetDiskId("Disk 1");
        auto describeVolumeResponse = std::make_unique<
            TEvSSProxy::TEvDescribeVolumeResponse>("", std::move(description));

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            describeVolumeResponse.release()));

        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                ->Record.GetDiskId(),
            "Disk 1");

        auto volumeInfoResponse
            = std::make_unique<TEvVolume::TEvGetVolumeInfoResponse>();
        volumeInfoResponse->Record.MutableVolume()->SetDiskId("Disk 1");
        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeInfoResponse.release()));

        auto response = ActorSystem.GrabEdgeEvent<
                TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();
        auto& placementGroups = response->LoadDBState.PlacementGroups;

        UNIT_ASSERT_EQUAL(
            placementGroups.size(), 2);
        UNIT_ASSERT_EQUAL(
            placementGroups[0].GetGroupId(), "Group 1");
        UNIT_ASSERT_EQUAL(
            placementGroups[0].GetDisks().size(), 1);
        UNIT_ASSERT_EQUAL(
            placementGroups[0].GetDisks()[0].GetDiskId(), "Disk 1");
    }

    Y_UNIT_TEST_F(CheckDevices, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        backup.Disks.emplace_back().SetDiskId("Disk 1");
        auto& disk = backup.Disks.emplace_back();
        disk.SetDiskId("Disk 2");
        disk.AddDeviceUUIDs("Device 2");

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 1");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();

        NKikimrSchemeOp::TPathDescription description;
        auto* mutableVolumeConfig = description.MutableBlockStoreVolumeDescription()
            ->MutableVolumeConfig();
        mutableVolumeConfig->SetDiskId("Disk 1");
        auto describeVolumeResponse = std::make_unique<
            TEvSSProxy::TEvDescribeVolumeResponse>("", std::move(description));

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            describeVolumeResponse.release()));

        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                ->Record.GetDiskId(),
            "Disk 1");

        auto volumeInfoResponse
            = std::make_unique<TEvVolume::TEvGetVolumeInfoResponse>();
        auto& volume = *volumeInfoResponse->Record.MutableVolume();
        volume.SetDiskId("Disk 1");

        NProto::TDevice device;
        device.SetDeviceUUID("Device 1");
        volume.MutableDevices()->Add(std::move(device));
        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeInfoResponse.release()));

        auto response = ActorSystem.GrabEdgeEvent<
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();
        UNIT_ASSERT(HasError(response->GetError()));
    }

    Y_UNIT_TEST_F(CheckValidation, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        auto& diskConfig = backup.Disks.emplace_back();
        diskConfig.SetDiskId("Disk 1");
        diskConfig.SetFolderId("Folder 1");
        diskConfig.SetCloudId("Cloud 1");
        diskConfig.SetBlockSize(42);

        auto& diskConfigRepl2 = backup.Disks.emplace_back();
        diskConfigRepl2.SetDiskId("Disk 1/1");
        diskConfigRepl2.SetFolderId("Folder 1");
        diskConfigRepl2.SetCloudId("Cloud 1");
        diskConfigRepl2.SetBlockSize(42);
        diskConfigRepl2.AddDeviceUUIDs("Device 1");
        diskConfigRepl2.AddDeviceUUIDs("Device 2");

        auto& diskConfigRepl1 = backup.Disks.emplace_back();
        diskConfigRepl1.SetDiskId("Disk 1/0");
        diskConfigRepl1.SetFolderId("Folder 1");
        diskConfigRepl1.SetCloudId("Cloud 1");
        diskConfigRepl1.SetBlockSize(42);
        diskConfigRepl1.AddDeviceUUIDs("Device 3");
        diskConfigRepl1.AddDeviceUUIDs("Device 4");

        auto& placementGroup = backup.PlacementGroups.emplace_back();
        placementGroup.SetGroupId("Group 1");
        placementGroup.AddDisks()->SetDiskId("Disk 1");
        placementGroup.AddDisks()->SetDiskId("Disk 1/0");
        placementGroup.AddDisks()->SetDiskId("Disk 1/1");

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 1");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();

        NKikimrSchemeOp::TPathDescription description;
        auto* mutableVolumeConfig =
            description.MutableBlockStoreVolumeDescription()
            ->MutableVolumeConfig();
        mutableVolumeConfig->SetDiskId("Disk 1");
        mutableVolumeConfig->SetBlockSize(42);
        mutableVolumeConfig->SetFolderId("Folder 1");
        mutableVolumeConfig->SetCloudId("Cloud 1");
        auto describeVolumeResponse = std::make_unique<
            TEvSSProxy::TEvDescribeVolumeResponse>("", std::move(description));

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            describeVolumeResponse.release()));

        UNIT_ASSERT_EQUAL(
            ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                ->Record.GetDiskId(),
            "Disk 1");

        auto volumeInfoResponse
            = std::make_unique<TEvVolume::TEvGetVolumeInfoResponse>();
        auto& volume = *volumeInfoResponse->Record.MutableVolume();
        volume.SetDiskId("Disk 1");

        NProto::TDevice device_1;
        device_1.SetDeviceUUID("Device 1");
        NProto::TDevice device_2;
        device_2.SetDeviceUUID("Device 2");
        NProto::TDevice device_3;
        device_3.SetDeviceUUID("Device 3");
        NProto::TDevice device_4;
        device_4.SetDeviceUUID("Device 4");

        auto& devices = *volume.MutableDevices();
        devices.Add(std::move(device_1));
        devices.Add(std::move(device_2));

        NProto::TReplicaInfo replica;
        auto& replicaDevices = *replica.MutableDevices();
        replicaDevices.Add(std::move(device_3));
        replicaDevices.Add(std::move(device_4));
        volume.MutableReplicas()->Add(std::move(replica));

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeInfoResponse.release()));

        auto response = ActorSystem.GrabEdgeEvent<
                TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();
        auto& state = response->LoadDBState;
        UNIT_ASSERT_EQUAL(
            state.Disks.size(), 3);
        UNIT_ASSERT_EQUAL(
            state.Disks[0].GetDiskId(), "Disk 1");
        UNIT_ASSERT_EQUAL(
            state.Disks[1].GetDiskId(), "Disk 1/0");
        UNIT_ASSERT_EQUAL(
            state.Disks[2].GetDiskId(), "Disk 1/1");
        UNIT_ASSERT_EQUAL(
            state.PlacementGroups[0].GetGroupId(), "Group 1");
        UNIT_ASSERT_EQUAL(
            state.PlacementGroups[0].GetDisks().size(), 3);
        UNIT_ASSERT_EQUAL(
            state.PlacementGroups[0].GetDisks()[0].GetDiskId(), "Disk 1");
        UNIT_ASSERT_EQUAL(
            state.PlacementGroups[0].GetDisks()[1].GetDiskId(), "Disk 1/0");
        UNIT_ASSERT_EQUAL(
            state.PlacementGroups[0].GetDisks()[2].GetDiskId(), "Disk 1/1");
    }

    Y_UNIT_TEST_F(CheckRestoreDiskMissingInSS, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        {
            auto& diskConfig = backup.Disks.emplace_back();
            diskConfig.SetDiskId("Disk 1");
            diskConfig.SetFolderId("Folder 1");
            diskConfig.SetCloudId("Cloud 1");
            diskConfig.SetBlockSize(41);
        }
        {
            auto& diskConfig = backup.Disks.emplace_back();
            diskConfig.SetDiskId("Disk 2");
            diskConfig.SetFolderId("Folder 2");
            diskConfig.SetCloudId("Cloud 2");
            diskConfig.SetBlockSize(42);
        }
        {
            auto& diskConfig = backup.Disks.emplace_back();
            diskConfig.SetDiskId("Disk 3");
            diskConfig.SetFolderId("Folder 3");
            diskConfig.SetCloudId("Cloud 3");
            diskConfig.SetBlockSize(43);
        }
        {
            auto& diskConfig = backup.Disks.emplace_back();
            diskConfig.SetDiskId("Disk 4");
            diskConfig.SetFolderId("Folder 4");
            diskConfig.SetCloudId("Cloud 4");
            diskConfig.SetBlockSize(44);
        }
        {
            auto& diskConfig = backup.Disks.emplace_back();
            diskConfig.SetDiskId("Disk 4/0");
            diskConfig.SetFolderId("Folder 4");
            diskConfig.SetCloudId("Cloud 4");
            diskConfig.SetBlockSize(44);
        }
        {
            auto& diskConfig = backup.Disks.emplace_back();
            diskConfig.SetDiskId("Disk 4/1");
            diskConfig.SetFolderId("Folder 4");
            diskConfig.SetCloudId("Cloud 4");
            diskConfig.SetBlockSize(44);
        }

        backup.DisksToCleanup.push_back("Disk 4");

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(
                EdgeActor,
                {},
                0,
                backup
            ));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse
            = std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 1");
        volumeListResponse->Record.AddVolumes("Disk 3");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
        {
            NKikimrSchemeOp::TPathDescription description;
            auto* mutableVolumeConfig =
                description.MutableBlockStoreVolumeDescription()
                ->MutableVolumeConfig();
            mutableVolumeConfig->SetDiskId("Disk 1");
            mutableVolumeConfig->SetBlockSize(41);
            mutableVolumeConfig->SetFolderId("Folder 1");
            mutableVolumeConfig->SetCloudId("Cloud 1");
            auto describeVolumeResponse = std::make_unique<
                TEvSSProxy::TEvDescribeVolumeResponse>("", std::move(description));

            ActorSystem.Send(new NActors::IEventHandle(
                validatorId,
                EdgeActor,
                describeVolumeResponse.release()));
        }

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
        {
            NKikimrSchemeOp::TPathDescription description;
            auto* mutableVolumeConfig =
                description.MutableBlockStoreVolumeDescription()
                ->MutableVolumeConfig();
            mutableVolumeConfig->SetDiskId("Disk 3");
            mutableVolumeConfig->SetBlockSize(43);
            mutableVolumeConfig->SetFolderId("Folder 3");
            mutableVolumeConfig->SetCloudId("Cloud 3");
            auto describeVolumeResponse = std::make_unique<
                TEvSSProxy::TEvDescribeVolumeResponse>("", std::move(description));

            ActorSystem.Send(new NActors::IEventHandle(
                validatorId,
                EdgeActor,
                describeVolumeResponse.release()));
        }

        {
            UNIT_ASSERT_EQUAL(
                ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                    ->Record.GetDiskId(),
                "Disk 1");

            auto volumeInfoResponse
                = std::make_unique<TEvVolume::TEvGetVolumeInfoResponse>();
            auto& volume = *volumeInfoResponse->Record.MutableVolume();
            volume.SetDiskId("Disk 1");

            ActorSystem.Send(new NActors::IEventHandle(
                validatorId,
                EdgeActor,
                volumeInfoResponse.release()));
        }

        {
            UNIT_ASSERT_EQUAL(
                ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                    ->Record.GetDiskId(),
                "Disk 3");

            auto volumeInfoResponse
                = std::make_unique<TEvVolume::TEvGetVolumeInfoResponse>();
            auto& volume = *volumeInfoResponse->Record.MutableVolume();
            volume.SetDiskId("Disk 3");

            ActorSystem.Send(new NActors::IEventHandle(
                validatorId,
                EdgeActor,
                volumeInfoResponse.release()));
        }

        auto response = ActorSystem.GrabEdgeEvent<
                TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();
        auto& state = response->LoadDBState;
        UNIT_ASSERT_EQUAL(
            state.Disks.size(), 5);
        UNIT_ASSERT_EQUAL(
            state.Disks[0].GetDiskId(), "Disk 1");
        UNIT_ASSERT_EQUAL(
            state.Disks[1].GetDiskId(), "Disk 3");
        UNIT_ASSERT_EQUAL(
            state.Disks[2].GetDiskId(), "Disk 4");
        UNIT_ASSERT_EQUAL(
            state.Disks[3].GetDiskId(), "Disk 4/0");
        UNIT_ASSERT_EQUAL(
            state.Disks[4].GetDiskId(), "Disk 4/1");
    }

    Y_UNIT_TEST_F(CheckSkipRestoreShadowDisk, TSetupEnvironment)
    {
        TDiskRegistryStateSnapshot backup;
        {   // Source disk
            auto& diskConfig = backup.Disks.emplace_back();
            diskConfig.SetDiskId("Disk 1");
            diskConfig.SetFolderId("Folder 1");
            diskConfig.SetCloudId("Cloud 1");
            diskConfig.SetBlockSize(41);
        }
        {   // checkpoint disk
            auto& diskConfig = backup.Disks.emplace_back();
            diskConfig.SetDiskId("Disk 1-cp1");
            diskConfig.SetFolderId("Folder 1");
            diskConfig.SetCloudId("Cloud 1");
            diskConfig.SetBlockSize(41);
            auto* checkpoint = diskConfig.MutableCheckpointReplica();
            checkpoint->SetSourceDiskId("Disk 1");
            checkpoint->SetCheckpointId("cp1");
        }

        auto validatorId = ActorSystem.Register(
            new TRestoreValidationActor(EdgeActor, {}, 0, backup));

        ActorSystem.GrabEdgeEvent<TEvService::TEvListVolumesRequest>();

        auto volumeListResponse =
            std::make_unique<TEvService::TEvListVolumesResponse>();
        volumeListResponse->Record.AddVolumes("Disk 1");

        ActorSystem.Send(new NActors::IEventHandle(
            validatorId,
            EdgeActor,
            volumeListResponse.release()));

        ActorSystem.GrabEdgeEvent<TEvSSProxy::TEvDescribeVolumeRequest>();
        {
            NKikimrSchemeOp::TPathDescription description;
            auto* mutableVolumeConfig =
                description.MutableBlockStoreVolumeDescription()
                    ->MutableVolumeConfig();
            mutableVolumeConfig->SetDiskId("Disk 1");
            mutableVolumeConfig->SetBlockSize(41);
            mutableVolumeConfig->SetFolderId("Folder 1");
            mutableVolumeConfig->SetCloudId("Cloud 1");
            auto describeVolumeResponse =
                std::make_unique<TEvSSProxy::TEvDescribeVolumeResponse>(
                    "",
                    std::move(description));

            ActorSystem.Send(new NActors::IEventHandle(
                validatorId,
                EdgeActor,
                describeVolumeResponse.release()));
        }

        {
            UNIT_ASSERT_EQUAL(
                ActorSystem.GrabEdgeEvent<TEvVolume::TEvGetVolumeInfoRequest>()
                    ->Record.GetDiskId(),
                "Disk 1");

            auto volumeInfoResponse =
                std::make_unique<TEvVolume::TEvGetVolumeInfoResponse>();
            auto& volume = *volumeInfoResponse->Record.MutableVolume();
            volume.SetDiskId("Disk 1");

            ActorSystem.Send(new NActors::IEventHandle(
                validatorId,
                EdgeActor,
                volumeInfoResponse.release()));
        }

        auto response = ActorSystem.GrabEdgeEvent<
            TEvDiskRegistryPrivate::TEvRestoreDiskRegistryValidationResponse>();
        auto& state = response->LoadDBState;
        UNIT_ASSERT_EQUAL(state.Disks.size(), 1);
        UNIT_ASSERT_EQUAL(state.Disks[0].GetDiskId(), "Disk 1");
    }
}

}   // namespace NDiskRegistry
}   // namespace NCloud::NBlockStore::NStorage
