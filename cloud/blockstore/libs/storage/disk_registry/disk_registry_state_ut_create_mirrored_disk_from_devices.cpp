#include "disk_registry_state.h"

#include "disk_registry_database.h"

#include <cloud/blockstore/libs/storage/disk_registry/testlib/test_state.h>
#include <cloud/blockstore/libs/storage/testlib/test_executor.h>
#include <cloud/blockstore/libs/storage/testlib/ut_helpers.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NBlockStore::NStorage {

using namespace NDiskRegistryStateTest;

namespace {

TDiskRegistryState::TCreateDiskFromDevicesParams MakeMirror2Params(
    bool force = true,
    bool keepBlockCount = true)
{
    return TDiskRegistryState::TCreateDiskFromDevicesParams(
        DefaultLogicalBlockSize,
        NProto::STORAGE_MEDIA_SSD_MIRROR2,
        force,
        keepBlockCount);
}

TVector<TVector<TString>> MakeMirror2ReplicaUuids()
{
    return {
        {"uuid-1", "uuid-2"},
        {"uuid-3", "uuid-4"},
    };
}

std::unique_ptr<TDiskRegistryState> MakeDefaultState()
{
    return TDiskRegistryStateBuilder()
        .WithKnownAgents({
            AgentConfig(
                1,
                {
                    Device("dev-1", "uuid-1", "rack-1"),
                    Device("dev-2", "uuid-2", "rack-1"),
                }),
            AgentConfig(
                2,
                {
                    Device("dev-1", "uuid-3", "rack-2"),
                    Device("dev-2", "uuid-4", "rack-2"),
                }),
        })
        .Build();
}

NProto::TError CreateMirroredDiskFromDevices(
    TDiskRegistryState& state,
    TDiskRegistryDatabase& db,
    const TString& diskId,
    const TDiskRegistryState::TCreateDiskFromDevicesParams& params,
    const TVector<TVector<TString>>& replicas)
{
    TDiskRegistryState::TAllocateDiskResult result;
    return state.CreateDiskFromDevices(
        Now(),
        db,
        diskId,
        params,
        {},
        replicas,
        &result);
}

}   // namespace

Y_UNIT_TEST_SUITE(TDiskRegistryStateCreateMirroredDiskFromDevicesTest)
{
    Y_UNIT_TEST(ShouldRejectInvalidCreateMirroredDiskFromDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto params =
                    TDiskRegistryState::TCreateDiskFromDevicesParams(
                        DefaultLogicalBlockSize,
                        NProto::STORAGE_MEDIA_SSD_NONREPLICATED,
                        /*force=*/false,
                        /*keepBlockCount=*/false);

                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    params,
                    MakeMirror2ReplicaUuids());

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains(
                    "Replicas count is inconsistent with media kind"));
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    {MakeMirror2ReplicaUuids()[0]});

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("expects 2 replicas"));
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> replicasWithGap = {
                    {"uuid-1", ""},
                    {"uuid-3", ""},
                };

                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    replicasWithGap);

                UNIT_ASSERT_VALUES_EQUAL(E_INVALID_STATE, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("no data for device"));
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const TVector<TVector<TString>> duplicateReplicas = {
                    {"uuid-1"},
                    {"uuid-1"},
                };

                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    duplicateReplicas);

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("already reserved"));
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto masterStatePtr =
                    TDiskRegistryStateBuilder()
                        .WithKnownAgents({
                            AgentConfig(
                                1,
                                {
                                    Device("dev-1", "uuid-1", "rack-1"),
                                    Device("dev-2", "uuid-2", "rack-1"),
                                }),
                            AgentConfig(
                                2,
                                {
                                    Device("dev-1", "uuid-3", "rack-2"),
                                    Device("dev-2", "uuid-4", "rack-2"),
                                }),
                        })
                        .WithDisks({Disk("disk-1", {"uuid-1"})})
                        .Build();

                const auto error = CreateMirroredDiskFromDevices(
                    *masterStatePtr,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    MakeMirror2ReplicaUuids());

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(error.GetMessage().Contains("invalid master disk"));
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                auto replicaState =
                    Disk("disk-1/1", {"uuid-3"}, NProto::DISK_STATE_ONLINE);
                replicaState.SetMasterDiskId("other-disk");

                auto boundStatePtr =
                    TDiskRegistryStateBuilder()
                        .WithKnownAgents({
                            AgentConfig(
                                1,
                                {
                                    Device("dev-1", "uuid-1", "rack-1"),
                                    Device("dev-2", "uuid-2", "rack-1"),
                                }),
                            AgentConfig(
                                2,
                                {
                                    Device("dev-1", "uuid-3", "rack-2"),
                                    Device("dev-2", "uuid-4", "rack-2"),
                                }),
                        })
                        .WithDisks({std::move(replicaState)})
                        .Build();

                const auto error = CreateMirroredDiskFromDevices(
                    *boundStatePtr,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    MakeMirror2ReplicaUuids());

                UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
                UNIT_ASSERT(
                    error.GetMessage().Contains("already attached to disk"));
            });

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                const auto error = CreateMirroredDiskFromDevices(
                    state,
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    {
                        {"uuid-1", "missing"},
                        {"uuid-3"},
                    });

                UNIT_ASSERT(error.GetCode() != S_OK);
            });
    }

    Y_UNIT_TEST(ShouldCreateMirroredDiskFromDevices)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TDiskRegistryDatabase db) { db.InitSchema(); });

        auto statePtr = MakeDefaultState();
        TDiskRegistryState& state = *statePtr;

        const auto replicaUuids = MakeMirror2ReplicaUuids();

        executor.WriteTx(
            [&](TDiskRegistryDatabase db)
            {
                TDiskRegistryState::TAllocateDiskResult result;
                const auto error = state.CreateDiskFromDevices(
                    TInstant::FromValue(1000),
                    db,
                    "disk-1",
                    MakeMirror2Params(),
                    {},
                    replicaUuids,
                    &result);

                UNIT_ASSERT_VALUES_EQUAL_C(
                    S_OK,
                    error.GetCode(),
                    error.GetMessage());

                UNIT_ASSERT_VALUES_EQUAL(2, result.Devices.size());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-1",
                    result.Devices[0].GetDeviceUUID());
                UNIT_ASSERT_VALUES_EQUAL(
                    "uuid-2",
                    result.Devices[1].GetDeviceUUID());
            });

        {
            TDiskInfo masterInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1", masterInfo));

            UNIT_ASSERT_EQUAL(NProto::DISK_STATE_ONLINE, masterInfo.State);
            UNIT_ASSERT_EQUAL(
                NProto::STORAGE_MEDIA_SSD_MIRROR2,
                masterInfo.MediaKind);
            UNIT_ASSERT_VALUES_EQUAL(
                DefaultLogicalBlockSize,
                masterInfo.LogicalBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(2, masterInfo.Devices.size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-1",
                masterInfo.Devices[0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-2",
                masterInfo.Devices[1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1, masterInfo.Replicas.size());
            UNIT_ASSERT_VALUES_EQUAL(2, masterInfo.Replicas[0].size());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-3",
                masterInfo.Replicas[0][0].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(
                "uuid-4",
                masterInfo.Replicas[0][1].GetDeviceUUID());
            UNIT_ASSERT_VALUES_EQUAL(1000, masterInfo.StateTs.GetValue());
        }

        {
            TDiskInfo replicaInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/0", replicaInfo));
            UNIT_ASSERT_VALUES_EQUAL("disk-1", replicaInfo.MasterDiskId);
        }

        {
            TDiskInfo replicaInfo;
            UNIT_ASSERT_SUCCESS(state.GetDiskInfo("disk-1/1", replicaInfo));
            UNIT_ASSERT_VALUES_EQUAL("disk-1", replicaInfo.MasterDiskId);
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "|uuid-1|uuid-3|"
            "|uuid-2|uuid-4|",
            GetReplicaTableRepr(state, "disk-1"));

        const auto* group = state.FindPlacementGroup("disk-1/g");
        UNIT_ASSERT(group);
        UNIT_ASSERT_VALUES_EQUAL(2, group->DisksSize());
        UNIT_ASSERT_VALUES_EQUAL("disk-1/0", group->GetDisks(0).GetDiskId());
        UNIT_ASSERT_VALUES_EQUAL("disk-1/1", group->GetDisks(1).GetDiskId());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
