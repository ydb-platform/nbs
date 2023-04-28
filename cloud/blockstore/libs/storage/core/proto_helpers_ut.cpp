#include "proto_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

void InitDev(const TString& uuid, NProto::TDeviceConfig* d)
{
    d->SetDeviceUUID(uuid);
    d->SetDeviceName(uuid + "-n");
    d->SetTransportId(uuid + "-t");
    d->SetBlockSize(DefaultBlockSize);
    d->SetBlocksCount(100);
}

}   // namespace

Y_UNIT_TEST_SUITE(TProtoHelpersTest)
{
    Y_UNIT_TEST(TestFillDeviceInfo)
    {

        google::protobuf::RepeatedPtrField<NProto::TDeviceConfig> devices;
        InitDev("uuid-1-1", devices.Add());
        InitDev("uuid-1-2", devices.Add());
        InitDev("uuid-1-3", devices.Add());

        google::protobuf::RepeatedPtrField<NProto::TReplica> replicas;
        auto* r1 = replicas.Add();
        InitDev("uuid-2-1", r1->AddDevices());
        InitDev("uuid-2-2", r1->AddDevices());
        InitDev("uuid-2-3", r1->AddDevices());

        auto* r2 = replicas.Add();
        InitDev("uuid-3-1", r2->AddDevices());
        InitDev("uuid-3-2", r2->AddDevices());
        InitDev("uuid-3-3", r2->AddDevices());

        google::protobuf::RepeatedPtrField<TString> freshDeviceIds;
        *freshDeviceIds.Add() = "uuid-1-1";
        *freshDeviceIds.Add() = "uuid-2-3";

        google::protobuf::RepeatedPtrField<NProto::TDeviceMigration> migrations;
        auto* m = migrations.Add();
        m->SetSourceDeviceId("uuid-1-2");
        InitDev("uuid-1-2-m", m->MutableTargetDevice());
        m = migrations.Add();
        m->SetSourceDeviceId("uuid-1-3");
        InitDev("uuid-1-3-m", m->MutableTargetDevice());
        m = migrations.Add();
        m->SetSourceDeviceId("uuid-2-1");
        InitDev("uuid-2-1-m", m->MutableTargetDevice());
        m = migrations.Add();
        m->SetSourceDeviceId("uuid-3-2");
        InitDev("uuid-3-2-m", m->MutableTargetDevice());

        NProto::TVolume volume;
        FillDeviceInfo(devices, migrations, replicas, freshDeviceIds, volume);

        UNIT_ASSERT_VALUES_EQUAL(3, volume.DevicesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-1",
            volume.GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-2",
            volume.GetDevices(1).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-3",
            volume.GetDevices(2).GetDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL(2, volume.ReplicasSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-1",
            volume.GetReplicas(0).GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-2",
            volume.GetReplicas(0).GetDevices(1).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-3",
            volume.GetReplicas(0).GetDevices(2).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-1",
            volume.GetReplicas(1).GetDevices(0).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-2",
            volume.GetReplicas(1).GetDevices(1).GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-3",
            volume.GetReplicas(1).GetDevices(2).GetDeviceUUID());

        UNIT_ASSERT_VALUES_EQUAL(2, volume.FreshDeviceIdsSize());
        UNIT_ASSERT_VALUES_EQUAL("uuid-1-1", volume.GetFreshDeviceIds(0));
        UNIT_ASSERT_VALUES_EQUAL("uuid-2-3", volume.GetFreshDeviceIds(1));

        UNIT_ASSERT_VALUES_EQUAL(4, volume.MigrationsSize());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-2",
            volume.GetMigrations(0).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-2-t",
            volume.GetMigrations(0).GetSourceTransportId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-2-m",
            volume.GetMigrations(0).GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-3",
            volume.GetMigrations(1).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-3-t",
            volume.GetMigrations(1).GetSourceTransportId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-1-3-m",
            volume.GetMigrations(1).GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-1",
            volume.GetMigrations(2).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-1-t",
            volume.GetMigrations(2).GetSourceTransportId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-2-1-m",
            volume.GetMigrations(2).GetTargetDevice().GetDeviceUUID());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-2",
            volume.GetMigrations(3).GetSourceDeviceId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-2-t",
            volume.GetMigrations(3).GetSourceTransportId());
        UNIT_ASSERT_VALUES_EQUAL(
            "uuid-3-2-m",
            volume.GetMigrations(3).GetTargetDevice().GetDeviceUUID());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
