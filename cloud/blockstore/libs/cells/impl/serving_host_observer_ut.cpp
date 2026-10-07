#include <cloud/blockstore/libs/cells/iface/serving_host_observer.h>

#include <cloud/blockstore/libs/diagnostics/volume_stats.h>

#include <cloud/storage/core/libs/common/timer.h>
#include <cloud/storage/core/libs/diagnostics/monitoring.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NCells {

Y_UNIT_TEST_SUITE(TServingCellHostObserverTest)
{
    Y_UNIT_TEST(ShouldShowServingHostWhileAttached)
    {
        const TString diskId = "disk";
        const TString clientId = "client";
        const TString instanceId = "instance";

        auto monitoring = CreateMonitoringServiceStub();
        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        NProto::TVolume volume;
        volume.SetDiskId(diskId);
        volume.SetCloudId("cloud");
        volume.SetFolderId("folder");
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
        volumeStats->MountVolume(volume, clientId, instanceId);

        auto cellMount = [&] (const TString& fqdn) -> i64 {
            auto cell = monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", diskId)
                ->GetSubgroup("instance", instanceId)
                ->GetSubgroup("cloud", "cloud")
                ->GetSubgroup("folder", "folder")
                ->GetSubgroup("type", "ssd")
                ->FindSubgroup("cell", "cell");
            auto host = cell ? cell->FindSubgroup("cell_host", fqdn) : nullptr;
            auto counter = host ? host->FindCounter("CellMount") : nullptr;
            return counter ? counter->Val() : 0;
        };

        auto observer =
            CreateServingCellHostObserver(volumeStats, "cell", clientId);

        // reported on connect, before the mount: kept, not shown yet
        observer->OnServingHostChanged("host-1");
        UNIT_ASSERT_VALUES_EQUAL(0, cellMount("host-1"));

        observer->Attach(diskId);
        UNIT_ASSERT_VALUES_EQUAL(1, cellMount("host-1"));

        observer->OnServingHostChanged("host-2");
        UNIT_ASSERT_VALUES_EQUAL(0, cellMount("host-1"));
        UNIT_ASSERT_VALUES_EQUAL(1, cellMount("host-2"));

        observer->Detach();
        UNIT_ASSERT_VALUES_EQUAL(0, cellMount("host-2"));

        // detached: further moves show nothing until attached again
        observer->OnServingHostChanged("host-3");
        UNIT_ASSERT_VALUES_EQUAL(0, cellMount("host-3"));
    }

    Y_UNIT_TEST(ShouldKeepServingHostWhileAnotherEndpointIsAttached)
    {
        auto monitoring = CreateMonitoringServiceStub();
        auto volumeStats = CreateVolumeStats(
            monitoring,
            {},
            EVolumeStatsType::EServerStats,
            CreateWallClockTimer());

        NProto::TVolume volume;
        volume.SetDiskId("disk");
        volume.SetCloudId("cloud");
        volume.SetFolderId("folder");
        volume.SetStorageMediaKind(NProto::STORAGE_MEDIA_SSD);
        volumeStats->MountVolume(volume, "client", "instance");

        auto cellMount = [&] () -> i64 {
            auto cell = monitoring->GetCounters()
                ->GetSubgroup("counters", "blockstore")
                ->GetSubgroup("component", "server_volume")
                ->GetSubgroup("host", "cluster")
                ->GetSubgroup("volume", "disk")
                ->GetSubgroup("instance", "instance")
                ->GetSubgroup("cloud", "cloud")
                ->GetSubgroup("folder", "folder")
                ->GetSubgroup("type", "ssd")
                ->FindSubgroup("cell", "cell");
            auto host = cell ? cell->FindSubgroup("cell_host", "host-1")
                             : nullptr;
            auto counter = host ? host->FindCounter("CellMount") : nullptr;
            return counter ? counter->Val() : 0;
        };

        // a local VM migration: the old and the new endpoint of the same disk
        // and client, each with its own connection
        auto oldEndpoint =
            CreateServingCellHostObserver(volumeStats, "cell", "client");
        auto newEndpoint =
            CreateServingCellHostObserver(volumeStats, "cell", "client");

        oldEndpoint->OnServingHostChanged("host-1");
        oldEndpoint->Attach("disk");
        newEndpoint->OnServingHostChanged("host-1");
        newEndpoint->Attach("disk");
        UNIT_ASSERT_VALUES_EQUAL(1, cellMount());

        oldEndpoint->Detach();
        UNIT_ASSERT_VALUES_EQUAL(1, cellMount());
    }
}

}   // namespace NCloud::NBlockStore::NCells
