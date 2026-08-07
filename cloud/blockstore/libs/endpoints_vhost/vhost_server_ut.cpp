#include "vhost_server.h"

#include <cloud/blockstore/libs/common/constants.h>
#include <cloud/storage/core/protos/media.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NServer {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVhostEndpointTest)
{
    Y_UNIT_TEST(ShouldSetDiscardEnabledOptionCorrectly)
    {
        {
            NProto::TVolume volume;
            volume.SetVhostDiscardEnabled(true);
            volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                ShouldEnableVhostDiscardForVolume(true, volume));
            UNIT_ASSERT_VALUES_EQUAL(
                true,
                ShouldEnableVhostDiscardForVolume(false, volume));
        }

        {
            NProto::TVolume volume;
            volume.SetVhostDiscardEnabled(false);
            volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                ShouldEnableVhostDiscardForVolume(true, volume));
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                ShouldEnableVhostDiscardForVolume(false, volume));
        }

        {
            NProto::TVolume volume;
            volume.SetVhostDiscardEnabled(true);
            volume.SetStorageMediaKind(
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);

            UNIT_ASSERT_VALUES_EQUAL(
                false,
                ShouldEnableVhostDiscardForVolume(true, volume));
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                ShouldEnableVhostDiscardForVolume(false, volume));
        }

        {
            NProto::TVolume volume;
            volume.SetVhostDiscardEnabled(false);
            volume.SetStorageMediaKind(
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);

            UNIT_ASSERT_VALUES_EQUAL(
                false,
                ShouldEnableVhostDiscardForVolume(true, volume));
            UNIT_ASSERT_VALUES_EQUAL(
                false,
                ShouldEnableVhostDiscardForVolume(false, volume));
        }
    }

    Y_UNIT_TEST(ShouldDropDiscardRequestsCorrectly)
    {
        {
            NProto::TVolume volume;
            volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                ShouldDropDiscardRequestsForVolume(true, volume));
        }

        {
            NProto::TVolume volume;
            volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);

            UNIT_ASSERT_VALUES_EQUAL(
                false,
                ShouldDropDiscardRequestsForVolume(false, volume));
        }

        {
            NProto::TVolume volume;
            volume.SetStorageMediaKind(NCloud::NProto::STORAGE_MEDIA_SSD);
            (*volume.MutableTags())[TString(DropDiscardRequestsTagName)] = "";

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                ShouldDropDiscardRequestsForVolume(false, volume));
        }

        {
            NProto::TVolume volume;
            volume.SetStorageMediaKind(
                NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED);

            UNIT_ASSERT_VALUES_EQUAL(
                true,
                ShouldDropDiscardRequestsForVolume(false, volume));
        }
    }

    Y_UNIT_TEST(ShouldSelectThreadCountByMediaKind)
    {
        const TVhostEndpointThreadCounts threadCounts{
            .SSD = 2,
            .HDD = 3,
            .NonReplicated = 4,
            .Mirror2 = 5,
            .Mirror3 = 6};

        auto threadCount = [&] (NCloud::NProto::EStorageMediaKind mediaKind)
        {
            return GetVhostEndpointThreadCount(threadCounts, mediaKind);
        };

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            threadCount(NCloud::NProto::STORAGE_MEDIA_SSD));

        UNIT_ASSERT_VALUES_EQUAL(
            3,
            threadCount(NCloud::NProto::STORAGE_MEDIA_HDD));
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            threadCount(NCloud::NProto::STORAGE_MEDIA_HYBRID));
        UNIT_ASSERT_VALUES_EQUAL(
            3,
            threadCount(NCloud::NProto::STORAGE_MEDIA_DEFAULT));

        UNIT_ASSERT_VALUES_EQUAL(
            4,
            threadCount(NCloud::NProto::STORAGE_MEDIA_SSD_NONREPLICATED));
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            threadCount(NCloud::NProto::STORAGE_MEDIA_HDD_NONREPLICATED));

        UNIT_ASSERT_VALUES_EQUAL(
            5,
            threadCount(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR2));
        UNIT_ASSERT_VALUES_EQUAL(
            6,
            threadCount(NCloud::NProto::STORAGE_MEDIA_SSD_MIRROR3));

        // Local disks are served by a single thread.
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            threadCount(NCloud::NProto::STORAGE_MEDIA_SSD_LOCAL));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            threadCount(NCloud::NProto::STORAGE_MEDIA_HDD_LOCAL));
    }
}

}   // namespace NCloud::NBlockStore::NServer
