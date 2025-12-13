#include "vhost_server.h"

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
}

}   // namespace NCloud::NBlockStore::NServer
