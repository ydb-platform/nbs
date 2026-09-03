#include "media.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMediaKindTest)
{
    Y_UNIT_TEST(ShouldRecognizeSsdDirectMirror3Of5GroupMediaKind)
    {
        UNIT_ASSERT(IsSsdDirectMirror3Of5GroupMediaKind(
            NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP));
        UNIT_ASSERT(!IsSsdDirectMirror3Of5GroupMediaKind(
            NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT(!IsDiskRegistryMediaKind(
            NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP));
        UNIT_ASSERT(!IsBlobStorageMediaKind(
            NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP));
        UNIT_ASSERT(IsBlobStorageMediaKind(NProto::STORAGE_MEDIA_SSD));
    }

    Y_UNIT_TEST(ShouldParseAndPrintSsdDirectMirror3Of5GroupMediaKind)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "ssd_direct_mirror3of5_group",
            MediaKindToString(
                NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP));

        NProto::EStorageMediaKind mediaKind;
        UNIT_ASSERT(ParseMediaKind("ssd_direct_mirror3of5_group", &mediaKind));
        UNIT_ASSERT_EQUAL(
            NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP,
            mediaKind);

        UNIT_ASSERT(ParseMediaKind("ssd-direct-mirror3of5-group", &mediaKind));
        UNIT_ASSERT_EQUAL(
            NProto::STORAGE_MEDIA_SSD_DIRECT_MIRROR3OF5_GROUP,
            mediaKind);
    }
}

}   // namespace NCloud
