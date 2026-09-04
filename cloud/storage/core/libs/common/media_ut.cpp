#include "media.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMediaKindTest)
{
    Y_UNIT_TEST(ShouldRecognizeNbs2MediaKind)
    {
        UNIT_ASSERT(IsNbs2MediaKind(NProto::STORAGE_MEDIA_SSD_NBS2));
        UNIT_ASSERT(!IsNbs2MediaKind(NProto::STORAGE_MEDIA_SSD));
        UNIT_ASSERT(!IsDiskRegistryMediaKind(NProto::STORAGE_MEDIA_SSD_NBS2));
        UNIT_ASSERT(!IsBlobStorageMediaKind(NProto::STORAGE_MEDIA_SSD_NBS2));
        UNIT_ASSERT(IsBlobStorageMediaKind(NProto::STORAGE_MEDIA_SSD));
    }

    Y_UNIT_TEST(ShouldParseAndPrintNbs2MediaKind)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "ssd_nbs2",
            MediaKindToString(NProto::STORAGE_MEDIA_SSD_NBS2));

        NProto::EStorageMediaKind mediaKind;
        UNIT_ASSERT(ParseMediaKind("ssd_nbs2", &mediaKind));
        UNIT_ASSERT_EQUAL(NProto::STORAGE_MEDIA_SSD_NBS2, mediaKind);

        UNIT_ASSERT(ParseMediaKind("ssd-nbs2", &mediaKind));
        UNIT_ASSERT_EQUAL(NProto::STORAGE_MEDIA_SSD_NBS2, mediaKind);
    }
}

}   // namespace NCloud
