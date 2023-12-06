#include "blob_id.h"

#include <ydb/core/base/tablet.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud {

using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartialBlobIdTest)
{
    Y_UNIT_TEST(ShouldCorrectlySplitBlobId)
    {
        {
            TPartialBlobId blobId = MakePartialBlobId(TLogoBlobID(1, 2, 3, 4, 5, 6));
            UNIT_ASSERT_EQUAL(blobId.Generation(), 2);
            UNIT_ASSERT_EQUAL(blobId.Step(), 3);
            UNIT_ASSERT_EQUAL(blobId.Channel(), 4);
            UNIT_ASSERT_EQUAL(blobId.BlobSize(), 5);
            UNIT_ASSERT_EQUAL(blobId.Cookie(), 6);
        }

        {
            TLogoBlobID blobId = MakeBlobId(1, TPartialBlobId(2, 3));
            UNIT_ASSERT_EQUAL(blobId.TabletID(), 1);
        }
    }

    Y_UNIT_TEST(ShouldUseBlobSizeAsDeletionMarkerIndicator)
    {
        {
            TPartialBlobId blobId = MakePartialBlobId(TLogoBlobID(1, 2, 3, 4, 5, 6));
            UNIT_ASSERT(!IsDeletionMarker(blobId));
        }

        {
            TPartialBlobId blobId = MakePartialBlobId(TLogoBlobID(1, 2, 3, 4, 0, 6));
            UNIT_ASSERT(IsDeletionMarker(blobId));
        }
    }
}

}   // namespace NCloud
