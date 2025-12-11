#include "partial_blob_id.h"

#include "commit.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartialBlobIdTest)
{
    Y_UNIT_TEST(ShouldCorrectlySplitBlobId)
    {
        TPartialBlobId blobId(MakeCommitId(1, 2), 0);
        UNIT_ASSERT_EQUAL(blobId.Generation(), 1);
        UNIT_ASSERT_EQUAL(blobId.Step(), 2);
    }
}

}   // namespace NCloud::NStorage
