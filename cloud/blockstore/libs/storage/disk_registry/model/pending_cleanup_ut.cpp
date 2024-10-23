#include "pending_cleanup.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPendingCleanupTest)
{
    Y_UNIT_TEST(ShouldPurgeDisk)
    {
        TPendingCleanup cleanup;

        UNIT_ASSERT(cleanup.IsEmpty());
        auto error = cleanup.Insert("foo", TVector<TString> {});
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        UNIT_ASSERT(cleanup.IsEmpty());
        UNIT_ASSERT(!cleanup.Contains("foo"));

        error = cleanup.Insert("foo", "");
        UNIT_ASSERT_VALUES_EQUAL(E_ARGUMENT, error.GetCode());
        UNIT_ASSERT(cleanup.IsEmpty());
        UNIT_ASSERT(!cleanup.Contains("foo"));

        error = cleanup.Insert("foo", TVector<TString> {"x", "y"});
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT(!cleanup.IsEmpty());
        UNIT_ASSERT(cleanup.Contains("foo"));

        UNIT_ASSERT_VALUES_EQUAL("foo", cleanup.FindDiskId("x"));
        UNIT_ASSERT_VALUES_EQUAL("foo", cleanup.FindDiskId("y"));
        UNIT_ASSERT_VALUES_EQUAL("", cleanup.FindDiskId("z"));
        UNIT_ASSERT_VALUES_EQUAL("", cleanup.FindDiskId("w"));

        error = cleanup.Insert("foo", "z");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL("foo", cleanup.FindDiskId("z"));

        UNIT_ASSERT_VALUES_EQUAL("", cleanup.EraseDevice("w"));

        UNIT_ASSERT_VALUES_EQUAL("", cleanup.EraseDevice("x"));
        UNIT_ASSERT_VALUES_EQUAL("", cleanup.FindDiskId("x"));
        UNIT_ASSERT(!cleanup.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL("", cleanup.EraseDevice("y"));
        UNIT_ASSERT_VALUES_EQUAL("", cleanup.FindDiskId("y"));
        UNIT_ASSERT(!cleanup.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL("foo", cleanup.EraseDevice("z"));
        UNIT_ASSERT_VALUES_EQUAL("", cleanup.FindDiskId("z"));

        UNIT_ASSERT(cleanup.IsEmpty());
        UNIT_ASSERT(!cleanup.Contains("foo"));

        error = cleanup.Insert("bar", "x");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = cleanup.Insert("bar", "y");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = cleanup.Insert("bar", "z");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        UNIT_ASSERT(!cleanup.IsEmpty());
        UNIT_ASSERT(cleanup.Contains("bar"));
        UNIT_ASSERT_VALUES_EQUAL("bar", cleanup.FindDiskId("x"));
        UNIT_ASSERT_VALUES_EQUAL("bar", cleanup.FindDiskId("y"));
        UNIT_ASSERT_VALUES_EQUAL("bar", cleanup.FindDiskId("z"));

        UNIT_ASSERT(!cleanup.EraseDisk("foo"));
        UNIT_ASSERT(cleanup.EraseDisk("bar"));
        UNIT_ASSERT(cleanup.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL("", cleanup.FindDiskId("x"));
        UNIT_ASSERT_VALUES_EQUAL("", cleanup.FindDiskId("y"));
        UNIT_ASSERT_VALUES_EQUAL("", cleanup.FindDiskId("z"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
