#include "pending_cleanup.h"

#include <library/cpp/testing/unittest/registar.h>

using TOpt2Disk = NCloud::NBlockStore::NStorage::TPendingCleanup::TOpt2Disk;

IOutputStream& operator<<(IOutputStream& os, const TOpt2Disk& pair)
{
    os << "{ '" << pair.AllocatingDiskId << "', '" << pair.DeallocatingDiskId
       << "'}";
    return os;
}

namespace NCloud::NBlockStore::NStorage {

bool operator==(const TOpt2Disk& lhs, const TOpt2Disk& rhs)
{
    return lhs.AllocatingDiskId == rhs.AllocatingDiskId &&
           lhs.DeallocatingDiskId == rhs.DeallocatingDiskId;
}

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

        error = cleanup.Insert("bar", TVector<TString> {"x", "z"}, /*allocation=*/true);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT(cleanup.Contains("bar"));

        using TOpt2Disk = TPendingCleanup::TOpt2Disk;
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("bar", "foo"), cleanup.FindDiskId("x"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("", "foo"), cleanup.FindDiskId("y"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("bar", ""), cleanup.FindDiskId("z"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("a"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("b"));

        error = cleanup.Insert("foo", "z");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("bar", "foo"), cleanup.FindDiskId("z"));

        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.EraseDevice("w"));

        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.EraseDevice("x"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("x"));
        UNIT_ASSERT(!cleanup.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.EraseDevice("y"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("y"));
        UNIT_ASSERT(!cleanup.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("bar", "foo"), cleanup.EraseDevice("z"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("z"));

        UNIT_ASSERT(cleanup.IsEmpty());
        UNIT_ASSERT(!cleanup.Contains("foo"));

        error = cleanup.Insert("foo", "a", /*allocation=*/true);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = cleanup.Insert("foo", "y", /*allocation=*/true);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        error = cleanup.Insert("bar", "x");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = cleanup.Insert("bar", "y");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());
        error = cleanup.Insert("bar", "z");
        UNIT_ASSERT_VALUES_EQUAL(S_OK, error.GetCode());

        UNIT_ASSERT(!cleanup.IsEmpty());
        UNIT_ASSERT(cleanup.Contains("bar"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("foo", "bar"), cleanup.FindDiskId("y"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("foo", ""), cleanup.FindDiskId("a"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("", "bar"), cleanup.FindDiskId("x"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk("", "bar"), cleanup.FindDiskId("z"));

        UNIT_ASSERT(!cleanup.EraseDisk("baz"));
        UNIT_ASSERT(cleanup.EraseDisk("foo"));
        UNIT_ASSERT(cleanup.EraseDisk("bar"));
        UNIT_ASSERT(cleanup.IsEmpty());

        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("a"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("y"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("x"));
        UNIT_ASSERT_VALUES_EQUAL(TOpt2Disk(), cleanup.FindDiskId("z"));
    }
}

}   // namespace NCloud::NBlockStore::NStorage
