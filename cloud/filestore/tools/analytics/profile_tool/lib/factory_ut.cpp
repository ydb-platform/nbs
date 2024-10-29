#include "factory.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NProfileTool {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFactory)
{
    Y_UNIT_TEST(ShouldNormalizeCommand)
    {
        UNIT_ASSERT_VALUES_EQUAL(
            "testcommand",
            NormalizeCommand("TeSt-CoM_Mand"));
    }

    Y_UNIT_TEST(ShouldGetCommand)
    {
        UNIT_ASSERT(!GetCommand("non-existing"));

        UNIT_ASSERT(GetCommand("dumpevents"));
        UNIT_ASSERT(GetCommand("findbytesaccess"));
    }

    Y_UNIT_TEST(ShouldGetCommandNames)
    {
        const auto names = GetCommandNames();

        UNIT_ASSERT_VALUES_EQUAL(3, names.size());

        UNIT_ASSERT_VALUES_EQUAL("dumpevents", names[0]);
        UNIT_ASSERT_VALUES_EQUAL("findbytesaccess", names[1]);
        UNIT_ASSERT_VALUES_EQUAL("masksensitivedata", names[2]);
    }
}

}   // namespace NCloud::NFileStore::NProfileTool
