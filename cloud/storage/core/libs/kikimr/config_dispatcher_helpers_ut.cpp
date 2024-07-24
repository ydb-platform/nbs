#include "config_dispatcher_helpers.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>

namespace NCloud {

using namespace NStorage;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TConfigDispatcherHelpersTest)
{
    Y_UNIT_TEST(ShouldFailIfCannotParseConfigDispatcherEkind)
    {
        auto [value, error] = ParseConfigDispatcherItems({"xyz"});

        UNIT_ASSERT(HasError(error));
    }

    Y_UNIT_TEST(ShouldParseConfigDispatcherEkind)
    {
        auto [value, error] = ParseConfigDispatcherItems({"xyz"});

        UNIT_ASSERT(HasError(error));
    }
}

}   // namespace NCloud
