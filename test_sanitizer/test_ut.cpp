#include <library/cpp/testing/unittest/registar.h>

#include <util/system/tempfile.h>
#include <iostream>


namespace NCloud::NStorage {

Y_UNIT_TEST_SUITE(TSomeTestSuite)
{
    Y_UNIT_TEST(ShouldAllocate100MB)
    {
        auto arr = std::vector<char>(100 * 1024 * 1024);
    }
}

}   // namespace NCloud::NStorage
