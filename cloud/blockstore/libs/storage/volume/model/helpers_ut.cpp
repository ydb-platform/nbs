#include "helpers.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TVolumeModelHelpersTest)
{
    Y_UNIT_TEST(ShouldGetAllTagValues)
    {
        const auto values = GetTagValues(
            "key=one,other=value,key=two,key-prefix=ignored,key=one", "key");
        const TVector<TString> expected{"one", "two", "one"};
        UNIT_ASSERT_VALUES_EQUAL(expected, values);
    }

    Y_UNIT_TEST(ShouldGetEmptyTagValuesAndPreserveEquals)
    {
        const auto values = GetTagValues("key,key=,key=value=tail", "key");
        const TVector<TString> expected{"", "", "value=tail"};
        UNIT_ASSERT_VALUES_EQUAL(expected, values);
    }

    Y_UNIT_TEST(ShouldReturnNoValuesForMissingTag)
    {
        UNIT_ASSERT(GetTagValues("", "key").empty());
        UNIT_ASSERT(GetTagValues("other=value,key-prefix=one", "key").empty());
    }
}

}   // namespace NCloud::NBlockStore::NStorage
