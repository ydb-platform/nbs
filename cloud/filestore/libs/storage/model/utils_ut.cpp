#include "utils.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>

#include <google/protobuf/repeated_ptr_field.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TUtilsTest)
{
    Y_UNIT_TEST(ShouldRemoveByIndices)
    {
        google::protobuf::RepeatedPtrField<TString> strings;
        strings.Add("aaa");
        strings.Add("bbb");
        strings.Add("ccc");
        strings.Add("ddd");
        strings.Add("eee");

        google::protobuf::RepeatedPtrField<TString> strings2;
        strings2.Add("100");
        strings2.Add("200");
        strings2.Add("300");
        strings2.Add("400");
        strings2.Add("500");

        TVector<ui32> indices{1, 4, 2};
        RemoveByIndices(strings, indices);
        RemoveByIndices(strings2, indices);

        UNIT_ASSERT_VALUES_EQUAL(
            "aaa,ddd",
            JoinRange(",", strings.begin(), strings.end()));

        UNIT_ASSERT_VALUES_EQUAL(
            "100,400",
            JoinRange(",", strings2.begin(), strings2.end()));
    }
}

}   // namespace NCloud::NFileStore::NStorage
