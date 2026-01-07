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

    Y_UNIT_TEST(ShouldCalcCorrectShardedIds)
    {
        constexpr ui64 unit64 = 1;
        constexpr ui64 sixBytesMask = (unit64 << (CHAR_BIT * 6U)) - 1U;

        // shardId <= 255, id uses less than 6 bytes
        ui32 shardNo = 0xfa;
        ui64 id = 0xfb0001;
        ui64 shardedId = ShardedId(id, shardNo);
        UNIT_ASSERT(!IsSeventhByteUsed(shardedId));
        UNIT_ASSERT_EQUAL(shardNo, ExtractShardNo(shardedId));
        UNIT_ASSERT_EQUAL(shardedId & sixBytesMask, id);

        // shardId > 255, id uses less than 6 bytes
        shardNo = 0xfa01;
        id = 0xfc0003;
        shardedId = ShardedId(id, shardNo);
        UNIT_ASSERT(IsSeventhByteUsed(shardedId));
        UNIT_ASSERT_EQUAL(shardNo, ExtractShardNo(shardedId));
        UNIT_ASSERT_EQUAL(shardedId & sixBytesMask, id);

        // shardId <= 255, id uses more than 6 bytes
        shardNo = 0xf1;
        id = 0xdf0000000000fd;
        shardedId = ShardedId(id, shardNo);
        UNIT_ASSERT(!IsSeventhByteUsed(shardedId));
        UNIT_ASSERT_EQUAL(shardNo, ExtractShardNo(shardedId));
        UNIT_ASSERT_EQUAL(shardedId & sixBytesMask, id & sixBytesMask);

        // shardId > 255, id uses more than 6 bytes
        shardNo = 0x1c50;
        id = 0xca0000000000df;
        shardedId = ShardedId(id, shardNo);
        UNIT_ASSERT(IsSeventhByteUsed(shardedId));
        UNIT_ASSERT_EQUAL(shardNo, ExtractShardNo(shardedId));
        UNIT_ASSERT_EQUAL(shardedId & sixBytesMask, id & sixBytesMask);
    }
}

}   // namespace NCloud::NFileStore::NStorage
