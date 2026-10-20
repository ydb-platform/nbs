#include <cloud/filestore/libs/storage/tablet/model/block.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

Y_UNIT_TEST_SUITE(TBlockGroupTest)
{
    Y_UNIT_TEST(ShouldPreserveLegacyGrouping)
    {
        for (ui32 type: {0, 1}) {
            auto legacy = CreateRangeIdHasher(type);
            auto absent = CreateRangeIdHasher(type, 0);
            auto explicitLegacy = CreateRangeIdHasher(type, 64);
            for (ui32 index: {0, 63, 64, 255, 256, 1024}) {
                UNIT_ASSERT_VALUES_EQUAL(
                    legacy->Calc(123, index),
                    absent->Calc(123, index));
                UNIT_ASSERT_VALUES_EQUAL(
                    legacy->Calc(123, index),
                    explicitLegacy->Calc(123, index));
            }
            UNIT_ASSERT(legacy->Calc(123, 63) != legacy->Calc(123, 64));
        }
    }

    Y_UNIT_TEST(ShouldUseConfiguredBlockGroupSize)
    {
        for (ui32 type: {0, 1}) {
            auto hasher = CreateRangeIdHasher(type, 256);
            UNIT_ASSERT_VALUES_EQUAL(
                hasher->Calc(123, 0),
                hasher->Calc(123, 255));
            UNIT_ASSERT(hasher->Calc(123, 255) != hasher->Calc(123, 256));
        }
    }

    Y_UNIT_TEST(ShouldValidateBlockGroupSize)
    {
        for (ui32 size: {1, 64, 256, 1024}) {
            UNIT_ASSERT(IsValidBlockGroupSize(size));
        }
        for (ui32 size: {0, 3, 255, 1025, 2048}) {
            UNIT_ASSERT(!IsValidBlockGroupSize(size));
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
