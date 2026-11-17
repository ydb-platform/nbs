#include "block.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/digest/numeric.h>

namespace NCloud::NFileStore::NStorage {

namespace {

////////////////////////////////////////////////////////////////////////////////

const ui64 NodeIds[] = {1, 2, 15, 16, 17, 1000, 123456789, Max<ui32>() + 5ULL};
const ui32 BlockIndices[] =
    {0, 1, 63, 64, 65, 255, 256, 4096, 1u << 22, Max<ui32>()};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TRangeIdHasherTest)
{
    Y_UNIT_TEST(ShouldKeepLegacyLayouts)
    {
        auto legacy = CreateRangeIdHasher(0);
        auto locality = CreateRangeIdHasher(1);
        UNIT_ASSERT(legacy);
        UNIT_ASSERT(locality);
        UNIT_ASSERT_VALUES_EQUAL(BlockGroupSize, legacy->GetBlockGroupSize());
        UNIT_ASSERT_VALUES_EQUAL(BlockGroupSize, locality->GetBlockGroupSize());

        for (ui64 nodeId: NodeIds) {
            for (ui32 blockIndex: BlockIndices) {
                const ui32 nodeGroup = static_cast<ui32>(nodeId) / 16;
                const ui32 blockGroup = blockIndex / 64;

                UNIT_ASSERT_VALUES_EQUAL(
                    IntHash(nodeGroup) ^ IntHash(blockGroup),
                    legacy->Calc(nodeId, blockIndex));
                UNIT_ASSERT_VALUES_EQUAL(
                    (0xFFFF0000 & IntHash(nodeGroup)) | (0xFFFF & blockGroup),
                    locality->Calc(nodeId, blockIndex));
            }
        }
    }

    Y_UNIT_TEST(ShouldGroupBlocksByRangeBlockGroupSize)
    {
        const std::pair<ui32, ui32> layouts[] = {{2, 128}, {3, 256}};
        for (auto [type, group]: layouts) {
            auto hasher = CreateRangeIdHasher(type);
            UNIT_ASSERT(hasher);
            UNIT_ASSERT_VALUES_EQUAL(group, hasher->GetBlockGroupSize());

            for (ui64 nodeId: NodeIds) {
                for (ui32 blockIndex: BlockIndices) {
                    const ui32 nodeGroup = static_cast<ui32>(nodeId) / 16;
                    const ui32 blockGroup = blockIndex / group;
                    UNIT_ASSERT_VALUES_EQUAL(
                        (0xFFFF0000 & IntHash(nodeGroup)) |
                            (0xFFFF & blockGroup),
                        hasher->Calc(nodeId, blockIndex));
                }

                const ui32 first = hasher->Calc(nodeId, group);
                UNIT_ASSERT_VALUES_EQUAL(
                    first,
                    hasher->Calc(nodeId, 2 * group - 1));
                UNIT_ASSERT_VALUES_UNEQUAL(
                    first,
                    hasher->Calc(nodeId, group - 1));
                UNIT_ASSERT_VALUES_UNEQUAL(
                    first,
                    hasher->Calc(nodeId, 2 * group));
            }
        }
    }

    Y_UNIT_TEST(ShouldRejectUnknownType)
    {
        UNIT_ASSERT(!CreateRangeIdHasher(4));
        UNIT_ASSERT(!CreateRangeIdHasher(7));
        UNIT_ASSERT(!CreateRangeIdHasher(Max<ui32>()));
    }
}

}   // namespace NCloud::NFileStore::NStorage
