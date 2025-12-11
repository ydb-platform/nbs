#include "fresh_blob_test.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFreshBlob)
{
    Y_UNIT_TEST(ShouldRestoreFreshBlocks)
    {
        constexpr ui64 commitId = 1234;
        constexpr bool isStoredInDb = false;

        for (const ui32 blockSize: {4096, 4096 * 4, 4096 * 16}) {
            const auto buffers = GetBuffers(blockSize);
            const auto blockRanges = GetBlockRanges();
            const auto blockIndices = GetBlockIndices(blockRanges);
            const auto holders = GetHolders(buffers);

            const auto blobContent =
                BuildWriteFreshBlocksBlobContent(blockRanges, holders);

            TVector<TOwningFreshBlock> result;
            auto error =
                ParseFreshBlobContent(commitId, blockSize, blobContent, result);

            UNIT_ASSERT(SUCCEEDED(error.GetCode()));
            UNIT_ASSERT_VALUES_EQUAL(15, result.size());

            auto subBuffer = buffers.begin();
            auto buffer = subBuffer->begin();
            auto blockIndex = blockIndices.begin();

            for (ui32 i = 0; i < result.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(*buffer, result[i].Content);

                const auto& block = result[i].Meta;
                UNIT_ASSERT_VALUES_EQUAL(*blockIndex, block.BlockIndex);
                UNIT_ASSERT_VALUES_EQUAL(commitId, block.CommitId);

                UNIT_ASSERT_VALUES_EQUAL(isStoredInDb, block.IsStoredInDb);

                if (++buffer == subBuffer->end()) {
                    if (++subBuffer != buffers.end()) {
                        buffer = subBuffer->begin();
                    }
                }

                ++blockIndex;
            }
        }
    }

    Y_UNIT_TEST(ShouldRestoreZeroedFreshBlocks)
    {
        constexpr ui64 commitId = 1234;
        constexpr bool isStoredInDb = false;
        constexpr ui32 blockSize = 4;

        const TString blobContent =
            BuildZeroFreshBlocksBlobContent(ZeroFreshBlocksRange);

        TVector<TOwningFreshBlock> result;
        auto error =
            ParseFreshBlobContent(commitId, blockSize, blobContent, result);

        UNIT_ASSERT(SUCCEEDED(error.GetCode()));
        UNIT_ASSERT_VALUES_EQUAL(5, result.size());

        for (ui32 i = 0; i < result.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(TString{}, result[i].Content);

            const auto& block = result[i].Meta;
            UNIT_ASSERT_VALUES_EQUAL(i, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.CommitId);
            UNIT_ASSERT_VALUES_EQUAL(isStoredInDb, block.IsStoredInDb);
        }
    }

    Y_UNIT_TEST(SerializationShouldBeForwardAndBackwardCompatibleWrite)
    {
        constexpr ui64 commitId = 1234;
        constexpr ui32 blockSize = 4096;
        constexpr bool isStoredInDb = false;

        const auto buffers = GetBuffers(blockSize);
        const auto blockRanges = GetBlockRanges();
        const auto blockIndices = GetBlockIndices(blockRanges);
        const auto holders = GetHolders(buffers);

        auto oldBlobContent = NResource::Find("fresh_write.blob");

        TVector<TOwningFreshBlock> result;
        auto error =
            ParseFreshBlobContent(commitId, blockSize, oldBlobContent, result);

        UNIT_ASSERT(SUCCEEDED(error.GetCode()));
        UNIT_ASSERT_VALUES_EQUAL(15, result.size());

        auto subBuffer = buffers.begin();
        auto buffer = subBuffer->begin();
        auto blockIndex = blockIndices.begin();

        for (ui32 i = 0; i < result.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(*buffer, result[i].Content);

            const auto& block = result[i].Meta;
            UNIT_ASSERT_VALUES_EQUAL(*blockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.CommitId);

            UNIT_ASSERT_VALUES_EQUAL(isStoredInDb, block.IsStoredInDb);

            if (++buffer == subBuffer->end()) {
                if (++subBuffer != buffers.end()) {
                    buffer = subBuffer->begin();
                }
            }

            ++blockIndex;
        }

        auto newBlobContent =
            BuildWriteFreshBlocksBlobContent(blockRanges, holders);

        UNIT_ASSERT_VALUES_EQUAL(oldBlobContent.size(), newBlobContent.size());
        for (size_t i = 0; i < oldBlobContent.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(oldBlobContent[i], newBlobContent[i]);
        }
    }

    Y_UNIT_TEST(SerializationShouldBeForwardAndBackwardCompatibleZero)
    {
        constexpr ui64 commitId = 1234;
        constexpr bool isStoredInDb = false;
        constexpr ui32 blockSize = 4;

        auto oldBlobContent = NResource::Find("fresh_zero.blob");

        TVector<TOwningFreshBlock> result;
        auto error =
            ParseFreshBlobContent(commitId, blockSize, oldBlobContent, result);

        UNIT_ASSERT(SUCCEEDED(error.GetCode()));
        UNIT_ASSERT_VALUES_EQUAL(5, result.size());

        for (ui32 i = 0; i < result.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(TString{}, result[i].Content);

            const auto& block = result[i].Meta;
            UNIT_ASSERT_VALUES_EQUAL(i, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.CommitId);
            UNIT_ASSERT_VALUES_EQUAL(isStoredInDb, block.IsStoredInDb);
        }

        auto newBlobContent =
            BuildZeroFreshBlocksBlobContent(ZeroFreshBlocksRange);

        UNIT_ASSERT_VALUES_EQUAL(oldBlobContent.size(), newBlobContent.size());
        for (size_t i = 0; i < oldBlobContent.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(oldBlobContent[i], newBlobContent[i]);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
