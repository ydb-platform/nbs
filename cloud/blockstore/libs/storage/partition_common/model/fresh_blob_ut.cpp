#include "fresh_blob_test.h"

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFreshBlob)
{
    Y_UNIT_TEST(ShouldRestoreFreshBlocks)
    {
        constexpr ui64 commitId = 1234;
        constexpr bool isStoredInDb = false;
        const auto writeTimestamp = TInstant::MicroSeconds(1234567);

        for (const ui32 blockSize: { 4096, 4096 * 4, 4096 * 16 }) {
            const auto buffers = GetBuffers(blockSize);
            const auto blockRanges = GetBlockRanges();
            const auto blockIndices = GetBlockIndices(blockRanges);
            const auto holders = GetHolders(buffers);

            const auto blobContent = BuildWriteFreshBlocksBlobContent(
                blockRanges,
                holders,
                writeTimestamp);

            TVector<TOwningFreshBlock> result;
            TInstant timestamp;
            auto error = ParseFreshBlobContent(
                commitId,
                {},   // BlobId
                blockSize,
                blobContent,
                result,
                timestamp);

            UNIT_ASSERT(SUCCEEDED(error.GetCode()));
            UNIT_ASSERT_VALUES_EQUAL(writeTimestamp, timestamp);
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
        const auto writeTimestamp = TInstant::MicroSeconds(1234567);

        const TString blobContent = BuildZeroFreshBlocksBlobContent(
            ZeroFreshBlocksRange,
            writeTimestamp);

        TVector<TOwningFreshBlock> result;
        TInstant timestamp;
        auto error = ParseFreshBlobContent(
            commitId,
            {},   // BlobId
            blockSize,
            blobContent,
            result,
            timestamp);

        UNIT_ASSERT(SUCCEEDED(error.GetCode()));
        UNIT_ASSERT_VALUES_EQUAL(writeTimestamp, timestamp);
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
        TInstant timestamp = TInstant::Max();
        auto error = ParseFreshBlobContent(
            commitId,
            {},   // BlobId
            blockSize,
            oldBlobContent,
            result,
            timestamp);

        UNIT_ASSERT(SUCCEEDED(error.GetCode()));
        // Old blobs have no timestamp.
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(), timestamp);
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

        // A blob without a timestamp has exactly the old layout.
        auto newBlobContent = BuildWriteFreshBlocksBlobContent(
            blockRanges,
            holders,
            TInstant::Zero());

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
        TInstant timestamp = TInstant::Max();
        auto error = ParseFreshBlobContent(
            commitId,
            {},   // BlobId
            blockSize,
            oldBlobContent,
            result,
            timestamp);

        UNIT_ASSERT(SUCCEEDED(error.GetCode()));
        // Old blobs have no timestamp.
        UNIT_ASSERT_VALUES_EQUAL(TInstant::Zero(), timestamp);
        UNIT_ASSERT_VALUES_EQUAL(5, result.size());

        for (ui32 i = 0; i < result.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(TString{}, result[i].Content);

            const auto& block = result[i].Meta;
            UNIT_ASSERT_VALUES_EQUAL(i, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.CommitId);
            UNIT_ASSERT_VALUES_EQUAL(isStoredInDb, block.IsStoredInDb);
        }

        // A blob without a timestamp has exactly the old layout.
        auto newBlobContent = BuildZeroFreshBlocksBlobContent(
            ZeroFreshBlocksRange,
            TInstant::Zero());

        UNIT_ASSERT_VALUES_EQUAL(oldBlobContent.size(), newBlobContent.size());
        for (size_t i = 0; i < oldBlobContent.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(oldBlobContent[i], newBlobContent[i]);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage
