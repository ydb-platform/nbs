#include "fresh_blob_test.h"

#include <cloud/storage/core/libs/tablet/model/commit.h>

#include <library/cpp/resource/resource.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFreshBlob)
{
    Y_UNIT_TEST(ShouldRestoreFreshBlocks)
    {
        constexpr ui64 commitId = 1234;

        for (const ui32 blockSize: {4096, 4096 * 4, 4096 * 16}) {
            const auto buffers = GetBuffers(blockSize);
            const auto blockRanges = GetBlockRanges();
            const auto blockIndices = GetBlockIndices(blockRanges);
            const auto holders = GetHolders(buffers);

            const auto blobContent = BuildFreshBlobContent(
                blockRanges,
                holders,
                FirstRequestDeletionId);

            TVector<TOwningFreshBlock> result;
            TBlobUpdatesByFresh blobUpdatesByFresh;

            auto error = ParseFreshBlobContent(
                commitId,
                blockSize,
                blobContent,
                result,
                blobUpdatesByFresh);

            UNIT_ASSERT(SUCCEEDED(error.GetCode()));
            UNIT_ASSERT_VALUES_EQUAL(15, result.size());

            auto subBuffer = buffers.begin();
            auto buffer = subBuffer->begin();
            auto blockIndex = blockIndices.begin();

            for (ui32 i = 0; i < result.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(*buffer, result[i].Content);

                const auto& block = result[i].Meta;
                UNIT_ASSERT_VALUES_EQUAL(*blockIndex, block.BlockIndex);
                UNIT_ASSERT_VALUES_EQUAL(commitId, block.MinCommitId);
                UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, block.MaxCommitId);

                if (++buffer == subBuffer->end()) {
                    if (++subBuffer != buffers.end()) {
                        buffer = subBuffer->begin();
                    }
                }

                ++blockIndex;
            }

            TVector<TBlobUpdate> updates;
            updates.assign(
                blobUpdatesByFresh.begin(),
                blobUpdatesByFresh.end());
            Sort(
                updates,
                [](const auto& lhs, const auto& rhs)
                { return lhs.DeletionId < rhs.DeletionId; });

            UNIT_ASSERT_VALUES_EQUAL(4, updates.size());

            auto deletionId = FirstRequestDeletionId;
            auto blockRange = blockRanges.begin();

            for (const auto update: updates) {
                UNIT_ASSERT_VALUES_EQUAL(deletionId, update.DeletionId);
                UNIT_ASSERT_VALUES_EQUAL(commitId, update.CommitId);
                UNIT_ASSERT_VALUES_EQUAL(*blockRange, update.BlockRange);

                ++deletionId;
                ++blockRange;
            }
        }
    }

    Y_UNIT_TEST(SerializationShouldBeForwardAndBackwardCompatible)
    {
        constexpr ui64 commitId = 1234;
        constexpr ui32 blockSize = 4096;

        const auto buffers = GetBuffers(blockSize);
        const auto blockRanges = GetBlockRanges();
        const auto blockIndices = GetBlockIndices(blockRanges);
        const auto holders = GetHolders(buffers);

        auto oldBlobContent = NResource::Find("fresh.blob");

        TVector<TOwningFreshBlock> result;
        TBlobUpdatesByFresh blobUpdatesByFresh;

        auto error = ParseFreshBlobContent(
            commitId,
            blockSize,
            oldBlobContent,
            result,
            blobUpdatesByFresh);

        UNIT_ASSERT(SUCCEEDED(error.GetCode()));
        UNIT_ASSERT_VALUES_EQUAL(15, result.size());

        auto subBuffer = buffers.begin();
        auto buffer = subBuffer->begin();
        auto blockIndex = blockIndices.begin();

        for (ui32 i = 0; i < result.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(*buffer, result[i].Content);

            const auto& block = result[i].Meta;
            UNIT_ASSERT_VALUES_EQUAL(*blockIndex, block.BlockIndex);
            UNIT_ASSERT_VALUES_EQUAL(commitId, block.MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, block.MaxCommitId);

            if (++buffer == subBuffer->end()) {
                if (++subBuffer != buffers.end()) {
                    buffer = subBuffer->begin();
                }
            }

            ++blockIndex;
        }

        TVector<TBlobUpdate> updates;
        updates.assign(blobUpdatesByFresh.begin(), blobUpdatesByFresh.end());
        Sort(
            updates,
            [](const auto& lhs, const auto& rhs)
            { return lhs.DeletionId < rhs.DeletionId; });

        UNIT_ASSERT_VALUES_EQUAL(4, updates.size());

        auto deletionId = FirstRequestDeletionId;
        auto blockRange = blockRanges.begin();

        for (const auto update: updates) {
            UNIT_ASSERT_VALUES_EQUAL(deletionId, update.DeletionId);
            UNIT_ASSERT_VALUES_EQUAL(commitId, update.CommitId);
            UNIT_ASSERT_VALUES_EQUAL(*blockRange, update.BlockRange);

            ++deletionId;
            ++blockRange;
        }

        auto newBlobContent =
            BuildFreshBlobContent(blockRanges, holders, FirstRequestDeletionId);

        UNIT_ASSERT_VALUES_EQUAL(oldBlobContent.size(), newBlobContent.size());
        for (size_t i = 0; i < oldBlobContent.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(oldBlobContent[i], newBlobContent[i]);
        }
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2

template <>
inline void Out<NCloud::NBlockStore::TBlockRange32>(
    IOutputStream& out,
    const NCloud::NBlockStore::TBlockRange32& range)
{
    out << "[" << range.Start << ", " << range.End << "]";
}
