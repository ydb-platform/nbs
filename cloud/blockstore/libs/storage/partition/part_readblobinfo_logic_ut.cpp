#include "part_readblobinfo_logic.h"

#include "part_database.h"

#include <cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition {

namespace {

using TOutputIndex = TTxPartition::TCompactionReadBlobInfo::TOutputIndex;

ui64 GetMethodCallCount(
    const TPartitionDatabase& db,
    const TString& methodName)
{
    ui64 count = 0;
    for (const auto& [name, callCount]: db.MethodCallCounts) {
        if (name.Contains(methodName)) {
            count += callCount;
        }
    }
    return count;
}

void AssertOutputIndex(
    const TOutputIndex& actual,
    TMaybe<ui32> expectedBlockMaskIndex,
    TMaybe<ui32> expectedBlobMetaIndex)
{
    if (expectedBlockMaskIndex) {
        UNIT_ASSERT(actual.BlockMaskIndex);
        UNIT_ASSERT_VALUES_EQUAL(
            *expectedBlockMaskIndex,
            *actual.BlockMaskIndex);
    } else {
        UNIT_ASSERT(!actual.BlockMaskIndex);
    }

    if (expectedBlobMetaIndex) {
        UNIT_ASSERT(actual.BlobMetaIndex);
        UNIT_ASSERT_VALUES_EQUAL(
            *expectedBlobMetaIndex,
            *actual.BlobMetaIndex);
    } else {
        UNIT_ASSERT(!actual.BlobMetaIndex);
    }
}

NProto::TBlobMeta MakeMergedBlobMeta(ui32 start, ui32 end, ui32 skipped)
{
    NProto::TBlobMeta meta;
    auto& mergedBlocks = *meta.MutableMergedBlocks();
    mergedBlocks.SetStart(start);
    mergedBlocks.SetEnd(end);
    mergedBlocks.SetSkipped(skipped);
    return meta;
}

}   // namespace

Y_UNIT_TEST_SUITE(TDeduplicateBlobInfosTest)
{
    Y_UNIT_TEST(ShouldIndexBlockMasksOnly)
    {
        const TPartialBlobId blob1(1, 0);
        const TPartialBlobId blob2(2, 0);

        const auto result = DeduplicateBlobInfos(
            {blob1, blob2},
            {});

        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        AssertOutputIndex(result.at(blob1), 0u, Nothing());
        AssertOutputIndex(result.at(blob2), 1u, Nothing());
    }

    Y_UNIT_TEST(ShouldIndexBlobMetasOnly)
    {
        const TPartialBlobId blob1(1, 0);
        const TPartialBlobId blob2(2, 0);

        const auto result = DeduplicateBlobInfos(
            {},
            {blob1, blob2});

        UNIT_ASSERT_VALUES_EQUAL(2, result.size());
        AssertOutputIndex(result.at(blob1), Nothing(), 0u);
        AssertOutputIndex(result.at(blob2), Nothing(), 1u);
    }

    Y_UNIT_TEST(ShouldIndexDisjointBlobSets)
    {
        const TPartialBlobId maskBlob1(1, 0);
        const TPartialBlobId maskBlob2(2, 0);
        const TPartialBlobId metaBlob1(3, 0);
        const TPartialBlobId metaBlob2(4, 0);

        const auto result = DeduplicateBlobInfos(
            {maskBlob1, maskBlob2},
            {metaBlob1, metaBlob2});

        UNIT_ASSERT_VALUES_EQUAL(4, result.size());
        AssertOutputIndex(result.at(maskBlob1), 0u, Nothing());
        AssertOutputIndex(result.at(maskBlob2), 1u, Nothing());
        AssertOutputIndex(result.at(metaBlob1), Nothing(), 0u);
        AssertOutputIndex(result.at(metaBlob2), Nothing(), 1u);
    }

    Y_UNIT_TEST(ShouldDeduplicateOverlappingBlobs)
    {
        const TPartialBlobId sharedBlob(1, 0);
        const TPartialBlobId maskOnlyBlob(2, 0);
        const TPartialBlobId metaOnlyBlob(3, 0);

        const auto result = DeduplicateBlobInfos(
            {sharedBlob, maskOnlyBlob},
            {sharedBlob, metaOnlyBlob});

        UNIT_ASSERT_VALUES_EQUAL(3, result.size());
        AssertOutputIndex(result.at(sharedBlob), 0u, 0u);
        AssertOutputIndex(result.at(maskOnlyBlob), 1u, Nothing());
        AssertOutputIndex(result.at(metaOnlyBlob), Nothing(), 1u);
    }
}

Y_UNIT_TEST_SUITE(TReadBlobsInfoTest)
{
    Y_UNIT_TEST(ShouldReadBlobInfoOnceForOverlappingBlob)
    {
        TTestExecutor executor;
        executor.WriteTx([](TPartitionDatabase db) {
            db.InitSchema();
        });

        TPartialBlobId sharedBlob;
        NProto::TBlobMeta sharedBlobMeta;
        TBlockMask sharedBlockMask;

        sharedBlobMeta = MakeMergedBlobMeta(10, 20, 3);
        sharedBlockMask.Set(1, 5);

        executor.WriteTx([&](TPartitionDatabase db) {
            sharedBlob = executor.MakeBlobId();
            db.WriteBlobMeta(sharedBlob, sharedBlobMeta);
            db.WriteBlockMask(sharedBlob, sharedBlockMask);
        });

        const auto blobsToOutputIndices = DeduplicateBlobInfos(
            {sharedBlob},
            {sharedBlob});

        TVector<TBlockMask> blockMasks(1);
        TVector<NProto::TBlobMeta> blobMetas(1);

        executor.ReadTx([&](TPartitionDatabase db) {
            db.MethodCallCounts.clear();

            const bool ready = ReadBlobsInfo(
                db,
                blobsToOutputIndices,
                TTestExecutor::TabletId,
                blockMasks,
                blobMetas);

            UNIT_ASSERT(ready);
            UNIT_ASSERT_VALUES_EQUAL(1, GetMethodCallCount(db, "ReadBlobInfo"));
            UNIT_ASSERT_VALUES_EQUAL(0, GetMethodCallCount(db, "ReadBlobMeta"));
            UNIT_ASSERT_VALUES_EQUAL(0, GetMethodCallCount(db, "ReadBlockMask"));
        });

        UNIT_ASSERT_VALUES_EQUAL(
            BlockMaskAsString(sharedBlockMask),
            BlockMaskAsString(blockMasks[0]));
        google::protobuf::util::MessageDifferencer differencer;
        UNIT_ASSERT(differencer.Compare(sharedBlobMeta, blobMetas[0]));
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition
