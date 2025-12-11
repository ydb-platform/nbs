#include "part2_database.h"

#include <cloud/blockstore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NBlockStore::NStorage::NPartition2 {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TPartitionDatabaseTest)
{
    Y_UNIT_TEST(ShouldStoreMeta)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                NProto::TPartitionMeta meta;
                db.WriteMeta(meta);
            });

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TMaybe<NProto::TPartitionMeta> meta;
                UNIT_ASSERT(db.ReadMeta(meta));
                UNIT_ASSERT(meta);
            });
    }

    Y_UNIT_TEST(ShouldStoreBlobs)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        auto blobId1 = TPartialBlobId(1, 0);
        auto blobId2 = TPartialBlobId(2, 0);
        auto blobId3 = TPartialBlobId(3, 0);
        auto blobId4 = TPartialBlobId(4, 0);
        auto blobId5 = TPartialBlobId(5, 0);

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                NProto::TBlobMeta2 blobMeta;
                blobMeta.AddStartIndices(0);
                blobMeta.AddEndIndices(1024);
                blobMeta.SetBlockCount(4);

                db.WriteGlobalBlob(blobId1, blobMeta);
                db.WriteBlockList(
                    blobId1,
                    BuildBlockList({
                        {0, 1, InvalidCommitId, false},
                        {200, 1, InvalidCommitId, false},
                        {400, 1, InvalidCommitId, false},
                        {600, 1, InvalidCommitId, false},
                    }));

                db.WriteGlobalBlob(blobId2, blobMeta);
                db.WriteBlockList(
                    blobId2,
                    BuildBlockList({
                        {100, 1, InvalidCommitId, false},
                        {300, 1, InvalidCommitId, false},
                        {500, 1, InvalidCommitId, false},
                        {700, 1, InvalidCommitId, false},
                    }));

                blobMeta.SetBlockCount(2);
                db.WriteZoneBlob(0, blobId3, blobMeta);
                db.WriteBlockList(
                    blobId3,
                    BuildBlockList({
                        {10, 1, InvalidCommitId, false},
                        {20, 1, InvalidCommitId, false},
                    }));

                db.WriteZoneBlob(0, blobId4, blobMeta);
                db.WriteBlockList(
                    blobId4,
                    BuildBlockList({
                        {30, 1, InvalidCommitId, false},
                        {40, 1, InvalidCommitId, false},
                    }));

                db.WriteZoneBlob(1, blobId5, blobMeta);
                db.WriteBlockList(
                    blobId5,
                    BuildBlockList({
                        {530, 1, InvalidCommitId, false},
                        {540, 1, InvalidCommitId, false},
                    }));
            });

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TVector<TPartitionDatabase::TBlobMeta> blobs;
                UNIT_ASSERT(db.ReadGlobalBlobs(blobs));
                UNIT_ASSERT_VALUES_EQUAL(2, blobs.size());
                UNIT_ASSERT_VALUES_EQUAL(blobId1, blobs[0].BlobId);
                UNIT_ASSERT_VALUES_EQUAL(blobId2, blobs[1].BlobId);

                blobs.clear();
                UNIT_ASSERT(db.ReadZoneBlobs(0, blobs));
                UNIT_ASSERT_VALUES_EQUAL(2, blobs.size());
                UNIT_ASSERT_VALUES_EQUAL(blobId3, blobs[0].BlobId);
                UNIT_ASSERT_VALUES_EQUAL(blobId4, blobs[1].BlobId);

                blobs.clear();
                UNIT_ASSERT(db.ReadZoneBlobs(1, blobs));
                UNIT_ASSERT_VALUES_EQUAL(1, blobs.size());
                UNIT_ASSERT_VALUES_EQUAL(blobId5, blobs[0].BlobId);

                blobs.clear();
                UNIT_ASSERT(db.ReadZoneBlobs(2, blobs));
                UNIT_ASSERT_VALUES_EQUAL(0, blobs.size());

                TMaybe<TBlockList> blocks1;
                UNIT_ASSERT(db.ReadBlockList(blobId1, blocks1));
                UNIT_ASSERT(blocks1);

                TMaybe<TBlockList> blocks2;
                UNIT_ASSERT(db.ReadBlockList(blobId2, blocks2));
                UNIT_ASSERT(blocks2);

                TMaybe<TBlockList> blocks3;
                UNIT_ASSERT(db.ReadBlockList(blobId3, blocks3));
                UNIT_ASSERT(blocks3);

                TMaybe<TBlockList> blocks4;
                UNIT_ASSERT(db.ReadBlockList(blobId4, blocks4));
                UNIT_ASSERT(blocks4);

                TMaybe<TBlockList> blocks5;
                UNIT_ASSERT(db.ReadBlockList(blobId5, blocks5));
                UNIT_ASSERT(blocks5);
            });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                db.DeleteGlobalBlob(blobId1);
                db.DeleteBlockList(blobId1);
                db.DeleteGlobalBlob(blobId2);
                db.DeleteBlockList(blobId2);
                db.DeleteZoneBlob(0, blobId3);
                db.DeleteBlockList(blobId3);
                db.DeleteZoneBlob(0, blobId4);
                db.DeleteBlockList(blobId4);
                db.DeleteZoneBlob(1, blobId5);
                db.DeleteBlockList(blobId5);
            });

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TVector<TPartitionDatabase::TBlobMeta> blobs;
                UNIT_ASSERT(db.ReadGlobalBlobs(blobs));
                UNIT_ASSERT_VALUES_EQUAL(0, blobs.size());

                UNIT_ASSERT(db.ReadZoneBlobs(0, blobs));
                UNIT_ASSERT_VALUES_EQUAL(0, blobs.size());

                UNIT_ASSERT(db.ReadZoneBlobs(1, blobs));
                UNIT_ASSERT_VALUES_EQUAL(0, blobs.size());

                TMaybe<TBlockList> blocks1;
                UNIT_ASSERT(db.ReadBlockList(blobId1, blocks1));
                UNIT_ASSERT(!blocks1);

                TMaybe<TBlockList> blocks2;
                UNIT_ASSERT(db.ReadBlockList(blobId2, blocks2));
                UNIT_ASSERT(!blocks2);

                TMaybe<TBlockList> blocks3;
                UNIT_ASSERT(db.ReadBlockList(blobId3, blocks3));
                UNIT_ASSERT(!blocks3);

                TMaybe<TBlockList> blocks4;
                UNIT_ASSERT(db.ReadBlockList(blobId4, blocks4));
                UNIT_ASSERT(!blocks4);

                TMaybe<TBlockList> blocks5;
                UNIT_ASSERT(db.ReadBlockList(blobId5, blocks5));
                UNIT_ASSERT(!blocks5);
            });
    }

    Y_UNIT_TEST(ShouldStoreBlobUpdates)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                db.WriteGlobalBlobUpdate(
                    1,
                    10,
                    TBlockRange32::WithLength(0, 100));
                db.WriteGlobalBlobUpdate(
                    2,
                    20,
                    TBlockRange32::WithLength(50, 150));
                db.WriteZoneBlobUpdate(
                    0,
                    3,
                    30,
                    TBlockRange32::WithLength(0, 9));
                db.WriteZoneBlobUpdate(
                    1,
                    4,
                    40,
                    TBlockRange32::WithLength(100, 109));
                db.WriteZoneBlobUpdate(
                    1,
                    5,
                    50,
                    TBlockRange32::WithLength(110, 119));
            });

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TVector<TBlobUpdate> updates;
                UNIT_ASSERT(db.ReadGlobalBlobUpdates(updates));
                UNIT_ASSERT_VALUES_EQUAL(2, updates.size());
                UNIT_ASSERT_VALUES_EQUAL(1, updates[0].DeletionId);
                UNIT_ASSERT_VALUES_EQUAL(10, updates[0].CommitId);
                UNIT_ASSERT_VALUES_EQUAL(2, updates[1].DeletionId);
                UNIT_ASSERT_VALUES_EQUAL(20, updates[1].CommitId);

                updates.clear();
                UNIT_ASSERT(db.ReadZoneBlobUpdates(0, updates));
                UNIT_ASSERT_VALUES_EQUAL(1, updates.size());
                UNIT_ASSERT_VALUES_EQUAL(3, updates[0].DeletionId);
                UNIT_ASSERT_VALUES_EQUAL(30, updates[0].CommitId);

                updates.clear();
                UNIT_ASSERT(db.ReadZoneBlobUpdates(1, updates));
                UNIT_ASSERT_VALUES_EQUAL(2, updates.size());
                UNIT_ASSERT_VALUES_EQUAL(4, updates[0].DeletionId);
                UNIT_ASSERT_VALUES_EQUAL(40, updates[0].CommitId);
                UNIT_ASSERT_VALUES_EQUAL(5, updates[1].DeletionId);
                UNIT_ASSERT_VALUES_EQUAL(50, updates[1].CommitId);
            });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                db.DeleteGlobalBlobUpdate(1);
                db.DeleteGlobalBlobUpdate(2);
                db.DeleteZoneBlobUpdate(0, 3);
                db.DeleteZoneBlobUpdate(1, 4);
                db.DeleteZoneBlobUpdate(1, 5);
            });

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TVector<TBlobUpdate> updates;
                UNIT_ASSERT(db.ReadGlobalBlobUpdates(updates));
                UNIT_ASSERT_VALUES_EQUAL(0, updates.size());

                UNIT_ASSERT(db.ReadZoneBlobUpdates(0, updates));
                UNIT_ASSERT_VALUES_EQUAL(0, updates.size());

                UNIT_ASSERT(db.ReadZoneBlobUpdates(1, updates));
                UNIT_ASSERT_VALUES_EQUAL(0, updates.size());
            });
    }

    Y_UNIT_TEST(ShouldStoreBlobGarbage)
    {
        TTestExecutor executor;
        executor.WriteTx([&](TPartitionDatabase db) { db.InitSchema(); });

        auto blobId1 = TPartialBlobId(1, 0);
        auto blobId2 = TPartialBlobId(2, 0);
        auto blobId3 = TPartialBlobId(3, 0);
        auto blobId4 = TPartialBlobId(4, 0);
        auto blobId5 = TPartialBlobId(5, 0);

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                db.WriteGlobalBlobGarbage({blobId1, 100});
                db.WriteGlobalBlobGarbage({blobId2, 200});
                db.WriteZoneBlobGarbage(0, {blobId3, 30});
                db.WriteZoneBlobGarbage(1, {blobId4, 40});
                db.WriteZoneBlobGarbage(1, {blobId5, 50});
            });

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TVector<TPartitionDatabase::TBlobGarbage> garbage;
                UNIT_ASSERT(db.ReadGlobalBlobGarbage(garbage));
                UNIT_ASSERT_VALUES_EQUAL(2, garbage.size());
                UNIT_ASSERT_VALUES_EQUAL(blobId1, garbage[0].BlobId);
                UNIT_ASSERT_VALUES_EQUAL(100, garbage[0].BlockCount);
                UNIT_ASSERT_VALUES_EQUAL(blobId2, garbage[1].BlobId);
                UNIT_ASSERT_VALUES_EQUAL(200, garbage[1].BlockCount);

                garbage.clear();
                UNIT_ASSERT(db.ReadZoneBlobGarbage(0, garbage));
                UNIT_ASSERT_VALUES_EQUAL(1, garbage.size());
                UNIT_ASSERT_VALUES_EQUAL(blobId3, garbage[0].BlobId);
                UNIT_ASSERT_VALUES_EQUAL(30, garbage[0].BlockCount);

                garbage.clear();
                UNIT_ASSERT(db.ReadZoneBlobGarbage(1, garbage));
                UNIT_ASSERT_VALUES_EQUAL(2, garbage.size());
                UNIT_ASSERT_VALUES_EQUAL(blobId4, garbage[0].BlobId);
                UNIT_ASSERT_VALUES_EQUAL(40, garbage[0].BlockCount);
                UNIT_ASSERT_VALUES_EQUAL(blobId5, garbage[1].BlobId);
                UNIT_ASSERT_VALUES_EQUAL(50, garbage[1].BlockCount);
            });

        executor.WriteTx(
            [&](TPartitionDatabase db)
            {
                db.DeleteGlobalBlobGarbage(blobId1);
                db.DeleteGlobalBlobGarbage(blobId2);
                db.DeleteZoneBlobGarbage(0, blobId3);
                db.DeleteZoneBlobGarbage(1, blobId4);
                db.DeleteZoneBlobGarbage(1, blobId5);
            });

        executor.ReadTx(
            [&](TPartitionDatabase db)
            {
                TVector<TPartitionDatabase::TBlobGarbage> garbage;
                UNIT_ASSERT(db.ReadGlobalBlobGarbage(garbage));
                UNIT_ASSERT_VALUES_EQUAL(0, garbage.size());

                UNIT_ASSERT(db.ReadZoneBlobGarbage(0, garbage));
                UNIT_ASSERT_VALUES_EQUAL(0, garbage.size());

                UNIT_ASSERT(db.ReadZoneBlobGarbage(1, garbage));
                UNIT_ASSERT_VALUES_EQUAL(0, garbage.size());
            });
    }

    Y_UNIT_TEST(ShouldStoreCheckpoints)
    {
        // TODO
    }

    Y_UNIT_TEST(ShouldStoreCheckpointBlobs)
    {
        // TODO
    }

    Y_UNIT_TEST(ShouldStoreCompactionMap)
    {
        // TODO
    }

    Y_UNIT_TEST(ShouldStoreGarbageBlobs)
    {
        // TODO
    }
}

}   // namespace NCloud::NBlockStore::NStorage::NPartition2
