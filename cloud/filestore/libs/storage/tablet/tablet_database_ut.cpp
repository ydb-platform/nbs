#include "tablet_database.h"

#include <cloud/filestore/libs/storage/testlib/test_executor.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/pathsplit.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletDatabaseTest)
{
    Y_UNIT_TEST(ShouldStoreFileSystem)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            NProto::TFileSystem fileSystem;
            fileSystem.SetFileSystemId("test");
            db.WriteFileSystem(fileSystem);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            NProto::TFileSystem fileSystem;
            UNIT_ASSERT(db.ReadFileSystem(fileSystem));
            UNIT_ASSERT_VALUES_EQUAL(fileSystem.GetFileSystemId(), "test");
        });
    }

    Y_UNIT_TEST(ShouldStoreFileSystemStats)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.WriteLastNodeId(1);
            db.WriteLastLockId(2);
            db.WriteLastCollectCommitId(3);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            NProto::TFileSystemStats stats;
            UNIT_ASSERT(db.ReadFileSystemStats(stats));
            UNIT_ASSERT_VALUES_EQUAL(stats.GetLastNodeId(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetLastLockId(), 2);
            UNIT_ASSERT_VALUES_EQUAL(stats.GetLastCollectCommitId(), 3);
        });
    }

    Y_UNIT_TEST(ShouldStoreNodes)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        constexpr ui64 commitId = 1;
        constexpr ui64 nodeId1 = 2;
        constexpr ui64 nodeId2 = 3;

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            NProto::TNode attrs;
            db.WriteNode(nodeId1, commitId, attrs);
            db.WriteNode(nodeId2, commitId, attrs);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TMaybe<IIndexTabletDatabase::TNode> node1;
            UNIT_ASSERT(db.ReadNode(nodeId1, commitId, node1));
            UNIT_ASSERT(node1);

            TMaybe<IIndexTabletDatabase::TNode> node2;
            UNIT_ASSERT(db.ReadNode(nodeId2, commitId, node2));
            UNIT_ASSERT(node2);

            TMaybe<IIndexTabletDatabase::TNode> node3;
            UNIT_ASSERT(db.ReadNode(12345, commitId, node3));
            UNIT_ASSERT(!node3);
        });
    }

    Y_UNIT_TEST(ShouldStoreNodeAttrs)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        constexpr ui64 commitId = 1;
        constexpr ui64 nodeId = 2;

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            NProto::TNode attrs;
            db.WriteNode(nodeId, commitId, attrs);
            db.WriteNodeAttr(nodeId, commitId, "name1", "value1", 0);
            db.WriteNodeAttr(nodeId, commitId, "name2", "value2", 0);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TMaybe<TIndexTabletDatabase::TNodeAttr> attr1;
            UNIT_ASSERT(db.ReadNodeAttr(nodeId, commitId, "name1", attr1));
            UNIT_ASSERT_EQUAL(attr1->Value, "value1");

            TMaybe<TIndexTabletDatabase::TNodeAttr> attr2;
            UNIT_ASSERT(db.ReadNodeAttr(nodeId, commitId, "name2", attr2));
            UNIT_ASSERT_EQUAL(attr2->Value, "value2");

            TMaybe<TIndexTabletDatabase::TNodeAttr> attr3;
            UNIT_ASSERT(db.ReadNodeAttr(nodeId, commitId, "name3", attr3));
            UNIT_ASSERT(!attr3);
        });
    }

    Y_UNIT_TEST(ShouldStoreNodeRefs)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        constexpr ui64 commitId = 1;
        constexpr ui64 nodeId = 1;
        constexpr ui64 childNodeId1 = 2;
        constexpr ui64 childNodeId2 = 3;

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            NProto::TNode attrs;
            db.WriteNode(nodeId, commitId, attrs);
            db.WriteNode(childNodeId1, commitId, attrs);
            db.WriteNode(childNodeId2, commitId, attrs);
            db.WriteNodeRef(nodeId, commitId, "child1", childNodeId1, "", "");
            db.WriteNodeRef(
                nodeId,
                commitId,
                "child2",
                childNodeId2,
                "shard",
                "name");
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TMaybe<IIndexTabletDatabase::TNodeRef> ref1;
            UNIT_ASSERT(db.ReadNodeRef(nodeId, commitId, "child1", ref1));
            UNIT_ASSERT_EQUAL(ref1->ChildNodeId, childNodeId1);
            UNIT_ASSERT_EQUAL(ref1->ShardId, "");
            UNIT_ASSERT_EQUAL(ref1->ShardName, "");

            TMaybe<IIndexTabletDatabase::TNodeRef> ref2;
            UNIT_ASSERT(db.ReadNodeRef(nodeId, commitId, "child2", ref2));
            UNIT_ASSERT_EQUAL(ref2->ChildNodeId, childNodeId2);
            UNIT_ASSERT_EQUAL(ref2->ShardId, "shard");
            UNIT_ASSERT_EQUAL(ref2->ShardName, "name");

            TMaybe<IIndexTabletDatabase::TNodeRef> ref3;
            UNIT_ASSERT(db.ReadNodeRef(nodeId, commitId, "child3", ref3));
            UNIT_ASSERT(!ref3);
        });
    }

    Y_UNIT_TEST(ShouldStoreCheckpoints)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        const TString checkpointId1 = "test1";
        const TString checkpointId2 = "test2";

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            NProto::TCheckpoint checkpoint;

            checkpoint.SetCheckpointId(checkpointId1);
            db.WriteCheckpoint(checkpoint);

            checkpoint.SetCheckpointId(checkpointId2);
            db.WriteCheckpoint(checkpoint);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<NProto::TCheckpoint> checkpoints;
            UNIT_ASSERT(db.ReadCheckpoints(checkpoints));
            UNIT_ASSERT_EQUAL(checkpoints.size(), 2);
        });
    }

    Y_UNIT_TEST(ShouldStoreSessions)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        const TString sessionId1 = "test1";
        const TString sessionId2 = "test2";

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            NProto::TSession session;

            session.SetSessionId(sessionId1);
            db.WriteSession(session);

            session.SetSessionId(sessionId2);
            db.WriteSession(session);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<NProto::TSession> sessions;
            UNIT_ASSERT(db.ReadSessions(sessions));
            UNIT_ASSERT_EQUAL(sessions.size(), 2);
        });
    }

    Y_UNIT_TEST(ShouldStoreDupCache)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        const TString sessionId1 = "test1";
        const TString sessionId2 = "test2";

        ui64 entryId = 1;
        ui64 requestId = 100500;

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            NProto::TDupCacheEntry entry;
            entry.SetSessionId(sessionId1);

            entry.SetRequestId(requestId++);
            entry.SetEntryId(entryId++);
            db.WriteSessionDupCacheEntry(entry);

            entry.SetRequestId(requestId++);
            entry.SetEntryId(entryId++);
            db.WriteSessionDupCacheEntry(entry);

            entry.SetRequestId(requestId++);
            entry.SetEntryId(entryId++);
            db.WriteSessionDupCacheEntry(entry);

            entryId = 1;
            entry.SetSessionId(sessionId2);

            entry.SetRequestId(requestId++);
            entry.SetEntryId(entryId++);
            db.WriteSessionDupCacheEntry(entry);

            entry.SetRequestId(requestId++);
            entry.SetEntryId(entryId++);
            db.WriteSessionDupCacheEntry(entry);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<NProto::TDupCacheEntry> entries;
            UNIT_ASSERT(db.ReadSessionDupCacheEntries(entries));
            UNIT_ASSERT_EQUAL(entries.size(), 5);

            UNIT_ASSERT_VALUES_EQUAL(entries[0].GetSessionId(), sessionId1);
            UNIT_ASSERT_VALUES_EQUAL(entries[0].GetRequestId(), 100500);
            UNIT_ASSERT_VALUES_EQUAL(entries[3].GetSessionId(), sessionId2);
            UNIT_ASSERT_VALUES_EQUAL(entries[3].GetRequestId(), 100503);
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.DeleteSessionDupCacheEntry(sessionId1, 1);
            db.DeleteSessionDupCacheEntry(sessionId2, 1);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<NProto::TDupCacheEntry> entries;
            UNIT_ASSERT(db.ReadSessionDupCacheEntries(entries));
            UNIT_ASSERT_EQUAL(entries.size(), 3);

            UNIT_ASSERT_VALUES_EQUAL(entries[0].GetSessionId(), sessionId1);
            UNIT_ASSERT_VALUES_EQUAL(entries[0].GetRequestId(), 100501);
            UNIT_ASSERT_VALUES_EQUAL(entries[2].GetSessionId(), sessionId2);
            UNIT_ASSERT_VALUES_EQUAL(entries[2].GetRequestId(), 100504);
        });

    }

    Y_UNIT_TEST(ShouldStoreFreshBytes)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        constexpr ui64 nodeId = 1;
        constexpr ui64 commitId = 2;
        constexpr ui64 offset = 3;
        const TString data = "bbb";

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.WriteFreshBytes(nodeId, commitId, offset, data);
            db.WriteFreshBytesDeletionMarker(nodeId, commitId + 10, offset + 1, 2);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<TIndexTabletDatabase::TFreshBytesEntry> bytes;
            UNIT_ASSERT(db.ReadFreshBytes(bytes));
            UNIT_ASSERT_VALUES_EQUAL(2, bytes.size());

            UNIT_ASSERT_VALUES_EQUAL(nodeId, bytes[0].NodeId);
            UNIT_ASSERT_VALUES_EQUAL(commitId, bytes[0].MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(offset, bytes[0].Offset);
            UNIT_ASSERT_VALUES_EQUAL(data, bytes[0].Data);
            UNIT_ASSERT_VALUES_EQUAL(data.Size(), bytes[0].Len);

            UNIT_ASSERT_VALUES_EQUAL(nodeId, bytes[1].NodeId);
            UNIT_ASSERT_VALUES_EQUAL(commitId + 10, bytes[1].MinCommitId);
            UNIT_ASSERT_VALUES_EQUAL(offset + 1, bytes[1].Offset);
            UNIT_ASSERT_VALUES_EQUAL(TString(), bytes[1].Data);
            UNIT_ASSERT_VALUES_EQUAL(2, bytes[1].Len);
        });
    }

    Y_UNIT_TEST(ShouldStoreStorageConfig)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        TMaybe<NProto::TStorageConfig> serviceConfig;

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            UNIT_ASSERT(db.ReadStorageConfig(serviceConfig));
            UNIT_ASSERT(!serviceConfig.Defined());
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            NProto::TStorageConfig config;
            config.SetHDDIndexChannelPoolKind("rotmirror");
            db.WriteStorageConfig(config);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            UNIT_ASSERT(db.ReadStorageConfig(serviceConfig));

            UNIT_ASSERT(serviceConfig.Defined());
            UNIT_ASSERT_VALUES_EQUAL(
                "rotmirror", serviceConfig->GetHDDIndexChannelPoolKind());
            // Shouldn't change values not from db
            UNIT_ASSERT(!serviceConfig->HasSSDSystemChannelPoolKind());
        });
    }

    Y_UNIT_TEST(ShouldStoreCompactionMap)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        using TEntries = TVector<TCompactionRangeInfo>;
        TEntries entries = {
            {1, {50, 100}},
            {4, {42, 10}},
            {32, {50, 200}},
            {65, {1, 400}},
            {110, {2, 333}},
            {113, {7, 444}},
            {4233, {150, 555}},
            {5632, {1000, 3}},
            {6000, {30, 11}},
            {6001, {12, 15}},
            {6002, {2, 20}},
            {6005, {99, 7}},
            {7000, {1000, 5000}},
        };

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            for (const auto& entry: entries) {
                db.WriteCompactionMap(
                    entry.RangeId,
                    entry.Stats.BlobsCount,
                    entry.Stats.DeletionsCount);
            }
        });

        auto toString = [] (
            const TEntries& v,
            ui32 i = 0,
            ui32 c = Max<ui32>(),
            bool skipZeroes = false)
        {
            TStringBuilder sb;
            ui32 processed = 0;
            while (i < v.size() && processed < c) {
                if (skipZeroes
                        && !v[i].Stats.BlobsCount
                        && !v[i].Stats.DeletionsCount)
                {
                    ++i;
                    continue;
                }

                if (processed) {
                    sb << " ";
                }
                sb << v[i].RangeId
                    << "," << v[i].Stats.BlobsCount
                    << "," << v[i].Stats.DeletionsCount;
                ++processed;
                ++i;
            }
            return sb;
        };

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<TCompactionRangeInfo> chunk;
            UNIT_ASSERT(db.ReadCompactionMap(chunk, 0, 5, true));
            UNIT_ASSERT_VALUES_EQUAL(toString(entries, 0, 5), toString(chunk));

            chunk.clear();
            UNIT_ASSERT(db.ReadCompactionMap(chunk, 111, 5, true));
            UNIT_ASSERT_VALUES_EQUAL(toString(entries, 5, 5), toString(chunk));

            chunk.clear();
            UNIT_ASSERT(db.ReadCompactionMap(chunk, 6002, 5, true));
            UNIT_ASSERT_VALUES_EQUAL(toString(entries, 10, 5), toString(chunk));

            chunk.clear();
            UNIT_ASSERT(db.ReadCompactionMap(chunk, 7001, 5, true));
            UNIT_ASSERT_VALUES_EQUAL("", toString(chunk));
        });

        TVector<ui32> toDelete({0, 6, 7, 12});

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            for (const auto i: toDelete) {
                db.WriteCompactionMap(entries[i].RangeId, 0, 0);
                entries[i].Stats = {};
            }
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<TCompactionRangeInfo> chunk;
            UNIT_ASSERT(db.ReadCompactionMap(chunk, 0, Max<ui32>(), true));
            UNIT_ASSERT_VALUES_EQUAL(
                toString(entries, 0, Max<ui32>(), true),
                toString(chunk));
        });
    }

    Y_UNIT_TEST(ShouldStoreLargeDeletionMarkers)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        using TEntries = TVector<TDeletionMarker>;
        TEntries entries = {
            {1, 100500, 1024, 1024 * 1024},
            {4, 100501, 20 * 1024 * 1024, 5 * 1024 * 1024},
            {10, 100502, 100 * 1024 * 1024, 100 * 1024 * 1024},
        };

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            for (const auto& entry: entries) {
                db.WriteLargeDeletionMarkers(
                    entry.NodeId,
                    entry.CommitId,
                    entry.BlockIndex,
                    entry.BlockCount);
            }
        });

        auto toString = [] (const TEntries& v) {
            TStringBuilder sb;
            for (ui32 i = 0; i < v.size(); ++i) {
                if (i) {
                    sb << " ";
                }
                sb << v[i].NodeId
                    << ",@" << v[i].CommitId
                    << "," << v[i].BlockIndex
                    << "," << v[i].BlockCount;
            }
            return sb;
        };

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<TDeletionMarker> markers;
            UNIT_ASSERT(db.ReadLargeDeletionMarkers(markers));
            UNIT_ASSERT_VALUES_EQUAL(toString(entries), toString(markers));
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.DeleteLargeDeletionMarker(
                entries.back().NodeId,
                entries.back().CommitId,
                entries.back().BlockIndex);
            entries.pop_back();
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<TDeletionMarker> markers;
            UNIT_ASSERT(db.ReadLargeDeletionMarkers(markers));
            UNIT_ASSERT_VALUES_EQUAL(toString(entries), toString(markers));
        });
    }

    Y_UNIT_TEST(ShouldStoreOrphanNodes)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema(false);
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.WriteOrphanNode(111);
            db.WriteOrphanNode(222);
            db.WriteOrphanNode(333);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<ui64> nodeIds;
            UNIT_ASSERT(db.ReadOrphanNodes(nodeIds));
            UNIT_ASSERT_VALUES_EQUAL(3, nodeIds.size());
            UNIT_ASSERT_VALUES_EQUAL(111, nodeIds[0]);
            UNIT_ASSERT_VALUES_EQUAL(222, nodeIds[1]);
            UNIT_ASSERT_VALUES_EQUAL(333, nodeIds[2]);
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.DeleteOrphanNode(222);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<ui64> nodeIds;
            UNIT_ASSERT(db.ReadOrphanNodes(nodeIds));
            UNIT_ASSERT_VALUES_EQUAL(2, nodeIds.size());
            UNIT_ASSERT_VALUES_EQUAL(111, nodeIds[0]);
            UNIT_ASSERT_VALUES_EQUAL(333, nodeIds[1]);
        });
    }
}

}   // namespace NCloud::NFileStore::NStorage
