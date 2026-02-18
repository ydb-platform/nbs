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
            db.InitSchema();
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
            db.InitSchema();
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
            db.InitSchema();
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
            db.InitSchema();
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
            db.InitSchema();
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
            UNIT_ASSERT_EQUAL(ref1->ShardNodeName, "");

            TMaybe<IIndexTabletDatabase::TNodeRef> ref2;
            UNIT_ASSERT(db.ReadNodeRef(nodeId, commitId, "child2", ref2));
            UNIT_ASSERT_EQUAL(ref2->ChildNodeId, childNodeId2);
            UNIT_ASSERT_EQUAL(ref2->ShardId, "shard");
            UNIT_ASSERT_EQUAL(ref2->ShardNodeName, "name");

            TMaybe<IIndexTabletDatabase::TNodeRef> ref3;
            UNIT_ASSERT(db.ReadNodeRef(nodeId, commitId, "child3", ref3));
            UNIT_ASSERT(!ref3);
        });
    }

    Y_UNIT_TEST(ShouldStoreCheckpoints)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema();
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
            db.InitSchema();
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
            db.InitSchema();
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
            db.InitSchema();
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
            UNIT_ASSERT_VALUES_EQUAL(data.size(), bytes[0].Len);

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
            db.InitSchema();
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
            db.InitSchema();
        });

        using TEntries = TVector<TCompactionRangeInfo>;
        TEntries entries = {
            {1, {50, 100, 200}},
            {4, {42, 10, 300}},
            {32, {50, 200, 100}},
            {65, {1, 400, 50}},
            {110, {2, 333, 10}},
            {113, {7, 444, 0}},
            {4233, {150, 555, 0}},
            {5632, {1000, 3, 500}},
            {6000, {30, 11, 600}},
            {6001, {12, 15, 30}},
            {6002, {2, 20, 10}},
            {6005, {99, 7, 55}},
            {7000, {1000, 5000, 7000}},
        };

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            for (const auto& entry: entries) {
                db.WriteCompactionMap(
                    entry.RangeId,
                    entry.Stats.BlobsCount,
                    entry.Stats.DeletionsCount,
                    entry.Stats.GarbageBlocksCount);
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
                        && !v[i].Stats.DeletionsCount
                        && !v[i].Stats.GarbageBlocksCount)
                {
                    ++i;
                    continue;
                }

                if (processed) {
                    sb << " ";
                }
                sb << v[i].RangeId
                    << "," << v[i].Stats.BlobsCount
                    << "," << v[i].Stats.DeletionsCount
                    << "," << v[i].Stats.GarbageBlocksCount;
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
                db.WriteCompactionMap(entries[i].RangeId, 0, 0, 0);
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
            db.InitSchema();
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
            db.InitSchema();
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

    Y_UNIT_TEST(ShouldUseNoAutoPrechargeArgForNodeRefs)
    {
        TTestExecutor executor;

        executor.WriteTx([&](TIndexTabletDatabase db)
                         { db.InitSchema(); });

        constexpr ui64 commitId = 1;
        constexpr ui64 nodeId = 1;
        constexpr ui64 childNodeId1 = 2;
        constexpr ui64 childNodeId2 = 3;

        executor.WriteTx(
            [&](TIndexTabletDatabase db)
            {
                NProto::TNode attrs;
                db.WriteNode(nodeId, commitId, attrs);
                db.WriteNode(childNodeId1, commitId, attrs);
                db.WriteNode(childNodeId2, commitId, attrs);
                db.WriteNodeRef(
                    nodeId,
                    commitId,
                    "child1",
                    childNodeId1,
                    "",
                    "");
                db.WriteNodeRef(
                    nodeId,
                    commitId,
                    "child2",
                    childNodeId2,
                    "",
                    "");
            });

        executor.ReadTx(
            [&](TIndexTabletDatabase db)
            {
                TVector<IIndexTabletDatabase::TNodeRef> refs;
                UNIT_ASSERT(db.ReadNodeRefs(
                    nodeId,
                    commitId,
                    "",
                    refs,
                    0,
                    nullptr,
                    nullptr,
                    false,
                    NProto::LNSM_NAME_ONLY));
                UNIT_ASSERT_VALUES_EQUAL(2, refs.size());
            });

        executor.ReadTx(
            [&](TIndexTabletDatabase db)
            {
                TVector<IIndexTabletDatabase::TNodeRef> refs;
                UNIT_ASSERT(db.ReadNodeRefs(
                    nodeId,
                    commitId,
                    "",
                    refs,
                    0,
                    nullptr,
                    nullptr,
                    true,
                    NProto::LNSM_NAME_ONLY));
                UNIT_ASSERT_VALUES_EQUAL(2, refs.size());
            });
    }

    Y_UNIT_TEST(ShouldStoreOpLog)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema();
        });

        auto makeEntry = [&] (
            ui64 entryId,
            TString fsId,
            ui64 newParentId,
            TString newName)
        {
            NProto::TOpLogEntry e;
            e.SetEntryId(entryId);
            auto* r = e.MutableRenameNodeInDestinationRequest();
            r->SetFileSystemId(std::move(fsId));
            r->SetNewParentId(newParentId);
            r->SetNewName(std::move(newName));
            return e;
        };

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.WriteOpLogEntry(makeEntry(100, "fs0", 10, "name0"));
            db.WriteOpLogEntry(makeEntry(101, "fs1", 11, "name1"));
            db.WriteOpLogEntry(makeEntry(102, "fs2", 12, "name2"));
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<NProto::TOpLogEntry> entries;
            UNIT_ASSERT(db.ReadOpLog(entries));
            UNIT_ASSERT_VALUES_EQUAL(3, entries.size());
            UNIT_ASSERT_VALUES_EQUAL(100, entries[0].GetEntryId());
            UNIT_ASSERT_VALUES_EQUAL(101, entries[1].GetEntryId());
            UNIT_ASSERT_VALUES_EQUAL(102, entries[2].GetEntryId());
            const auto& r0 = entries[0].GetRenameNodeInDestinationRequest();
            const auto& r1 = entries[1].GetRenameNodeInDestinationRequest();
            const auto& r2 = entries[2].GetRenameNodeInDestinationRequest();
            UNIT_ASSERT_VALUES_EQUAL("fs0", r0.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(10, r0.GetNewParentId());
            UNIT_ASSERT_VALUES_EQUAL("name0", r0.GetNewName());
            UNIT_ASSERT_VALUES_EQUAL("fs1", r1.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(11, r1.GetNewParentId());
            UNIT_ASSERT_VALUES_EQUAL("name1", r1.GetNewName());
            UNIT_ASSERT_VALUES_EQUAL("fs2", r2.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(12, r2.GetNewParentId());
            UNIT_ASSERT_VALUES_EQUAL("name2", r2.GetNewName());
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.DeleteOpLogEntry(101);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<NProto::TOpLogEntry> entries;
            UNIT_ASSERT(db.ReadOpLog(entries));
            UNIT_ASSERT_VALUES_EQUAL(2, entries.size());
            UNIT_ASSERT_VALUES_EQUAL(100, entries[0].GetEntryId());
            UNIT_ASSERT_VALUES_EQUAL(102, entries[1].GetEntryId());
            const auto& r0 = entries[0].GetRenameNodeInDestinationRequest();
            const auto& r2 = entries[1].GetRenameNodeInDestinationRequest();
            UNIT_ASSERT_VALUES_EQUAL("fs0", r0.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(10, r0.GetNewParentId());
            UNIT_ASSERT_VALUES_EQUAL("name0", r0.GetNewName());
            UNIT_ASSERT_VALUES_EQUAL("fs2", r2.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(12, r2.GetNewParentId());
            UNIT_ASSERT_VALUES_EQUAL("name2", r2.GetNewName());
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TMaybe<NProto::TOpLogEntry> entry;
            UNIT_ASSERT(db.ReadOpLogEntry(100, entry));
            UNIT_ASSERT_VALUES_EQUAL(100, entry->GetEntryId());
            const auto& r = entry->GetRenameNodeInDestinationRequest();
            UNIT_ASSERT_VALUES_EQUAL("fs0", r.GetFileSystemId());
            UNIT_ASSERT_VALUES_EQUAL(10, r.GetNewParentId());
            UNIT_ASSERT_VALUES_EQUAL("name0", r.GetNewName());
        });
    }

    Y_UNIT_TEST(ShouldStoreResponseLog)
    {
        TTestExecutor executor;
        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.InitSchema();
        });

        auto makeEntry = [&] (
            ui64 clientTabletId,
            ui64 requestId,
            TString oldTargetNodeShardId,
            TString oldTargetNodeShardNodeName)
        {
            NProtoPrivate::TResponseLogEntry e;
            e.SetClientTabletId(clientTabletId);
            e.SetRequestId(requestId);
            auto* r = e.MutableRenameNodeInDestinationResponse();
            r->SetOldTargetNodeShardId(std::move(oldTargetNodeShardId));
            r->SetOldTargetNodeShardNodeName(
                std::move(oldTargetNodeShardNodeName));
            return e;
        };

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.WriteResponseLogEntry(makeEntry(100, 10, "sh0", "n0"));
            db.WriteResponseLogEntry(makeEntry(101, 11, "sh1", "n1"));
            db.WriteResponseLogEntry(makeEntry(102, 12, "sh2", "n2"));
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<NProtoPrivate::TResponseLogEntry> entries;
            UNIT_ASSERT(db.ReadResponseLog(entries));
            UNIT_ASSERT_VALUES_EQUAL(3, entries.size());
            UNIT_ASSERT_VALUES_EQUAL(100, entries[0].GetClientTabletId());
            UNIT_ASSERT_VALUES_EQUAL(10, entries[0].GetRequestId());
            UNIT_ASSERT_VALUES_EQUAL(101, entries[1].GetClientTabletId());
            UNIT_ASSERT_VALUES_EQUAL(11, entries[1].GetRequestId());
            UNIT_ASSERT_VALUES_EQUAL(102, entries[2].GetClientTabletId());
            UNIT_ASSERT_VALUES_EQUAL(12, entries[2].GetRequestId());
            const auto& r0 = entries[0].GetRenameNodeInDestinationResponse();
            const auto& r1 = entries[1].GetRenameNodeInDestinationResponse();
            const auto& r2 = entries[2].GetRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL("sh0", r0.GetOldTargetNodeShardId());
            UNIT_ASSERT_VALUES_EQUAL("n0", r0.GetOldTargetNodeShardNodeName());
            UNIT_ASSERT_VALUES_EQUAL("sh1", r1.GetOldTargetNodeShardId());
            UNIT_ASSERT_VALUES_EQUAL("n1", r1.GetOldTargetNodeShardNodeName());
            UNIT_ASSERT_VALUES_EQUAL("sh2", r2.GetOldTargetNodeShardId());
            UNIT_ASSERT_VALUES_EQUAL("n2", r2.GetOldTargetNodeShardNodeName());
        });

        executor.WriteTx([&] (TIndexTabletDatabase db) {
            db.DeleteResponseLogEntry(101, 11);
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TVector<NProtoPrivate::TResponseLogEntry> entries;
            UNIT_ASSERT(db.ReadResponseLog(entries));
            UNIT_ASSERT_VALUES_EQUAL(2, entries.size());
            UNIT_ASSERT_VALUES_EQUAL(100, entries[0].GetClientTabletId());
            UNIT_ASSERT_VALUES_EQUAL(10, entries[0].GetRequestId());
            UNIT_ASSERT_VALUES_EQUAL(102, entries[1].GetClientTabletId());
            UNIT_ASSERT_VALUES_EQUAL(12, entries[1].GetRequestId());
            const auto& r0 = entries[0].GetRenameNodeInDestinationResponse();
            const auto& r2 = entries[1].GetRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL("sh0", r0.GetOldTargetNodeShardId());
            UNIT_ASSERT_VALUES_EQUAL("n0", r0.GetOldTargetNodeShardNodeName());
            UNIT_ASSERT_VALUES_EQUAL("sh2", r2.GetOldTargetNodeShardId());
            UNIT_ASSERT_VALUES_EQUAL("n2", r2.GetOldTargetNodeShardNodeName());
        });

        executor.ReadTx([&] (TIndexTabletDatabase db) {
            TMaybe<NProtoPrivate::TResponseLogEntry> entry;
            UNIT_ASSERT(db.ReadResponseLogEntry(100, 10, entry));
            UNIT_ASSERT_VALUES_EQUAL(100, entry->GetClientTabletId());
            UNIT_ASSERT_VALUES_EQUAL(10, entry->GetRequestId());
            const auto& r = entry->GetRenameNodeInDestinationResponse();
            UNIT_ASSERT_VALUES_EQUAL("sh0", r.GetOldTargetNodeShardId());
            UNIT_ASSERT_VALUES_EQUAL("n0", r.GetOldTargetNodeShardNodeName());
        });
    }
}

}   // namespace NCloud::NFileStore::NStorage
