#include "tablet.h"

#include <cloud/filestore/libs/storage/tablet/model/mixed_blocks.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <contrib/ydb/core/base/counters.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TTxStats
{
    i64 ROCacheHitCount;
    i64 ROCacheMissCount;
    i64 RWCount;
    i64 NodesCount;
    i64 NodeRefsCount;
    i64 NodeAttrsCount;
    bool IsExhaustive;
};

TBlobMetaMapStats GetBlobMetaMapStats(TTestEnv& env, TIndexTabletClient& tablet)
{
    TBlobMetaMapStats stats;
    TTestRegistryVisitor visitor;

    tablet.SendRequest(tablet.CreateUpdateCounters());
    env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
    env.GetRegistry()->Visit(TInstant::Zero(), visitor);

    visitor.ValidateExpectedCountersWithPredicate({
        {{{"filesystem", "test"}, {"sensor", "MixedIndexLoadedRanges"}},
         [&stats](i64 value)
         {
             stats.LoadedRanges = value;
             return true;
         }},
        {{{"filesystem", "test"}, {"sensor", "MixedIndexOffloadedRanges"}},
         [&stats](i64 value)
         {
             stats.OffloadedRanges = value;
             return true;
         }},
    });
    return stats;
}

TTxStats GetTxStats(TTestEnv& env, TIndexTabletClient& tablet)
{
    TTxStats stats;
    TTestRegistryVisitor visitor;

    tablet.SendRequest(tablet.CreateUpdateCounters());
    env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
    env.GetRegistry()->Visit(TInstant::Zero(), visitor);

    visitor.ValidateExpectedCountersWithPredicate({
        {{{"filesystem", "test"},
          {"sensor", "InMemoryIndexStateROCacheHitCount"}},
         [&stats](i64 value)
         {
             stats.ROCacheHitCount = value;
             return true;
         }},
        {{{"filesystem", "test"},
          {"sensor", "InMemoryIndexStateROCacheMissCount"}},
         [&stats](i64 value)
         {
             stats.ROCacheMissCount = value;
             return true;
         }},
        {{{"filesystem", "test"}, {"sensor", "InMemoryIndexStateRWCount"}},
         [&stats](i64 value)
         {
             stats.RWCount = value;
             return true;
         }},
        {{{"filesystem", "test"}, {"sensor", "InMemoryIndexStateNodesCount"}},
         [&stats](i64 value)
         {
             stats.NodesCount = value;
             return true;
         }},
        {{{"filesystem", "test"}, {"sensor", "InMemoryIndexStateNodeRefsCount"}},
         [&stats](i64 value)
         {
             stats.NodeRefsCount = value;
             return true;
         }},
        {{{"filesystem", "test"}, {"sensor", "InMemoryIndexStateNodeAttrsCount"}},
         [&stats](i64 value)
         {
             stats.NodeAttrsCount = value;
             return true;
         }},
        {{{"filesystem", "test"}, {"sensor", "InMemoryIndexStateIsExhaustive"}},
         [&stats](i64 value)
         {
             stats.IsExhaustive = value;
             return true;
         }},
    });
    return stats;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_NodesCache)
{
    Y_UNIT_TEST(ShouldUpdateAndEvictCacheUponCreateNode)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        // Create node should have populated the cache, thus the next
        // GetNodeAttr should not trigger a transaction.
        {
            auto node =
                tablet.GetNodeAttr(RootNodeId, "test")->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(id, node.GetId());
            UNIT_ASSERT(node.GetType() == NProto::E_REGULAR_NODE);
        }

        // The following GetNodeAttr should not produce a transaction either, as
        // the requested node is already in the cache.
        {
            auto node = tablet.GetNodeAttr(RootNodeId, "")->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(RootNodeId, node.GetId());
            UNIT_ASSERT(node.GetType() == NProto::E_DIRECTORY_NODE);
        }

        // Now the Nodes table has two entries: Root and test.
        // The NodeRefs table has one entry: Root -> test.
        //
        // Creating another node should evict the first one from the cache
        tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test2"));
        // Thus, the following GetNodeAttr should be a cache miss
        UNIT_ASSERT_VALUES_EQUAL(
            id,
            tablet.GetNodeAttr(RootNodeId, "test")->Record.GetNode().GetId());
        // ListNodes can not be performed using in-memory cache
        tablet.ListNodes(RootNodeId);

        // Two out of three GetNodeAttr calls should have been performed using
        // the cache. ListNodes is a cache miss also.
        auto statsAfter = GetTxStats(env, tablet);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(2, statsAfter.RWCount - statsBefore.RWCount);
        UNIT_ASSERT_VALUES_EQUAL(1, statsAfter.NodeRefsCount - statsBefore.NodeRefsCount);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.NodesCount - statsBefore.NodesCount);
    }

    // Note: this test does not check the cache eviction policy, as cache size
    // changes are not expected upon node rename
    Y_UNIT_TEST(ShouldUpdateCacheUponRenameNode)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(3);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(2);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"));
        // RO transaction, cache hit
        auto ctime =
            tablet.GetNodeAttr(RootNodeId, "")->Record.GetNode().GetCTime();

        // Upon rename node the wall clock time is used to update the ctime, so
        // until this is replaced with ctx.Now, we need to perform sleep, not
        // tablet.AdvanceTime
        Sleep(TDuration::Seconds(2));

        // RW transaction, Nodes table has 2 entries: Root and test2, NodeRefs
        // has 1 entry: Root -> test2
        tablet.RenameNode(RootNodeId, "test", RootNodeId, "test2");

        // RO transaction, cache hit
        auto newCtime =
            tablet.GetNodeAttr(RootNodeId, "")->Record.GetNode().GetCTime();
        UNIT_ASSERT_GT(newCtime, ctime);

        // GetNodeAttr of a non-existing node can not be performed using the
        // cache, this is a RO transaction, cache miss
        tablet.AssertGetNodeAttrFailed(RootNodeId, "test");

        // Two out of three should have been performed using the cache
        auto statsAfter = GetTxStats(env, tablet);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(2, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponUnlinkNode)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(3);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(3);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id1 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));

        // Both nodes are in the cache. RO transaction, cache hit
        tablet.GetNodeAttr(id1, "");
        tablet.GetNodeAttr(id2, "");
        tablet.GetNodeAttr(RootNodeId, "test1");
        tablet.GetNodeAttr(RootNodeId, "test2");

        // RW transactions, cache should be updated and nodes should be removed
        tablet.UnlinkNode(RootNodeId, "test1", false);
        tablet.UnlinkNode(RootNodeId, "test2", false);

        // All four requests should have been performed without the cache
        tablet.AssertGetNodeAttrFailed(id1, "");
        tablet.AssertGetNodeAttrFailed(id2, "");
        tablet.AssertGetNodeAttrFailed(RootNodeId, "test1");
        tablet.AssertGetNodeAttrFailed(RootNodeId, "test2");

        // Four out of eight GetNodeAttr calls should have been performed using
        // the cache. Other four are cache misses and should have failed
        auto statsAfter = GetTxStats(env, tablet);
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            4,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(4, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponDestroySession)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(2);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        auto handle = tablet.CreateHandle(id, TCreateHandleArgs::RDWR);

        // Cache hit
        tablet.GetNodeAttr(id, "")->Record.GetNode();

        tablet.UnlinkNode(RootNodeId, "test", false);
        // Should work as there is an existing handle
        tablet.GetNodeAttr(id, "");

        // Upon session destruction the node should be removed from the cache as
        // well as the localDB
        tablet.DestroySession();

        tablet.InitSession("client", "session2");

        UNIT_ASSERT_VALUES_EQUAL(
            E_FS_NOENT,
            tablet.AssertGetNodeAttrFailed(id, "")->GetError().GetCode());

        auto statsAfter = GetTxStats(env, tablet);
        // First two GetNodeAttr calls should have been performed with the cache
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        // Last GetNodeAttr call should have been a cache miss as it is not
        // supposed to be present in the cache
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        // CreateNode, CreateHandle, UnlinkNode, DestroySession and InitSession
        // are RW txs
        UNIT_ASSERT_VALUES_EQUAL(5, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponResetSession)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(2);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        auto handle = tablet.CreateHandle(id, TCreateHandleArgs::RDWR);

        // Cache hit
        tablet.GetNodeAttr(id, "")->Record.GetNode();

        tablet.UnlinkNode(RootNodeId, "test", false);
        // Should work as there is an existing handle
        tablet.GetNodeAttr(id, "");

        // Upon reset session the node should be removed from the cache as well
        // as the localDB
        tablet.ResetSession("");

        tablet.InitSession("client", "session2");

        UNIT_ASSERT_VALUES_EQUAL(
            E_FS_NOENT,
            tablet.AssertGetNodeAttrFailed(id, "")->GetError().GetCode());

        auto statsAfter = GetTxStats(env, tablet);
        // First two GetNodeAttr calls should have been performed with the cache
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        // Last GetNodeAttr call should have been a cache miss as it is not
        // supposed to be present in the cache
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        // CreateNode, CreateHandle, UnlinkNode, DestroySession and InitSession
        // are RW txs
        UNIT_ASSERT_VALUES_EQUAL(5, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponSetNodeAttr)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();

        // RW transaction, updates cache
        tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(77));

        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            77,
            tablet.GetNodeAttr(id, "")->Record.GetNode().GetSize());

        auto statsAfter = GetTxStats(env, tablet);
        // The only one GetNodeAttr call should have been performed with the
        // cache
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        // There was no cache misses
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        // CreateNode and SetNodeAttr are RW txs
        UNIT_ASSERT_VALUES_EQUAL(2, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponSetNodeXAttr)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        storageConfig.SetInMemoryIndexCacheNodeAttrsCapacity(2);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();

        // RW transaction, updates cache
        tablet.SetNodeXAttr(id, "user.test", "value");

        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            "value",
            tablet.GetNodeXAttr(id, "user.test")->Record.GetValue());

        // RW transaction, updates cache
        tablet.SetNodeXAttr(id, "user.test", "value2");
        // RW transaction, updates cache
        tablet.SetNodeXAttr(id, "user.test2", "value");

        // RO transactions, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            "value2",
            tablet.GetNodeXAttr(id, "user.test")->Record.GetValue());
        UNIT_ASSERT_VALUES_EQUAL(
            "value",
            tablet.GetNodeXAttr(id, "user.test2")->Record.GetValue());

        // RW transaction, evicts the first entry from the cache (because of the
        // LRU policy)
        tablet.SetNodeXAttr(id, "user.test3", "value");

        // RO transaction, cache miss
        UNIT_ASSERT_VALUES_EQUAL(
            "value2",
            tablet.GetNodeXAttr(id, "user.test")->Record.GetValue());

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            3,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(5, statsAfter.RWCount - statsBefore.RWCount);
        UNIT_ASSERT_VALUES_EQUAL(2, statsAfter.NodeAttrsCount - statsBefore.NodeAttrsCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponRemoveNodeXAttr)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        storageConfig.SetInMemoryIndexCacheNodeAttrsCapacity(2);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();

        // RW transaction, updates cache
        tablet.SetNodeXAttr(id, "user.test", "value");

        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            "value",
            tablet.GetNodeXAttr(id, "user.test")->Record.GetValue());

        // RW transaction, updates cache
        tablet.RemoveNodeXAttr(id, "user.test");

        // RO transaction, cache miss
        UNIT_ASSERT_VALUES_EQUAL(
            MAKE_FILESTORE_ERROR(NProto::E_FS_NOXATTR),
            tablet.AssertGetNodeXAttrFailed(id, "user.test")
                ->GetError()
                .GetCode());

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(3, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponWriteAndCreateHandle)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        // RW transaction
        auto handle = tablet.CreateHandle(id, TCreateHandleArgs::RDWR)
                          ->Record.GetHandle();
        // RW transaction, updates file size
        tablet.WriteData(handle, 0, 99, '0');

        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            99,
            tablet.GetNodeAttr(id, "")->Record.GetNode().GetSize());

        // RW transaction, updates cache
        tablet.CreateHandle(
            id,
            "",
            TCreateHandleArgs::CREATE | TCreateHandleArgs::TRUNC);

        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet.GetNodeAttr(id, "")->Record.GetNode().GetSize());

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(4, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponDestroyHandle)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        // RW transaction, updates cache
        auto handle = tablet.CreateHandle(id, TCreateHandleArgs::RDWR)
                          ->Record.GetHandle();

        // RW transaction, updates cache
        tablet.UnlinkNode(RootNodeId, "test", false);
        // RO transaction, cache hit
        tablet.GetNodeAttr(id, "");
        // RW transaction, updates cache
        tablet.DestroyHandle(handle);
        // RO transaction, cache miss
        tablet.AssertGetNodeAttrFailed(id, "");

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(4, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponAddBlob)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        // RW transaction, updates cache
        auto handle = tablet.CreateHandle(id, TCreateHandleArgs::RDWR)
                          ->Record.GetHandle();

        // explicitly write data to blob storage
        const auto dataSize = DefaultBlockSize * BlockGroupSize;
        NKikimr::TLogoBlobID blobId;
        ui64 commitId = 0;
        {
            auto gbi = tablet
                           .GenerateBlobIds(
                               id,
                               handle,
                               /* offset */ 0,
                               /* length */ dataSize)
                           ->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, gbi.BlobsSize());
            commitId = gbi.GetCommitId();

            blobId = LogoBlobIDFromLogoBlobID(gbi.GetBlobs(0).GetBlobId());
            auto evPut = std::make_unique<TEvBlobStorage::TEvPut>(
                blobId,
                TString(dataSize, 'x'),
                TInstant::Max(),
                NKikimrBlobStorage::UserData);
            NKikimr::TActorId proxy =
                MakeBlobStorageProxyID(gbi.GetBlobs(0).GetBSGroupId());
            auto evPutSender =
                env.GetRuntime().AllocateEdgeActor(proxy.NodeId());
            env.GetRuntime().Send(CreateEventForBSProxy(
                evPutSender,
                proxy,
                evPut.release(),
                blobId.Cookie()));
        }

        // RW transaction, updates cache, subsequently calls AddData
        tablet.AddData(
            id,
            handle,
            /* offset */ 0,
            /* length */ dataSize,
            TVector<NKikimr::TLogoBlobID>{blobId},
            commitId,
            /* unaligned parts */ TVector<NProtoPrivate::TFreshDataRange>{});

        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            dataSize,
            tablet.GetNodeAttr(id, "")->Record.GetNode().GetSize());

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(4, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponAllocateData)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        // RW transaction, updates cache
        auto handle = tablet.CreateHandle(id, TCreateHandleArgs::RDWR)
                          ->Record.GetHandle();

        auto newSize = DefaultBlockSize * BlockGroupSize;
        // RW transaction, updates cache
        tablet.AllocateData(
            handle,
            0,
            newSize,
            ProtoFlag(NProto::TAllocateDataRequest::F_ZERO_RANGE));

        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            newSize,
            tablet.GetNodeAttr(id, "")->Record.GetNode().GetSize());

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(3, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponGetNodeAttr)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetUid(77));

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RO transaction, populates the cache
        UNIT_ASSERT_VALUES_EQUAL(
            77,
            tablet.GetNodeAttr(id, "")->Record.GetNode().GetUid());
        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            77,
            tablet.GetNodeAttr(id, "")->Record.GetNode().GetUid());

        // reading node by inode + name should also populate the cache:
        // RO transaction, populates the cache
        UNIT_ASSERT_VALUES_EQUAL(
            77,
            tablet.GetNodeAttr(RootNodeId, "test")->Record.GetNode().GetUid());
        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            77,
            tablet.GetNodeAttr(RootNodeId, "test")->Record.GetNode().GetUid());

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponGetNodeXAttr)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        tablet.SetNodeXAttr(id, "user.test", "value");

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RO transaction, populates the cache
        UNIT_ASSERT_VALUES_EQUAL(
            "value",
            tablet.GetNodeXAttr(id, "user.test")->Record.GetValue());

        // RO transaction, cache hit
        UNIT_ASSERT_VALUES_EQUAL(
            "value",
            tablet.GetNodeXAttr(id, "user.test")->Record.GetValue());

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUpdateCacheUponListNodes)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(3);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(2);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test1"))
                       ->Record.GetNode()
                       .GetId();
        auto id2 = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test2"))
                       ->Record.GetNode()
                       .GetId();

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RO transaction, populates the cache
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            tablet.ListNodes(RootNodeId)->Record.NodesSize());

        // RO transactions, cache hits
        tablet.GetNodeAttr(RootNodeId, "");
        tablet.GetNodeAttr(RootNodeId, "test1");
        tablet.GetNodeAttr(RootNodeId, "test2");
        tablet.GetNodeAttr(id1, "");
        tablet.GetNodeAttr(id2, "");

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            5,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.RWCount - statsBefore.RWCount);
    }

    Y_UNIT_TEST(ShouldUseNodeRefsCacheIfOneIsExhaustive)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(100);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(100);
        storageConfig.SetInMemoryIndexCacheLoadOnTabletStart(true);
        storageConfig.SetInMemoryIndexCacheLoadOnTabletStartRowsPerTx(1);
        storageConfig.SetInMemoryIndexCacheLoadSchedulePeriod(
            TDuration::Seconds(1).MilliSeconds());
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test1"));

        env.GetRuntime().ClearCounters();
        tablet.RebootTablet();

        for (int i = 0; i < 10; ++i) {
            tablet.AdvanceTime(TDuration::Seconds(1));
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        }

        tablet.InitSession("client", "session");

        // It will take 1 iteration to load all the nodeRefs (root -> test)1
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.GetRuntime().GetCounter(
                TEvIndexTabletPrivate::EEvents::EvLoadNodeRefs));
        // It also will take 2 iterations to load all the nodes (root and test1)
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            env.GetRuntime().GetCounter(
                TEvIndexTabletPrivate::EEvents::EvLoadNodes));

        auto statsBefore = GetTxStats(env, tablet);

        // The noderefs cache is exhaustive thus list nodes should be a cache
        // hit
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            tablet.ListNodes(RootNodeId)->Record.NodesSize());

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.RWCount - statsBefore.RWCount);

        statsBefore = statsAfter;

        auto id2 =
            tablet.CreateNode(TCreateNodeArgs::Directory(RootNodeId, "test2"))
                ->Record.GetNode()
                .GetId();
        tablet.CreateNode(TCreateNodeArgs::File(id2, "test3"));
        tablet.CreateNode(TCreateNodeArgs::File(id2, "test4"));
        tablet.CreateNode(TCreateNodeArgs::File(id2, "test5"));

        /*
        |- test1
        |- test2
            |- test3
            |- test4
            |- test5
        */

        // The NodeRefs cache is still exhaustive thus list nodes should be a
        // cache hit
        UNIT_ASSERT_VALUES_EQUAL(3, tablet.ListNodes(id2)->Record.NodesSize());
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            tablet.ListNodes(RootNodeId)->Record.NodesSize());

        statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            2,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT(statsAfter.IsExhaustive);

        // Now let us ensure that the cache is evicted
        for (int i = 0; i < 100; ++i) {
            tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, std::to_string(i)));
        }

        statsBefore = statsAfter;

        tablet.ListNodes(RootNodeId);

        statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
        UNIT_ASSERT(!statsAfter.IsExhaustive);
    }

    Y_UNIT_TEST(ShouldCalculateCacheCapacityBasedOnRatio)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        // max(1000, 1024 / 10) = 1000
        storageConfig.SetInMemoryIndexCacheNodesCapacity(1000);
        storageConfig.SetInMemoryIndexCacheNodesToNodesCapacityRatio(10);

        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(20);
        storageConfig.SetInMemoryIndexCacheNodesToNodeRefsCapacityRatio(0);

        // max(100, 1024 / 8) = 128
        storageConfig.SetInMemoryIndexCacheNodeAttrsCapacity(100);
        storageConfig.SetInMemoryIndexCacheNodesToNodeAttrsCapacityRatio(8);

        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            TFileSystemConfig{.NodeCount = 1024});
        tablet.RebootTablet();

        tablet.SendRequest(tablet.CreateUpdateCounters());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        TTestRegistryVisitor visitor;
        env.GetRegistry()->Visit(TInstant::Zero(), visitor);
        visitor.ValidateExpectedCounters({
            {{{"filesystem", "test"},
              {"sensor", "InMemoryIndexStateNodesCapacity"}},
             1000},
            {{{"filesystem", "test"},
              {"sensor", "InMemoryIndexStateNodeRefsCapacity"}},
             20},
            {{{"filesystem", "test"},
              {"sensor", "InMemoryIndexStateNodeAttrsCapacity"}},
             128},
        });
    }

    Y_UNIT_TEST(ShouldUseInMemoryCacheAndMixedBlocksCacheForReads)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        storageConfig.SetInMemoryIndexCacheNodeAttrsCapacity(2);
        storageConfig.SetMixedBlocksOffloadedRangesCapacity(1024);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto statsBefore = GetTxStats(env, tablet);

        // RW transaction, populates the cache
        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();

        // RW transaction, updates cache
        auto handle = tablet.CreateHandle(id, TCreateHandleArgs::RDWR)
                          ->Record.GetHandle();

        // RW transaction
        tablet.WriteData(handle, 0, 1_MB, '0');

        // RO transaction, cache miss
        tablet.ReadData(handle, 0, 1_MB);
        // RO transaction, cache hit
        tablet.ReadData(handle, 0, 1_MB);

        auto statsAfter = GetTxStats(env, tablet);

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheHitCount - statsBefore.ROCacheHitCount);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            statsAfter.ROCacheMissCount - statsBefore.ROCacheMissCount);
    }

    Y_UNIT_TEST(ShouldNotLeakMixedIndexCacheInCaseOfUnsuccessfulReads)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetInMemoryIndexCacheEnabled(true);
        storageConfig.SetInMemoryIndexCacheNodesCapacity(2);
        storageConfig.SetInMemoryIndexCacheNodeRefsCapacity(1);
        storageConfig.SetInMemoryIndexCacheNodeAttrsCapacity(2);
        storageConfig.SetMixedBlocksOffloadedRangesCapacity(1024);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"))
                      ->Record.GetNode()
                      .GetId();
        auto handle = tablet.CreateHandle(id, TCreateHandleArgs::RDWR)
                          ->Record.GetHandle();
        tablet.WriteData(handle, 0, 1_MB, '0');

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        // Now no data is present in the mixed index cache
        tablet.ReadData(handle, 0, 256_KB);
        {
            auto statsBeforeTx = GetTxStats(env, tablet);
            // Reading the same data again should be a cache hit
            tablet.ReadData(handle, 0, 256_KB);
            // Not enough data is present in the mixed index cache to satisfy
            // the request, thus it should be a cache miss
            tablet.ReadData(handle, 0, 1_MB);
            auto statsAfterTx = GetTxStats(env, tablet);
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                statsAfterTx.ROCacheHitCount - statsBeforeTx.ROCacheHitCount);
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                statsAfterTx.ROCacheMissCount - statsBeforeTx.ROCacheMissCount);
        }

        // All 4 ranges (1MB / 256KB) should have been offloaded to the mixed
        // index cache
        auto stats = GetBlobMetaMapStats(env, tablet);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.LoadedRanges);
        UNIT_ASSERT_VALUES_EQUAL(4, stats.OffloadedRanges);
    }
}

}   // namespace NCloud::NFileStore::NStorage
