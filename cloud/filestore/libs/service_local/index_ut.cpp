#include "index.h"

#include <cloud/filestore/libs/service/filestore.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/folder/tempdir.h>

namespace NCloud::NFileStore {

namespace {

////////////////////////////////////////////////////////////////////////////////

struct TEnvironment
    : public NUnitTest::TBaseFixture
{
    ILoggingServicePtr Logging =
        CreateLoggingService("console", {TLOG_RESOURCES});
    TLog Log;

    const TTempDir TempDir;
    const TFsPath RootPath = TempDir.Path() / "root";
    const TFsPath StatePath = TempDir.Path() / "state";

    TEnvironment() : Log(Logging->CreateLog("INDEX_TEST"))
    {
        RootPath.MkDir();
        StatePath.MkDir();
    }

protected:
    bool CreateNestedDir(
        ui32 pathLen,
        TMap<TString, ui64>& nodeMap)
    {
        nodeMap.clear();

        RootPath.ForceDelete();
        RootPath.MkDir();

        StatePath.ForceDelete();
        StatePath.MkDir();

        TLocalIndex index(RootPath, StatePath, pathLen, false, 1000, Log);

        auto node = index.LookupNode(RootNodeId);
        UNIT_ASSERT_C(node, "Failed to lookup RootNode");

        auto path = RootPath;
        for (ui32 i = 0; i < pathLen; i++) {
            TString name = ToString(i);
            path = path / name;
            path.MkDir();

            auto childNode = TIndexNode::Create(*node, name);
            UNIT_ASSERT_C(childNode, "Failed to create node: " << name);

            STORAGE_DEBUG(
                "NodeId=" << childNode->GetNodeId() << " ,Name=" << name);

            auto inserted =
                index.TryInsertNode(childNode, node->GetNodeId(), name);
            UNIT_ASSERT_C(inserted, "Failed to insert node: " << name);

            nodeMap.emplace(name, childNode->GetNodeId());
            if (childNode->GetNodeId() <= node->GetNodeId()) {
                // Rarely inodes reused and not increased when new dir created
                // in this case we skip the test
                STORAGE_WARN(
                    "node id=" <<  node->GetNodeId() <<
                    " , child node id=" << childNode->GetNodeId());
                return false;
            }

            UNIT_ASSERT_LT_C(
                node->GetNodeId(),
                childNode->GetNodeId(),
                "node id=" <<  node->GetNodeId() <<
                " , child node id=" << childNode->GetNodeId());
            node = childNode;
        }
        return true;
    }

    void SafeCreateNestedDir(ui32 pathLen, TMap<TString, ui64>& nodeMap)
    {
        for (int i = 0; i < 10; i++) {
            if (CreateNestedDir(pathLen, nodeMap)) {
                return;
            }
            STORAGE_WARN("Failed to create nested dir in iteration #" << i);
        }
        UNIT_ASSERT_C(
            false,
            "Failed to create nested dir with increasing inode numbers");
    }

    bool CreateReversedNodeIdNestedDir(
        ui32 pathLen,
        TMap<TString, ui64>& nodeMap)
    {
        nodeMap.clear();

        RootPath.ForceDelete();
        RootPath.MkDir();

        StatePath.ForceDelete();
        StatePath.MkDir();

        TLocalIndex index(RootPath, StatePath, pathLen, false, 1000, Log);

        auto node = index.LookupNode(RootNodeId);
        UNIT_ASSERT_C(node, "Failed to lookup RootNode");

        auto path = RootPath;
        for (ui32 i = 0; i < pathLen; i++) {
            TString name = ToString(pathLen - 1 - i);
            path = RootPath / name;
            path.MkDir();

            if (i > 0) {
                TString prevName = ToString(pathLen - i);
                auto prevPath = RootPath / prevName;
                prevPath.RenameTo(RootPath / name / prevName);
            }
        }

        for (ui32 i = 0; i < pathLen; i++) {
            TString name = ToString(i);

            auto childNode = TIndexNode::Create(*node, name);
            UNIT_ASSERT_C(childNode, "Failed to create node: " << name);

            STORAGE_DEBUG(
                "NodeId=" << childNode->GetNodeId() << " ,Name=" << name);

            auto inserted =
                index.TryInsertNode(childNode, node->GetNodeId(), name);
            UNIT_ASSERT_C(inserted, "Failed to insert node: " << name);

            nodeMap.emplace(name, childNode->GetNodeId());
            if (i > 0 && node->GetNodeId() <= childNode->GetNodeId()) {
                // Rarely inodes reused and not increased when new dir created
                // in this case we skip the test
                STORAGE_WARN(
                    "node id=" <<  node->GetNodeId() <<
                    " , child node id=" << childNode->GetNodeId());
                return false;
            }
            node = childNode;
        }

        return true;
    }

    void SafeCreateReversedNodeIdNestedDir(ui32 pathLen, TMap<TString, ui64>& nodeMap)
    {
        for (int i = 0; i < 10; i++) {
            if (CreateReversedNodeIdNestedDir(pathLen, nodeMap)) {
                return;
            }
            STORAGE_WARN("Failed to create nested dir in iteration #" << i);
        }
        UNIT_ASSERT_C(
            false,
            "Failed to create nested dir with decreaing inode numbers");
    }

    void CheckNestedDir(ui32 pathLen, const TMap<TString, ui64>& nodeMap)
    {
        TLocalIndex index(RootPath, StatePath, pathLen, false, 1000, Log);
        auto node = index.LookupNode(RootNodeId);
        UNIT_ASSERT_C(node, "Failed to lookup root node");

        for (ui32 i = 0; i < pathLen; i++) {
            TString name = ToString(i);

            auto nodes = node->List(false /* don't ignore errors */);
            UNIT_ASSERT_VALUES_EQUAL(nodes.size(), 1);

            auto& [nodeName, nodeStat] = nodes[0];
            UNIT_ASSERT_VALUES_EQUAL(name, nodeName);

            auto it = nodeMap.find(nodeName);
            UNIT_ASSERT_C(it != nodeMap.end(), "node not found: " << nodeName);

            auto nodeId = it->second;
            STORAGE_DEBUG("Found node: " << nodeName << ", NodeId=" << nodeId);

            node = index.LookupNode(nodeId);
            UNIT_ASSERT_C(node,
                "Failed to lookup  node id: " << nodeId <<
                ", node: " << nodeName);
        }
    }

    void CheckMissingNodes(ui32 pathLen, const TVector<ui64>& nodeIds)
    {
        TLocalIndex index(RootPath, StatePath, pathLen, false, 1000, Log);
        auto node = index.LookupNode(RootNodeId);
        UNIT_ASSERT_C(node, "Failed to lookup root node");

        for (auto& nodeId: nodeIds) {
            node = index.LookupNode(nodeId);
            UNIT_ASSERT_C(!node, "Found node id: " << nodeId);
        }
    }
};

struct TNodeTableHeader
{
};

struct TNodeTableRecord
{
    ui64 NodeId = 0;
    ui64 ParentNodeId = 0;
    char Name[NAME_MAX + 1];
};

using TNodeTable = TPersistentTable<TNodeTableHeader, TNodeTableRecord>;

struct NodeLoaderStub
    : public INodeLoader
{

    TMap<ui64, TIndexNodePtr> NodeMap;

    TIndexNodePtr LoadNode(ui64 nodeId) const override
    {
        auto it = NodeMap.find(nodeId);
        if (it != NodeMap.end()) {
            return it->second;
        }
        return nullptr;
    }

    TString ToString() const override
    {
        return "NodeLoaderStub";
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TLocalIndex)
{
    Y_UNIT_TEST_F(ShouldRecoverNestedDir, TEnvironment)
    {
        ui32 pathLen = 10;
        TMap<TString, ui64> nodeMap;

        SafeCreateNestedDir(pathLen, nodeMap);
        CheckNestedDir(pathLen, nodeMap);
    }

    Y_UNIT_TEST_F(ShouldRecoverNestedDirWithReversedInodeOrder, TEnvironment)
    {
        ui32 pathLen = 10;
        TMap<TString, ui64> nodeMap;

        SafeCreateReversedNodeIdNestedDir(pathLen, nodeMap);
        CheckNestedDir(pathLen, nodeMap);
    }

    Y_UNIT_TEST_F(ShouldDiscardUnresolvedNodePath, TEnvironment)
    {
        ui32 pathLen = 10;
        TMap<TString, ui64> nodeMap;

        SafeCreateNestedDir(pathLen, nodeMap);

        TNodeTable nodeTable((StatePath / "nodes").GetPath(), pathLen);

        nodeTable.DeleteRecord(pathLen / 2);

        CheckNestedDir(pathLen / 2, nodeMap);

        TVector<ui64> missingNodes;
        for (ui32 i = pathLen / 2; i < pathLen; i++) {
            TString name = ToString(i);
            auto it = nodeMap.find(name);
            UNIT_ASSERT_C(it != nodeMap.end(), "Node " << name << " not found");

            missingNodes.push_back(it->second);
        }

        CheckMissingNodes(pathLen, missingNodes);
    }

    Y_UNIT_TEST_F(ShouldDiscardDeletedNodes, TEnvironment)
    {
        RootPath.ForceDelete();
        RootPath.MkDir();

        StatePath.ForceDelete();
        StatePath.MkDir();

        auto index = std::make_unique<TLocalIndex>(
            RootPath,
            StatePath,
            100,
            false,
            1000,
            Log);
        auto rootNode = index->LookupNode(RootNodeId);

        // create /dir1
        auto dir1 = RootPath / "dir1";
        dir1.MkDir();
        auto node1 = TIndexNode::Create(*rootNode, dir1.GetName());
        auto inserted =
            index->TryInsertNode(node1, RootNodeId, dir1.GetName());
        UNIT_ASSERT_C(inserted, "Failed to insert node: " << dir1.GetName());

        // create /dir2/dir3/dir4
        auto dir2 = RootPath / "dir2";
        dir2.MkDir();
        auto node2 = TIndexNode::Create(*rootNode, dir2.GetName());
        inserted =
            index->TryInsertNode(node2, RootNodeId, dir2.GetName());
        UNIT_ASSERT_C(inserted, "Failed to insert node: " << dir2.GetName());

        auto dir3 = dir2 / "dir3";
        dir3.MkDir();
        auto node3 = TIndexNode::Create(*node2, dir3.GetName());
        inserted =
            index->TryInsertNode(node3, node2->GetNodeId(), dir3.GetName());
        UNIT_ASSERT_C(inserted, "Failed to insert node: " << dir3.GetName());

        auto dir4 = dir3 / "dir4";
        dir4.MkDir();
        auto node4 = TIndexNode::Create(*node3, dir4.GetName());
        inserted =
            index->TryInsertNode(node4, node3->GetNodeId(), dir4.GetName());
        UNIT_ASSERT_C(inserted, "Failed to insert node: " << dir4.GetName());

        // delete dir3
        dir3.ForceDelete();
        index = std::make_unique<TLocalIndex>(
            RootPath,
            StatePath,
            100,
            false,
            1000,
            Log);

        // /dir1 and /dir2 restored
        UNIT_ASSERT_C(index->LookupNode(node1->GetNodeId()),
            "Failed to lookup  node id: " << node1->GetNodeId() <<
            ", node: " << dir1.GetName());
        UNIT_ASSERT_C(index->LookupNode(node2->GetNodeId()),
            "Failed to lookup  node id: " << node1->GetNodeId() <<
            ", node: " << dir2.GetName());

        // dir3/dir4 discarded
        UNIT_ASSERT_C(!index->LookupNode(node3->GetNodeId()),
            "Did not failed to lookup node id: " << node3->GetNodeId() <<
            ", node: " << dir3.GetName());
        UNIT_ASSERT_C(!index->LookupNode(node4->GetNodeId()),
            "Did not failed to lookup node id: " << node4->GetNodeId() <<
            ", node: " << dir4.GetName());
    }

    Y_UNIT_TEST_F(ShouldLoadNodes, TEnvironment)
    {
        RootPath.ForceDelete();
        RootPath.MkDir();

        auto nodeLoader = std::make_shared<NodeLoaderStub>();

        auto index = std::make_unique<TLocalIndex>(
            RootPath,
            StatePath,
            100,
            true, // openNodeByHandleEnabled
            10,   // nodeCleanupBatchSize
            Log,
            nodeLoader);

        auto rootNode = index->LookupNode(RootNodeId);
        UNIT_ASSERT_C(rootNode, "Failed to lookup root node");

        auto nodeAPath = RootPath / "a";
        nodeAPath.MkDir();
        auto nodeA = TIndexNode::Create(*rootNode, nodeAPath.GetName());
        UNIT_ASSERT(nodeA);

        auto node = index->LookupNode(nodeA->GetNodeId());
        UNIT_ASSERT(!node);

        nodeLoader->NodeMap.emplace(nodeA->GetNodeId(), nodeA);
        node = index->LookupNode(nodeA->GetNodeId());
        UNIT_ASSERT_C(node, "failed to lookup node a");

    }

    Y_UNIT_TEST_F(ShouldCleanupNodes, TEnvironment)
    {
        RootPath.ForceDelete();
        RootPath.MkDir();

        auto nodeLoader = std::make_shared<NodeLoaderStub>();

        auto index = std::make_unique<TLocalIndex>(
            RootPath,
            StatePath,
            8,
            true, // openNodeByHandleEnabled
            2,    // nodeCleanupBatchSize
            Log,
            nodeLoader);

        auto rootNode = index->LookupNode(RootNodeId);
        UNIT_ASSERT_C(rootNode, "Failed to lookup root node");

        TVector<TIndexNodePtr> nodes;
        for (ui32 i = 0; i < 10; i++) {
            auto path = RootPath / ToString(i);
            path.MkDir();
            auto node = TIndexNode::Create(*rootNode, path.GetName());
            UNIT_ASSERT(node);
            nodes.push_back(std::move(node));
        }

        auto checkNodeInCache = [&](ui32 nodeIndex, bool isNodeInCache) {
            UNIT_ASSERT_VALUES_EQUAL_C(
                isNodeInCache ? 2 : 1,
                nodes[nodeIndex].use_count(),
                "Node #" << nodeIndex << ", NodeId=" << nodes[nodeIndex]->GetNodeId()
                << (isNodeInCache ? "doesn't " :  " ") << "exist in node cache");
        };

        // fill node cache RootNode + 6 nodes = 7/8 nodes
        for (ui32 i = 0; i < 6; i++) {
            auto inserted = index->TryInsertNode(nodes[i], RootNodeId, ToString(i));
            UNIT_ASSERT(inserted);
        }

        // check nodes in cache
        for (ui32 i = 0; i < 6; i++) {
            {
                auto node = index->LookupNode(nodes[i]->GetNodeId());
                UNIT_ASSERT_C(
                    node,
                    "Node #" << i << ", NodeId=" << nodes[i]->GetNodeId()
                             << " doesn't exist in node cache");
            }
            checkNodeInCache(i, true);
        }

        // insert extra node cache node is full root node + 7 nodes
        UNIT_ASSERT(index->TryInsertNode(nodes[6], RootNodeId, ToString(6)));

        // Trigger initial nodeCleanupBatchSize (2) nodes cleanup
        UNIT_ASSERT(index->LookupNode(nodes[6]->GetNodeId()));
        checkNodeInCache(0, false);
        checkNodeInCache(1, false);
        checkNodeInCache(2, true);

        // Trigger single node cleanup
        UNIT_ASSERT(index->LookupNode(nodes[6]->GetNodeId()));
        checkNodeInCache(2, false);
        checkNodeInCache(3, true);

        // Trigger single node cleanup
        UNIT_ASSERT(index->LookupNode(nodes[6]->GetNodeId()));
        checkNodeInCache(3, false);
        checkNodeInCache(4, true);

        // Node cache reduced to half of it's size no more cleanup triggered
        UNIT_ASSERT(index->LookupNode(nodes[6]->GetNodeId()));
        checkNodeInCache(4, true);
        checkNodeInCache(5, true);
    }
};

}   // namespace NCloud::NFileStore
