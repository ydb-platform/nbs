#include "helpers.h"
#include "tablet_state_cache.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TInMemoryIndexStateTest)
{
    const ui64 commitId1 = 1;
    const ui64 commitId2 = 2;

    const ui64 nodeId1 = 101;
    const ui64 nodeId2 = 102;

    const NProto::TNode nodeAttrs1 = CreateRegularAttrs(0666, 1, 2);
    const NProto::TNode nodeAttrs2 = CreateRegularAttrs(0666, 3, 4);

    //
    // Nodes
    //
    Y_UNIT_TEST(ShouldPopulateNodes)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(1, 0, 0);

        TMaybe<IIndexTabletDatabase::TNode> node;
        UNIT_ASSERT(!state.ReadNode(nodeId1, commitId2, node));

        NProto::TNode realNode = nodeAttrs1;

        state.UpdateState({TInMemoryIndexState::TWriteNodeRequest{
            .NodeId = nodeId1,
            .Row = {
                .CommitId = commitId2,
                .Node = realNode,
            }}});

        UNIT_ASSERT(state.ReadNode(nodeId1, commitId2, node));
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, node->NodeId);
        UNIT_ASSERT_VALUES_EQUAL(
            realNode.DebugString(),
            node->Attrs.DebugString());
        UNIT_ASSERT_VALUES_EQUAL(commitId2, node->MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, node->MaxCommitId);

        // Though the node is present in Nodes table, it is not visible at
        // commitId1. So read from cache is considered successful, but the node
        // that was read should be empty
        node = {};
        UNIT_ASSERT(state.ReadNode(nodeId1, commitId1, node));
        UNIT_ASSERT(node.Empty());
    }

    Y_UNIT_TEST(ShouldEvictNodes)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(1, 0, 0);

        state.UpdateState(
            {TInMemoryIndexState::TWriteNodeRequest{
                 .NodeId = nodeId1,
                 .Row =
                     {
                         .CommitId = commitId1,
                         .Node = nodeAttrs1,
                     }},
             TInMemoryIndexState::TWriteNodeRequest{
                 .NodeId = nodeId2,
                 .Row = {
                     .CommitId = commitId1,
                     .Node = nodeAttrs2,
                 }}});

        TMaybe<IIndexTabletDatabase::TNode> node;

        UNIT_ASSERT(!state.ReadNode(nodeId1, commitId1, node));
    }

    Y_UNIT_TEST(ShouldDeleteNodes)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(1, 0, 0);

        state.UpdateState({TInMemoryIndexState::TWriteNodeRequest{
            .NodeId = nodeId1,
            .Row = {
                .CommitId = commitId1,
                .Node = nodeAttrs1,
            }}});

        TMaybe<IIndexTabletDatabase::TNode> node;
        UNIT_ASSERT(state.ReadNode(nodeId1, commitId1, node));
        UNIT_ASSERT(node.Defined());

        state.UpdateState(
            {TInMemoryIndexState::TDeleteNodeRequest{.NodeId = nodeId1}});

        UNIT_ASSERT(!state.ReadNode(1, commitId1, node));
    }

    const TString attrName1 = "name1";
    const TString attrName2 = "name2";

    const TString attrValue1 = "value1";
    const TString attrValue2 = "value2";

    const ui64 attrVersion1 = 301;
    const ui64 attrVersion2 = 302;

    //
    // NodeAttrs
    //
    Y_UNIT_TEST(ShouldPopulateNodeAttrs)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 1, 0);

        TMaybe<IIndexTabletDatabase::TNodeAttr> attr;
        UNIT_ASSERT(!state.ReadNodeAttr(nodeId1, commitId2, attrName1, attr));

        state.UpdateState({TInMemoryIndexState::TWriteNodeAttrsRequest{
            .NodeAttrsKey =
                {
                    nodeId1,
                    attrName1,
                },
            .NodeAttrsRow = {
                commitId2,
                attrValue1,
                attrVersion1,
            }}});

        UNIT_ASSERT(state.ReadNodeAttr(nodeId1, commitId2, attrName1, attr));
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, attr->NodeId);
        UNIT_ASSERT_VALUES_EQUAL(attrName1, attr->Name);
        UNIT_ASSERT_VALUES_EQUAL(attrValue1, attr->Value);
        UNIT_ASSERT_VALUES_EQUAL(commitId2, attr->MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, attr->MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(attrVersion1, attr->Version);

        // Though the node is present in NodeAttrs table, it is not visible at
        // commitId1. So read from cache is considered successful, but the node
        // that was read should be empty
        attr = {};
        UNIT_ASSERT(state.ReadNodeAttr(nodeId1, commitId1, attrName1, attr));
        UNIT_ASSERT(attr.Empty());
    }

    Y_UNIT_TEST(ShouldEvictNodeAttrs)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 1, 0);

        state.UpdateState(
            {TInMemoryIndexState::TWriteNodeAttrsRequest{
                 .NodeAttrsKey = {nodeId1, attrName1},
                 .NodeAttrsRow = {commitId1, attrValue1, attrVersion1},
             },
             TInMemoryIndexState::TWriteNodeAttrsRequest{
                 .NodeAttrsKey = {nodeId2, attrName2},
                 .NodeAttrsRow = {commitId1, attrValue2, attrVersion2},
             }});

        TMaybe<IIndexTabletDatabase::TNodeAttr> attr;

        UNIT_ASSERT(!state.ReadNodeAttr(nodeId1, commitId1, attrName1, attr));
    }

    Y_UNIT_TEST(ShouldDeleteNodeAttrs)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 1, 0);

        state.UpdateState({TInMemoryIndexState::TWriteNodeAttrsRequest{
            .NodeAttrsKey = {nodeId1, attrName1},
            .NodeAttrsRow = {commitId1, attrValue1, attrVersion1},
        }});

        TMaybe<IIndexTabletDatabase::TNodeAttr> attr;
        UNIT_ASSERT(state.ReadNodeAttr(nodeId1, commitId1, attrName1, attr));
        UNIT_ASSERT(attr.Defined());

        state.UpdateState(
            {TInMemoryIndexState::TDeleteNodeAttrsRequest{nodeId1, attrName1}});

        UNIT_ASSERT(!state.ReadNodeAttr(nodeId1, commitId1, attrName1, attr));
    }

    const TVector<TString> nodeNames = {"node1", "node2", "node3", "node4"};
    const TVector<TString> shardNodeNames = {
        "shard1",
        "shard2",
        "shard3",
        "shard4"};
    const TVector<TString> shardIds = {
        "shardId1",
        "shardId2",
        "shardId3",
        "shardId4"};
    const TVector<ui64> rootNodeIds = {1, 2, 3, 4};
    const TVector<ui64> childNodeIds = {1001, 1002, 1003, 1004};

    namespace {

    void CheckNodeRef(
        const TInMemoryIndexState::TWriteNodeRefsRequest& request,
        IIndexTabletDatabase::TNodeRef& ref)
    {
        UNIT_ASSERT_VALUES_EQUAL(request.NodeRefsKey.NodeId, ref.NodeId);
        UNIT_ASSERT_VALUES_EQUAL(request.NodeRefsKey.Name, ref.Name);
        UNIT_ASSERT_VALUES_EQUAL(request.NodeRefsRow.ChildId, ref.ChildNodeId);
        UNIT_ASSERT_VALUES_EQUAL(request.NodeRefsRow.CommitId, ref.MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, ref.MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(request.NodeRefsRow.ShardId, ref.ShardId);
        UNIT_ASSERT_VALUES_EQUAL(
            request.NodeRefsRow.ShardNodeName,
            ref.ShardNodeName);
    }

    void ReadAndCheckNodeRef(
        TInMemoryIndexState& state,
        const TInMemoryIndexState::TWriteNodeRefsRequest& request)
    {
        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        UNIT_ASSERT(state.ReadNodeRef(
            request.NodeRefsKey.NodeId,
            request.NodeRefsRow.CommitId,
            request.NodeRefsKey.Name,
            ref));
        CheckNodeRef(request, *ref);
    }

    void FillStatesRequests(
        TVector<TInMemoryIndexState::TIndexStateRequest>& stateRequests,
        const TVector<TInMemoryIndexState::TWriteNodeRefsRequest>& requests)
    {
        stateRequests.reserve(requests.size());
        for (const auto& req: requests) {
            stateRequests.push_back(req);
        }
    }

    } // namespace

    //
    // NodeRefs
    //
    Y_UNIT_TEST(ShouldPopulateNodeRefs)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 0, 1);

        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        UNIT_ASSERT(
            !state.ReadNodeRef(rootNodeIds[0], commitId1, nodeNames[0], ref));

        TInMemoryIndexState::TWriteNodeRefsRequest request = {
            .NodeRefsKey = {rootNodeIds[0], nodeNames[0]},
            .NodeRefsRow = {
                .CommitId = commitId2,
                .ChildId = childNodeIds[0],
                .ShardId = shardIds[0],
                .ShardNodeName = shardNodeNames[0]}};
        state.UpdateState({request});

        ReadAndCheckNodeRef(state, request);

        // The cache is preemptive, so the listing should not be possible for
        // now
        TVector<IIndexTabletDatabase::TNodeRef> refs;
        TString next;
        UNIT_ASSERT(!state.ReadNodeRefs(
            rootNodeIds[0],
            commitId2,
            "",
            refs,
            Max<ui32>(),
            &next));
        UNIT_ASSERT(refs.empty());
        UNIT_ASSERT(next.empty());

        // Though the node is present in NodeRefs table, it is not visible at
        // commitId1. So read from cache is considered successful, but the node
        // that was read should be empty
        ref = {};
        UNIT_ASSERT(state.ReadNodeRef(
            request.NodeRefsKey.NodeId,
            commitId1,
            request.NodeRefsKey.Name,
            ref));
        UNIT_ASSERT(ref.Empty());
    }

    Y_UNIT_TEST(ShouldEvictNodeRefs)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 0, 3);

        const TVector<TInMemoryIndexState::TWriteNodeRefsRequest> requests = {
            {.NodeRefsKey = {rootNodeIds[0], nodeNames[0]},
             .NodeRefsRow = {
                .CommitId = commitId1,
                .ChildId = childNodeIds[0],
                .ShardId = shardIds[0],
                .ShardNodeName = shardNodeNames[0],
             }},
            {.NodeRefsKey = {rootNodeIds[0], nodeNames[1]},
             .NodeRefsRow = {
                .CommitId = commitId1,
                .ChildId = childNodeIds[1],
                .ShardId = shardIds[1],
                .ShardNodeName = shardNodeNames[1],
             }},
            {.NodeRefsKey = {rootNodeIds[2], nodeNames[2]},
             .NodeRefsRow = {
                 .CommitId = commitId1,
                 .ChildId = childNodeIds[2],
                 .ShardId = shardIds[2],
                 .ShardNodeName = shardNodeNames[2],
             }}};

        TVector<TInMemoryIndexState::TIndexStateRequest> stateRequests;
        FillStatesRequests(stateRequests, requests);

        state.UpdateState(stateRequests);
        state.MarkNodeRefsLoadComplete();

        // read two first refs
        TVector<IIndexTabletDatabase::TNodeRef> refs;
        TString nextName;

        UNIT_ASSERT(state.ReadNodeRefs(
            requests[0].NodeRefsKey.NodeId,
            commitId1,
            requests[0].NodeRefsKey.Name,
            refs,
            Max<ui32>(),
            &nextName));
        UNIT_ASSERT(refs.size() == 2);
        UNIT_ASSERT(nextName.empty());

        // all three refs should be in cache
        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        for (const auto& request: requests) {
            ReadAndCheckNodeRef(state, request);
        }

        // here the order should be 2, 1, 0
        // after we add one more ref, 0 should be evicted
        TInMemoryIndexState::TWriteNodeRefsRequest request3 = {
            .NodeRefsKey = {rootNodeIds[3], nodeNames[3]},
            .NodeRefsRow = {
                .CommitId = commitId1,
                .ChildId = childNodeIds[3],
                .ShardId = shardIds[3],
                .ShardNodeName = shardNodeNames[3],
            }};
        state.UpdateState({request3});

        ReadAndCheckNodeRef(state, request3);
        ReadAndCheckNodeRef(state, requests[1]);
        ReadAndCheckNodeRef(state, requests[2]);

        // after some refs are evicted ReadNodeRefs should always return false
        UNIT_ASSERT(!state.ReadNodeRefs(
            requests[0].NodeRefsKey.NodeId,
            commitId1,
            requests[0].NodeRefsKey.Name,
            refs,
            Max<ui32>(),
            &nextName));

        // write a ref with the same key bu different value
        TInMemoryIndexState::TWriteNodeRefsRequest request4 = {
            .NodeRefsKey = {rootNodeIds[3], nodeNames[3]},
            .NodeRefsRow = {
                .CommitId = commitId2,
                .ChildId = childNodeIds[0],
                .ShardId = shardIds[1],
                .ShardNodeName = shardNodeNames[2],
            }};
        state.UpdateState({request4});

        ReadAndCheckNodeRef(state, request4);
    }

    Y_UNIT_TEST(ShouldDeleteNodeRefs)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 0, 1);

        TInMemoryIndexState::TWriteNodeRefsRequest request = {
            .NodeRefsKey = {rootNodeIds[0], nodeNames[0]},
            .NodeRefsRow = {
                .CommitId = commitId1,
                .ChildId = childNodeIds[0],
                .ShardId = shardIds[0],
                .ShardNodeName = shardNodeNames[0]}};

        state.UpdateState({request});
        state.MarkNodeRefsLoadComplete();

        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        UNIT_ASSERT(state.ReadNodeRef(
            request.NodeRefsKey.NodeId,
            request.NodeRefsRow.CommitId,
            request.NodeRefsKey.Name,
            ref));
        UNIT_ASSERT(ref.Defined());

        state.UpdateState({TInMemoryIndexState::TDeleteNodeRefsRequest{
            request.NodeRefsKey.NodeId,
            request.NodeRefsKey.Name}});

        UNIT_ASSERT(!state.ReadNodeRef(
            request.NodeRefsKey.NodeId,
            request.NodeRefsRow.CommitId,
            request.NodeRefsKey.Name,
            ref));
    }
}

}   // namespace NCloud::NFileStore::NStorage
