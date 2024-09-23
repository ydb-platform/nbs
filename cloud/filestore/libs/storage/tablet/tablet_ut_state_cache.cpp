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

    const TString nodeName1 = "node1";
    const TString nodeName2 = "node2";
    const TString followerName1 = "follower1";
    const TString followerName2 = "follower2";
    const TString followerId1 = "followerId1";
    const TString followerId2 = "followerId2";

    //
    // NodeRefs
    //
    Y_UNIT_TEST(ShoulPopulateNodeRefs)
    {
        // For now
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 0, 1);

        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        UNIT_ASSERT(!state.ReadNodeRef(nodeId1, commitId2, nodeName1, ref));

        state.UpdateState({TInMemoryIndexState::TWriteNodeRefsRequest{
            .NodeRefsKey = {RootNodeId, nodeName1},
            .NodeRefsRow = {
                .CommitId = commitId2,
                .ChildId = nodeId1,
                .FollowerId = followerId1,
                .FollowerName = followerName1}}});

        UNIT_ASSERT(state.ReadNodeRef(RootNodeId, commitId2, nodeName1, ref));
        UNIT_ASSERT_VALUES_EQUAL(RootNodeId, ref->NodeId);
        UNIT_ASSERT_VALUES_EQUAL(nodeName1, ref->Name);
        UNIT_ASSERT_VALUES_EQUAL(nodeId1, ref->ChildNodeId);
        UNIT_ASSERT_VALUES_EQUAL(commitId2, ref->MinCommitId);
        UNIT_ASSERT_VALUES_EQUAL(InvalidCommitId, ref->MaxCommitId);
        UNIT_ASSERT_VALUES_EQUAL(followerId1, ref->FollowerId);
        UNIT_ASSERT_VALUES_EQUAL(followerName1, ref->FollowerName);

        // The cache is preemptive, so the listing should not be possible for
        // now
        TVector<IIndexTabletDatabase::TNodeRef> refs;
        TString next;
        UNIT_ASSERT(!state.ReadNodeRefs(
            RootNodeId,
            commitId2,
            "",
            refs,
            Max<ui32>(),
            &next));
        UNIT_ASSERT(refs.empty());
        UNIT_ASSERT(next.Empty());

        // Though the node is present in NodeRefs table, it is not visible at
        // commitId1. So read from cache is considered successful, but the node
        // that was read should be empty
        ref = {};
        UNIT_ASSERT(state.ReadNodeRef(RootNodeId, commitId1, nodeName1, ref));
        UNIT_ASSERT(ref.Empty());
    }

    Y_UNIT_TEST(ShouldEvictNodeRefs)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 0, 1);

        state.UpdateState(
            {TInMemoryIndexState::TWriteNodeRefsRequest{
                 .NodeRefsKey = {RootNodeId, nodeName1},
                 .NodeRefsRow =
                     {
                         .CommitId = commitId1,
                         .ChildId = nodeId1,
                         .FollowerId = followerId1,
                         .FollowerName = followerName1,
                     }},
             TInMemoryIndexState::TWriteNodeRefsRequest{
                 .NodeRefsKey = {RootNodeId, nodeName2},
                 .NodeRefsRow = {
                     .CommitId = commitId1,
                     .ChildId = nodeId2,
                     .FollowerId = followerId2,
                     .FollowerName = followerName2,
                 }}});

        TMaybe<IIndexTabletDatabase::TNodeRef> ref;

        UNIT_ASSERT(!state.ReadNodeRef(RootNodeId, commitId1, nodeName1, ref));
    }

    Y_UNIT_TEST(ShouldDeleteNodeRefs)
    {
        TInMemoryIndexState state(TDefaultAllocator::Instance());
        state.Reset(0, 0, 1);

        state.UpdateState({TInMemoryIndexState::TWriteNodeRefsRequest{
            .NodeRefsKey = {RootNodeId, nodeName1},
            .NodeRefsRow = {
                .CommitId = commitId1,
                .ChildId = nodeId1,
                .FollowerId = followerId1,
                .FollowerName = followerName1,
            }}});

        TMaybe<IIndexTabletDatabase::TNodeRef> ref;
        UNIT_ASSERT(state.ReadNodeRef(RootNodeId, commitId1, nodeName1, ref));
        UNIT_ASSERT(ref.Defined());

        state.UpdateState({TInMemoryIndexState::TDeleteNodeRefsRequest{
            RootNodeId,
            nodeName1}});

        UNIT_ASSERT(!state.ReadNodeRef(RootNodeId, commitId1, nodeName1, ref));
    }
}

}   // namespace NCloud::NFileStore::NStorage
