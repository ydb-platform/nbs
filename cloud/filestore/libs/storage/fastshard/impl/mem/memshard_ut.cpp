#include "memshard.h"

#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>

#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

#include <sys/stat.h>

namespace NCloud::NFileStore::NStorage::NFastShard {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TMemShardTest)
{
    //
    // Tablet ut test suite contains tests with memshard w/o
    // CreateNodeUponAccess flag.
    //

    Y_UNIT_TEST(ShouldCreateNodeUponAccess)
    {
        constexpr ui32 ShardNo = 77;

        NProtoPrivate::TMemFastShardConfig config;
        config.SetCreateNodeUponAccess(true);
        auto s = CreateMemFileSystemShard(ShardNo, config);

        ui64 lastNodeId = 0;
        {
            //
            // Create upon access
            //

            NProto::TGetNodeAttrRequest request;
            request.SetNodeId(RootNodeId);
            request.SetName("f0");
            auto response = s->GetNodeAttr(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
            const ui64 nodeId = response.GetNode().GetId();
            lastNodeId = nodeId;
            UNIT_ASSERT(nodeId);
            UNIT_ASSERT_C(
                S_IFREG & response.GetNode().GetMode(),
                response.GetNode().GetMode());
            UNIT_ASSERT(response.GetNode().GetUid());
            UNIT_ASSERT(response.GetNode().GetGid());

            //
            // Next accesses should return the same node.
            //

            response = s->GetNodeAttr(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL(nodeId, response.GetNode().GetId());

            request.SetNodeId(nodeId);
            request.ClearName();
            response = s->GetNodeAttr(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL(nodeId, response.GetNode().GetId());
        }

        {
            //
            // Create upon access
            //

            NProtoPrivate::TGetNodeAttrBatchRequest request;
            request.SetNodeId(RootNodeId);
            request.AddNames("f1");
            auto response = s->GetNodeAttrBatch(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(1, response.ResponsesSize());
            const ui64 nodeId = response.GetResponses(0).GetNode().GetId();
            lastNodeId = nodeId;
            UNIT_ASSERT(nodeId);
            UNIT_ASSERT_C(
                S_IFREG & response.GetResponses(0).GetNode().GetMode(),
                response.GetResponses(0).GetNode().GetMode());
            UNIT_ASSERT(response.GetResponses(0).GetNode().GetUid());
            UNIT_ASSERT(response.GetResponses(0).GetNode().GetGid());

            //
            // Next accesses should return the same node.
            //

            response = s->GetNodeAttrBatch(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL(1, response.ResponsesSize());
            UNIT_ASSERT_VALUES_EQUAL(
                nodeId,
                response.GetResponses(0).GetNode().GetId());
        }

        {
            //
            // Create upon access
            //

            NProto::TCreateHandleRequest request;
            request.SetNodeId(RootNodeId);
            request.SetName("f2");
            auto response = s->CreateHandle(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
            const ui64 nodeId = response.GetNodeAttr().GetId();
            UNIT_ASSERT(nodeId);
            UNIT_ASSERT_C(
                nodeId > lastNodeId,
                TStringBuilder() << nodeId << ", " << lastNodeId);
            lastNodeId = nodeId;
            UNIT_ASSERT_C(
                S_IFREG & response.GetNodeAttr().GetMode(),
                response.GetNodeAttr().GetMode());
            UNIT_ASSERT(response.GetNodeAttr().GetUid());
            UNIT_ASSERT(response.GetNodeAttr().GetGid());

            //
            // Next accesses should return the same node.
            //

            response = s->CreateHandle(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL(nodeId, response.GetNodeAttr().GetId());

            request.SetNodeId(nodeId);
            request.ClearName();
            response = s->CreateHandle(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL(nodeId, response.GetNodeAttr().GetId());
        }

        {
            //
            // Just testing that basic create still works.
            //

            constexpr ui32 Mode = 0444;
            constexpr ui64 Uid = 1111;
            constexpr ui64 Gid = 2222;
            NProto::TCreateNodeRequest request;
            request.SetNodeId(RootNodeId);
            request.SetName("f3");
            request.MutableFile()->SetMode(Mode);
            request.SetUid(Uid);
            request.SetGid(Gid);
            auto response = s->CreateNode(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
            const ui64 nodeId = response.GetNode().GetId();
            UNIT_ASSERT(nodeId);
            UNIT_ASSERT_C(
                nodeId > lastNodeId,
                TStringBuilder() << nodeId << ", " << lastNodeId);
            lastNodeId = nodeId;
            UNIT_ASSERT_VALUES_EQUAL(
                S_IFREG | Mode,
                response.GetNode().GetMode());
            UNIT_ASSERT_VALUES_EQUAL(Uid, response.GetNode().GetUid());
            UNIT_ASSERT_VALUES_EQUAL(Gid, response.GetNode().GetGid());

            //
            // Next accesses should return E_FS_EXIST.
            //

            response = s->CreateNode(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_EXIST,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }

        {
            //
            // Nonexistent file - no op.
            //

            NProto::TUnlinkNodeRequest request;
            request.SetNodeId(RootNodeId);
            request.SetName("f4");
            auto response = s->UnlinkNode(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());

            response = s->UnlinkNode(request).GetValue();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response.GetError().GetCode(),
                response.GetError().GetMessage());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage::NFastShard
