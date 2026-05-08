#include "tablet.h"

#include <cloud/filestore/libs/storage/api/ss_proxy.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/diagnostics/public.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Adapter)
{
    TABLET_TEST_4K_ONLY(ShouldUseAdapter)
    {
        NProto::TStorageConfig storageConfig;
        TTestEnv env({}, storageConfig);

        const ui32 nodeIdx = env.AddDynamicNode();
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);

        tablet.ConfigureAsShard(
            1 /* shardNo */,
            true /* directoryCreationInShardsEnabled */,
            TVector<TString>() /* shardIds */,
            NProtoPrivate::TFastShardConfig(),
            true /* isFastShard */);

        tablet.ReconnectPipe();
        tablet.WaitReady();
        tablet.InitSession("client", "session");

        const TString shardId1 = "shard1";
        const TString uuid1 = CreateGuidAsString();

        {
            auto response = tablet.SendAndRecvCreateNode(
                TCreateNodeArgs::Directory(RootNodeId, uuid1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        const TString uuid2 = CreateGuidAsString();
        ui64 nodeId2 = 0;
        ui64 handle2 = 0;

        {
            auto response = tablet.SendAndRecvCreateNode(
                TCreateNodeArgs::File(RootNodeId, uuid2));
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            nodeId2 = response->Record.GetNode().GetId();

            auto hResponse =
                tablet.SendAndRecvCreateHandle(nodeId2, 0 /* flags */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                hResponse->GetStatus(),
                hResponse->GetErrorReason());
            handle2 = hResponse->Record.GetHandle();
        }

        const TString uuid3 = CreateGuidAsString();
        ui64 nodeId3 = 0;
        ui64 handle3 = 0;

        {
            auto hResponse = tablet.SendAndRecvCreateHandle(
                RootNodeId,
                uuid3,
                0 /* flags */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                hResponse->GetStatus(),
                hResponse->GetErrorReason());
        }

        {
            auto hResponse = tablet.SendAndRecvCreateHandle(
                RootNodeId,
                uuid3,
                TCreateHandleArgs::CREATE);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                hResponse->GetStatus(),
                hResponse->GetErrorReason());
            nodeId3 = hResponse->Record.GetNodeAttr().GetId();
            handle3 = hResponse->Record.GetHandle();
        }

        {
            auto response = tablet.SendAndRecvUnlinkNode(
                RootNodeId,
                uuid1,
                false /* unlinkDirectory */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvGetNodeAttr(RootNodeId, uuid2);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            const auto& node = response->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(nodeId2, node.GetId());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::E_REGULAR_NODE),
                node.GetType());
        }

        {
            auto response = tablet.SendAndRecvUnlinkNode(
                RootNodeId,
                uuid2,
                false /* unlinkDirectory */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvGetNodeAttr(RootNodeId, uuid2);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvGetNodeAttr(nodeId2);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            const auto& node = response->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(nodeId2, node.GetId());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::E_REGULAR_NODE),
                node.GetType());
        }

        {
            auto response = tablet.SendAndRecvDestroyHandle(handle2);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvGetNodeAttr(nodeId2);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                response->GetErrorReason());
        }

        TString expected;
        expected.ReserveAndResize(6_KB);

        {
            auto response = tablet.SendAndRecvWriteData(
                handle3,
                0 /* offset */,
                1_KB /* len */,
                'a' /* fill */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            memset(expected.begin(), 'a', 1_KB);
        }

        {
            auto response = tablet.SendAndRecvWriteData(
                handle3,
                3_KB /* offset */,
                2_KB /* len */,
                'b' /* fill */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            memset(expected.begin() + 3_KB, 'b', 2_KB);
        }

        {
            auto response = tablet.SendAndRecvReadData(
                handle3,
                0 /* offset */,
                6_KB /* len */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(expected, response->Record.GetBuffer());
        }

        {
            auto response = tablet.SendAndRecvGetNodeAttr(nodeId3);
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            const auto& node = response->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(nodeId3, node.GetId());
            UNIT_ASSERT_VALUES_EQUAL(
                static_cast<ui32>(NProto::E_REGULAR_NODE),
                node.GetType());
            UNIT_ASSERT_VALUES_EQUAL(5_KB, node.GetSize());
        }

        tablet.DestroySession();
    }
}

}   // namespace NCloud::NFileStore::NStorage
