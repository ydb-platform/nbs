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
        const TString name1 = "name1";
        const TString uuid1 = CreateGuidAsString();

        {
            auto response = tablet.SendAndRecvCreateNode(
                TCreateNodeArgs::Directory(RootNodeId, uuid1));
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvUnlinkNode(
                RootNodeId,
                uuid1,
                false /* unlinkDirectory */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvCreateHandle(
                RootNodeId,
                uuid1,
                0 /* flags */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvDestroyHandle(1 /* handle */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvWriteData(
                1 /* handle */,
                0 /* offset */,
                4_KB /* len */,
                'a' /* fill */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        {
            auto response = tablet.SendAndRecvReadData(
                1 /* handle */,
                0 /* offset */,
                4_KB /* len */);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_NOT_IMPLEMENTED,
                response->GetStatus(),
                response->GetErrorReason());
        }

        tablet.DestroySession();
    }
}

}   // namespace NCloud::NFileStore::NStorage
