#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_NodesInternal)
{
    // TODO:
    // 1. check error when dest == local node
    // 2. check CommitIdOverflow for otherwise correct request
    // 3. check error when src == dir and dst == file
    //      OR src == file and dst == dir
    //      OR src == dir and dst == non-empty dir
    // 4. check dupcache - retry an already completed op

    TABLET_TEST_4K_ONLY(ShouldHandleCommitIdOverflowUponRenameNodeInDestination)
    {
        const auto maxTabletStep = 5;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        const ui32 nodeIdx = env.CreateNode("nfs");

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        const ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            tabletConfig);
        tablet.InitSession("client", "session");

        auto dir =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        ui32 failures = 0;

        TVector<TString> names;
        TVector<NProto::TNodeAttr> nodes;

        for (ui32 i = 0; i < 10; ++i) {
            auto fileName = TStringBuilder() << "file" << i;
            auto shardId = TStringBuilder() << "shard" << i;
            auto shardNodeName = CreateGuidAsString();

            tablet.SendRenameNodeInDestinationRequest(
                dir,
                fileName,
                shardId,
                shardNodeName);
            auto response = tablet.RecvRenameNodeInDestinationResponse();
            reconnectIfNeeded();

            if (HasError(response->GetError())) {
                UNIT_ASSERT_VALUES_EQUAL_C(
                    E_REJECTED,
                    response->GetStatus(),
                    FormatError(response->GetError()));
                ++failures;
                continue;
            }

            names.push_back(std::move(fileName));
            NProto::TNodeAttr node;
            node.SetShardFileSystemId(shardId);
            node.SetShardNodeName(shardNodeName);
            nodes.push_back(std::move(node));
        }

        auto response = tablet.ListNodes(dir);
        const auto& listedNames = response->Record.GetNames();
        const auto& listedNodes = response->Record.GetNodes();
        UNIT_ASSERT_VALUES_EQUAL(names.size(), listedNames.size());
        UNIT_ASSERT_VALUES_EQUAL(nodes.size(), listedNodes.size());
        for (ui32 i = 0; i < names.size(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(names[i], listedNames[i]);
            UNIT_ASSERT_VALUES_EQUAL(
                nodes[i].ShortUtf8DebugString(),
                listedNodes[i].ShortUtf8DebugString());
        }

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 2,
            "Expected at least 2 different generations due to tablet reboot, "
            "got "
                << rebootTracker.GetGenerationCount());

        UNIT_ASSERT_GT(failures, 0);
    }
}

}   // namespace NCloud::NFileStore::NStorage
