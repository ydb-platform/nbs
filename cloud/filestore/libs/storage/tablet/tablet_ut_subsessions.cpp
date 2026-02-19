#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_SubSessions)
{
    Y_UNIT_TEST(ShouldSupportMultipleSubSessions)
    {
        TTestEnvConfig envCfg;
        envCfg.DynamicNodes = 2;
        TTestEnv env(envCfg);

        ui32 nodeIdx1 = env.AddDynamicNode();
        ui32 nodeIdx2 = env.AddDynamicNode();

        ui64 tabletId = env.BootIndexTablet(nodeIdx1);

        TIndexTabletClient tclient1(env.GetRuntime(), nodeIdx1, tabletId);
        TIndexTabletClient tclient2(env.GetRuntime(), nodeIdx2, tabletId);

        tclient1.InitSession("client1", "session", "", 0, true);
        tclient2.InitSession("client1", "session", "", 1, false);

        tclient1.WithSessionSeqNo(0);
        tclient2.WithSessionSeqNo(1);

        auto id1 = CreateNode(tclient1, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle1 = CreateHandle(tclient1, id1);
        tclient1.AcquireLock(handle1, 1, 0, 1_KB);

        auto id2 = CreateNode(tclient2, TCreateNodeArgs::File(RootNodeId, "xxx"));
        CreateHandle(tclient2, id2);
        tclient2.UnlinkNode(RootNodeId, "xxx", false);
    }

    Y_UNIT_TEST(ShouldKeepSubSessionsIfAnotherRemoved)
    {
        TTestEnvConfig envCfg;
        envCfg.DynamicNodes = 2;
        TTestEnv env(envCfg);

        ui32 nodeIdx1 = env.AddDynamicNode();
        ui32 nodeIdx2 = env.AddDynamicNode();

        ui64 tabletId = env.BootIndexTablet(nodeIdx1);

        TIndexTabletClient tclient1(env.GetRuntime(), nodeIdx1, tabletId);
        TIndexTabletClient tclient2(env.GetRuntime(), nodeIdx2, tabletId);

        tclient1.InitSession("client1", "session", "", 0, true);
        tclient2.InitSession("client1", "session", "", 1, false);

        tclient1.WithSessionSeqNo(0);
        tclient2.WithSessionSeqNo(1);

        auto id = CreateNode(tclient1, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tclient1, id);
        tclient1.WriteData(handle, 0, 4_KB, '0');
        tclient1.WriteData(handle, 100, 10, 'a');

        tclient1.DestroySession();

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), '0', 4_KB);
        memset(expected.begin() + 100, 'a', 10);

        {
            auto response = tclient2.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        auto id1 = CreateNode(tclient2, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto handle1 = CreateHandle(tclient2, id1);
        tclient2.AcquireLock(handle1, 1, 0, 1_KB);
    }

    Y_UNIT_TEST(ShouldRemoveTooOldSubSessions)
    {
        TTestEnvConfig envCfg;
        envCfg.DynamicNodes = 3;
        TTestEnv env(envCfg);

        ui32 nodeIdx1 = env.AddDynamicNode();
        ui32 nodeIdx2 = env.AddDynamicNode();
        ui32 nodeIdx3 = env.AddDynamicNode();

        ui64 tabletId = env.BootIndexTablet(nodeIdx1);

        TIndexTabletClient tclient1(env.GetRuntime(), nodeIdx1, tabletId);
        TIndexTabletClient tclient2(env.GetRuntime(), nodeIdx2, tabletId);

        tclient1.InitSession("client1", "session", "", 0, true);
        tclient2.InitSession("client1", "session", "", 1, false);

        tclient1.WithSessionSeqNo(0);
        tclient2.WithSessionSeqNo(1);

        {
            auto id1 = CreateNode(tclient1, TCreateNodeArgs::File(RootNodeId, "aaa"));
            auto handle1 = CreateHandle(tclient1, id1);
            tclient1.AcquireLock(handle1, 1, 0, 1_KB);
        }

        {
            auto id1 = CreateNode(tclient1, TCreateNodeArgs::File(RootNodeId, "bbb"));
            auto handle1 = CreateHandle(tclient1, id1);
            tclient1.AcquireLock(handle1, 1, 0, 1_KB);
        }

        TIndexTabletClient tclient3(env.GetRuntime(), nodeIdx3, tabletId);
        tclient3.InitSession("client1", "session", "", 2, true);
        tclient3.WithSessionSeqNo(2);

        {
            auto id1 = CreateNode(tclient3, TCreateNodeArgs::File(RootNodeId, "ccc"));
            auto handle1 = CreateHandle(tclient3, id1);
            tclient3.AcquireLock(handle1, 1, 0, 1_KB);
        }

        {
            tclient1.SendCreateNodeRequest(TCreateNodeArgs::File(RootNodeId, "ddd"));
            auto response = tclient1.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL(FAILED(response->GetStatus()), true);
        }
    }

    Y_UNIT_TEST(ShouldKeepSubSessionsIfAnotherReset)
    {
        TTestEnvConfig envCfg;
        envCfg.DynamicNodes = 2;
        TTestEnv env(envCfg);

        ui32 nodeIdx1 = env.AddDynamicNode();
        ui32 nodeIdx2 = env.AddDynamicNode();

        ui64 tabletId = env.BootIndexTablet(nodeIdx1);

        TIndexTabletClient tclient1(env.GetRuntime(), nodeIdx1, tabletId);
        TIndexTabletClient tclient2(env.GetRuntime(), nodeIdx2, tabletId);

        tclient1.InitSession("client1", "session", "", 0, true);
        tclient2.InitSession("client1", "session", "", 1, false);

        tclient1.WithSessionSeqNo(0);
        tclient2.WithSessionSeqNo(1);

        auto id = CreateNode(tclient1, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tclient1, id);
        tclient1.WriteData(handle, 0, 4_KB, '0');
        tclient1.WriteData(handle, 100, 10, 'a');

        tclient1.ResetSession(TString());

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), '0', 4_KB);
        memset(expected.begin() + 100, 'a', 10);

        {
            auto response = tclient2.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }

        auto id1 = CreateNode(tclient2, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto handle1 = CreateHandle(tclient2, id1);
        tclient2.AcquireLock(handle1, 1, 0, 1_KB);
    }

    Y_UNIT_TEST(ShouldSupportInterhostMigrationScenario)
    {
        TTestEnvConfig envCfg;
        envCfg.DynamicNodes = 2;
        TTestEnv env(envCfg);

        ui32 nodeIdx1 = env.AddDynamicNode();
        ui32 nodeIdx2 = env.AddDynamicNode();

        ui64 tabletId = env.BootIndexTablet(nodeIdx1);

        TIndexTabletClient tclient1(env.GetRuntime(), nodeIdx1, tabletId);
        TIndexTabletClient tclient2(env.GetRuntime(), nodeIdx2, tabletId);

        tclient1.InitSession("client1", "session", "", 1, false);
        tclient2.InitSession("client1", "session", "", 2, true);

        tclient1.WithSessionSeqNo(1);
        tclient2.WithSessionSeqNo(2);

        auto id = CreateNode(tclient1, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tclient1, id);
        tclient1.WriteData(handle, 0, 4_KB, '0');
        tclient1.WriteData(handle, 100, 10, 'a');

        tclient2.InitSession("client1", "session", "", 2, false);
        tclient1.DestroySession();

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), '0', 4_KB);
        memset(expected.begin() + 100, 'a', 10);

        {
            auto response = tclient2.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }

    Y_UNIT_TEST(ShouldSupportInterhostMigrationRollbackScenario)
    {
        TTestEnvConfig envCfg;
        envCfg.DynamicNodes = 2;
        TTestEnv env(envCfg);

        ui32 nodeIdx1 = env.AddDynamicNode();
        ui32 nodeIdx2 = env.AddDynamicNode();

        ui64 tabletId = env.BootIndexTablet(nodeIdx1);

        TIndexTabletClient tclient1(env.GetRuntime(), nodeIdx1, tabletId);
        TIndexTabletClient tclient2(env.GetRuntime(), nodeIdx2, tabletId);

        tclient1.InitSession("client1", "session", "", 1, false);
        tclient2.InitSession("client1", "session", "", 2, true);

        tclient1.WithSessionSeqNo(1);
        tclient2.WithSessionSeqNo(2);

        auto id = CreateNode(tclient1, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tclient1, id);
        tclient1.WriteData(handle, 0, 4_KB, '0');
        tclient1.WriteData(handle, 100, 10, 'a');

        tclient1.InitSession("client1", "session", "", 1, true);
        tclient2.DestroySession();

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), '0', 4_KB);
        memset(expected.begin() + 100, 'a', 10);

        {
            auto response = tclient1.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage

