#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Sessions)
{
    Y_UNIT_TEST(ShouldTrackClients)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.InitSession("client", "session");
        tablet.DestroySession();
    }

    Y_UNIT_TEST(ShouldTrackHandles)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.InitSession("client", "session");

        ui64 id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldTrackLocks)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        tablet.AcquireLock(handle, 1, 0, 4_KB);

        auto response = tablet.AssertAcquireLockFailed(handle, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        tablet.ReleaseLock(handle, 1, 0, 4_KB);
        tablet.AcquireLock(handle, 2, 0, 4_KB);

        response = tablet.AssertAcquireLockFailed(handle, 1, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        tablet.ReleaseLock(handle, 2, 0, 4_KB);
        tablet.AcquireLock(handle, 1, 0, 0);

        response = tablet.AssertAcquireLockFailed(handle, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        response = tablet.AssertAcquireLockFailed(handle, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        tablet.DestroyHandle(handle);

        handle = CreateHandle(tablet, id, {}, TCreateHandleArgs::RDNLY);
        tablet.AssertTestLockFailed(handle, 1, 0, 4_KB);
        tablet.AssertAcquireLockFailed(handle, 1, 0, 4_KB);
    }

    Y_UNIT_TEST(ShouldTrackSharedLocks)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);

        tablet.AcquireLock(handle, 1, 0, 4_KB, DefaultPid, NProto::E_SHARED);
        tablet.AcquireLock(handle, 2, 0, 4_KB, DefaultPid, NProto::E_SHARED);

        auto response = tablet.AssertAcquireLockFailed(handle, 1, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        response = tablet.AssertAcquireLockFailed(handle, 1, 0, 0);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        tablet.AcquireLock(handle, 3, 0, 4_KB, DefaultPid, NProto::E_SHARED);

        tablet.DestroyHandle(handle);

        handle = CreateHandle(tablet, id, {}, TCreateHandleArgs::WRNLY);
        tablet.AssertTestLockFailed(handle, 1, 0, 4_KB, DefaultPid, NProto::E_SHARED);
        tablet.AssertAcquireLockFailed(handle, 1, 0, 4_KB, DefaultPid, NProto::E_SHARED);
    }

    Y_UNIT_TEST(ShouldCleanupLocksKeptBySession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));

        ui64 handle1 = CreateHandle(tablet, id1);
        ui64 handle2 = CreateHandle(tablet, id2);

        tablet.AcquireLock(handle1, 1, 0, 0);
        tablet.AcquireLock(handle2, 1, 0, 4_KB);

        tablet.RebootTablet();
        tablet.DestroySession();
        tablet.RebootTablet();

        tablet.InitSession("client", "session2");
        handle1 = CreateHandle(tablet, id1);
        tablet.AcquireLock(handle1, 1, 0, 4_KB);

        auto response = tablet.AssertAcquireLockFailed(handle1, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);
    }

    Y_UNIT_TEST(ShouldCreateHandlesAndFiles)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        tablet.CreateHandle(id1, TCreateHandleArgs::CREATE);
        tablet.CreateHandle(id1, TCreateHandleArgs::RDWR);

        auto response2 = tablet.CreateHandle(RootNodeId, "xxx", TCreateHandleArgs::CREATE);
        auto id2 = response2->Record.GetNodeAttr().GetId();
        UNIT_ASSERT(id2 != InvalidNodeId);

        auto response3 = tablet.CreateHandle(RootNodeId, "yyy", TCreateHandleArgs::CREATE_EXL);
        auto id3 = response3->Record.GetNodeAttr().GetId();
        UNIT_ASSERT(id3 != InvalidNodeId);

        auto response = tablet.ListNodes(RootNodeId);
        const auto& names = response->Record.GetNames();
        UNIT_ASSERT_VALUES_EQUAL(names.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(names[0], "test");
        UNIT_ASSERT_VALUES_EQUAL(names[1], "xxx");
        UNIT_ASSERT_VALUES_EQUAL(names[2], "yyy");

        const auto& nodes = response->Record.GetNodes();
        UNIT_ASSERT_VALUES_EQUAL(nodes.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(nodes.Get(0).GetId(), id1);
        UNIT_ASSERT_VALUES_EQUAL(nodes.Get(1).GetId(), id2);
        UNIT_ASSERT_VALUES_EQUAL(nodes.Get(2).GetId(), id3);
        UNIT_ASSERT_VALUES_EQUAL(nodes.Get(2).GetLinks(), 1);
    }

    Y_UNIT_TEST(ShouldNotCreateInvalidHandles)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        tablet.AssertCreateHandleFailed(100500, TCreateHandleArgs::RDWR);
        // create file under file node
        tablet.AssertCreateHandleFailed(id, "xxx", TCreateHandleArgs::CREATE);
        // create the same file with O_EXCL
        tablet.AssertCreateHandleFailed(RootNodeId, "test", TCreateHandleArgs::CREATE_EXL);
        // open non existent file w/o O_CREAT
        tablet.AssertCreateHandleFailed(RootNodeId, "xxx", TCreateHandleArgs::RDWR);
    }

    Y_UNIT_TEST(ShouldKeepFileByOpenHandle)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.UnlinkNode(RootNodeId, "test", false);

        auto attrs = GetNodeAttrs(tablet, id);
        UNIT_ASSERT_VALUES_EQUAL(attrs.GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(attrs.GetSize(), 0);

        tablet.WriteData(handle, 0, 1_KB, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 1_KB);

        auto buffer = ReadData(tablet, handle, 16_KB, 0);
        UNIT_ASSERT_VALUES_EQUAL(buffer.size(), 1_KB);
        auto expected = TString(1_KB, '1');
        UNIT_ASSERT_VALUES_EQUAL(buffer, expected);

        tablet.DestroyHandle(handle);
        tablet.AssertGetNodeAttrFailed(id);
        tablet.AssertWriteDataFailed(handle, 0, 1_KB, '2');
        tablet.AssertReadDataFailed(handle, 16_KB, 0);
    }

    Y_UNIT_TEST(ShouldCleanupHandlesAndNodesKeptBySession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.WriteData(handle, 0, 1_KB, '1');

        tablet.DestroySession();

        tablet.InitSession("client", "session");

        tablet.AssertGetNodeAttrFailed(id);
        tablet.AssertWriteDataFailed(handle, 0, 1_KB, '2');
        tablet.AssertReadDataFailed(handle, 0, 1_KB);
    }

    Y_UNIT_TEST(ShouldSendSessionEvents)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.SubscribeSession();
        tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "test"));

        auto response = tablet.RecvResponse<TEvService::TEvGetSessionEventsResponse>();
        UNIT_ASSERT_C(
            SUCCEEDED(response->GetStatus()),
            response->GetErrorReason());

        const auto& events = response->Record.GetEvents();
        UNIT_ASSERT_VALUES_EQUAL(events.size(), 1);
    }

    Y_UNIT_TEST(ShouldRestoreClientSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        TString sessionId;
        {
            auto response = tablet.CreateSession(
                "client",
                "session",  // sessionId
                "",         // checkpointId
                0,
                false,
                true        // restoreClientSession
            );
            sessionId = response->Record.GetSessionId();
        }
        UNIT_ASSERT_VALUES_EQUAL("session", sessionId);

        tablet.SetHeaders("client", sessionId, 0);

        ui64 id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        ui64 handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, 4_KB, '0');

        {
            auto response = tablet.CreateSession(
                "client",
                "vasya",    // sessionId
                "",         // checkpointId
                0,
                false,
                true        // restoreClientSession
            );
            UNIT_ASSERT_VALUES_EQUAL("session", response->Record.GetSessionId());
        }

        {
            auto response = tablet.CreateSession(
                "client",
                "",         // sessionId
                "",         // checkpointId
                0,
                false,
                true        // restoreClientSession
            );
            UNIT_ASSERT_VALUES_EQUAL("session", response->Record.GetSessionId());
        }

        TString expected;
        expected.ReserveAndResize(4_KB);
        memset(expected.begin(), '0', 4_KB);

        {
            auto response = tablet.ReadData(handle, 0, 4_KB);
            const auto& buffer = response->Record.GetBuffer();
            UNIT_ASSERT_VALUES_EQUAL(expected, buffer);
        }
    }

    Y_UNIT_TEST(ShouldResetSessionState)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle1 = CreateHandle(tablet, id1);
        tablet.AcquireLock(handle1, 1, 0, 1_KB);

        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "xxx"));
        CreateHandle(tablet, id2);
        tablet.UnlinkNode(RootNodeId, "xxx", false);

        tablet.ResetSession("client", "session", 0, "state");

        // check that handles are invalidated
        auto lock = tablet.AssertAcquireLockFailed(handle1, 1, 0, 1_KB);
        UNIT_ASSERT_VALUES_EQUAL(lock->Record.GetError().GetCode(), (ui32)E_FS_BADHANDLE);

        // check that locks are invalidated
        handle1 = CreateHandle(tablet, id1);
        tablet.AcquireLock(handle1, 2, 0, 1_KB);

        // check that nodes are cleaned up
        tablet.AssertAccessNodeFailed(id2);

        // check that state is properly returned
        auto create = tablet.CreateSession("client", "session");
        UNIT_ASSERT_VALUES_EQUAL(create->Record.GetSessionState(), "state");

        tablet.ResetSession("client", "session", 0, "");

        // check that state is properly updated
        create = tablet.CreateSession("client", "session");
        UNIT_ASSERT_VALUES_EQUAL(create->Record.GetSessionState(), "");
    }

    Y_UNIT_TEST(ShouldCleanupHandlesAndLocksKeptBySession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle1 = CreateHandle(tablet, id);
        auto handle2 = CreateHandle(tablet, id);

        tablet.AcquireLock(handle1, 1, 0, 1_KB);
        tablet.AcquireLock(handle1, 1, 1_KB, 1_KB);

        tablet.AssertAcquireLockFailed(handle2, 2, 0, 1_KB);
        tablet.DestroyHandle(handle1);

        tablet.AcquireLock(handle2, 1, 0, 1_KB);
        tablet.AcquireLock(handle2, 1, 1_KB, 1_KB);

        tablet.DestroySession();
        tablet.InitSession("client", "session");

        auto handle3 = CreateHandle(tablet, id);
        tablet.AcquireLock(handle3, 1, 0, 1_KB);
        tablet.AcquireLock(handle3, 1, 1_KB, 1_KB);
    }

    Y_UNIT_TEST(ShouldDeduplicateCreateHandleRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto createRequest = [&] (ui64 reqId) {
            auto request = tablet.CreateCreateHandleRequest(
                RootNodeId,
                "xxx",
                TCreateHandleArgs::CREATE_EXL);
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return std::move(request);
        };

        ui64 handle = 0;
        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(!HasError(response->Record.GetError()));

            handle = response->Record.GetHandle();
            UNIT_ASSERT(handle);
        }

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(!HasError(response->GetError()));

            UNIT_ASSERT_VALUES_EQUAL(handle, response->Record.GetHandle());
        }

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(handle, response->Record.GetHandle());
        }

        tablet.SendRequest(createRequest(100501));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(HasError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldCleanupDedupCacheKeptBySession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto createRequest = [&] (ui64 reqId, const TString& name) {
            auto request = tablet.CreateCreateHandleRequest(
                RootNodeId,
                name,
                TCreateHandleArgs::CREATE_EXL);
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return std::move(request);
        };

        tablet.SendRequest(createRequest(100500, "xxx"));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(!HasError(response->Record.GetError()));
        }

        tablet.SendRequest(createRequest(100501, "yyy"));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
        }

        tablet.SendRequest(createRequest(100500, "xxx"));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(!HasError(response->Record.GetError()));
        }

        tablet.SendRequest(createRequest(100501, "yyy"));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
        }

        tablet.DestroySession();
        tablet.InitSession("client", "session");

        tablet.SendRequest(createRequest(100500, "xxx"));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(HasError(response->Record.GetError()));
        }

        tablet.SendRequest(createRequest(100501, "yyy"));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT(HasError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ChangeCacheTest)
    {
        NKikimr::NFake::TCaches cachesConfig;
        cachesConfig.Shared = 1;
        TTestEnv env({}, {}, std::move(cachesConfig));
        env.CreateSubDomain("nfs");
        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);
        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto cacheSize = env.GetPrivateCacheSize(tabletId);
        UNIT_ASSERT_VALUES_EQUAL(cacheSize, 393216);

        env.UpdatePrivateCacheSize(tabletId, 10000000);
        cacheSize = env.GetPrivateCacheSize(tabletId);
        UNIT_ASSERT_VALUES_EQUAL(cacheSize, 10000000);
    }

    Y_UNIT_TEST(ShouldProperlyHandleSeqNoInDestroySession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.InitSession("client", "session", {}, 2);

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));

        ui64 handle1 = CreateHandle(tablet, id1);
        ui64 handle2 = CreateHandle(tablet, id2);

        tablet.AcquireLock(handle1, 1, 0, 0);
        tablet.AcquireLock(handle2, 1, 0, 4_KB);

        tablet.RebootTablet();
        tablet.DestroySession(1);
        tablet.RebootTablet();

        tablet.InitSession("client", "session2");
        handle1 = CreateHandle(tablet, id1);
        auto response = tablet.AssertAcquireLockFailed(handle1, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        tablet.InitSession("client", "session", {}, 2);
        tablet.DestroySession(2);

        tablet.InitSession("client", "session2");
        handle1 = CreateHandle(tablet, id1);
        tablet.AcquireLock(handle1, 2, 0, 4_KB);
    }

    Y_UNIT_TEST(ShouldProperlyHandleSeqNoInResetSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.InitSession("client", "session", {}, 2);

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));

        ui64 handle1 = CreateHandle(tablet, id1);
        ui64 handle2 = CreateHandle(tablet, id2);

        tablet.AcquireLock(handle1, 1, 0, 0);
        tablet.AcquireLock(handle2, 1, 0, 4_KB);

        tablet.RebootTablet();
        tablet.ResetSession("client", "session", 1, "");
        tablet.RebootTablet();

        tablet.InitSession("client", "session2");
        handle1 = CreateHandle(tablet, id1);
        auto response = tablet.AssertAcquireLockFailed(handle1, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(response->GetError().GetCode(), E_FS_WOULDBLOCK);

        tablet.InitSession("client", "session", {}, 2);
        tablet.ResetSession("client", "session", 2, "");

        tablet.InitSession("client", "session2");
        tablet.AcquireLock(handle1, 2, 0, 4_KB);
    }

    Y_UNIT_TEST(ShouldPreserveSessionStateWhileRestoreClientSession)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        TString sessionId;
        {
            auto response = tablet.CreateSession(
                "client",
                "session",  // sessionId
                "",         // checkpointId
                0,
                false,
                true        // restoreClientSession
            );
            sessionId = response->Record.GetSessionId();
        }
        UNIT_ASSERT_VALUES_EQUAL("session", sessionId);

        tablet.ResetSession("client", "session", 0, "hello");
        tablet.RebootTablet();

        {
            auto response = tablet.CreateSession(
                "client",
                "vasya",    // sessionId
                "",         // checkpointId
                1,
                false,
                true        // restoreClientSession
            );
            UNIT_ASSERT_VALUES_EQUAL("session", response->Record.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("hello", response->Record.GetSessionState());
        }

        tablet.RebootTablet();

        {
            auto response = tablet.CreateSession(
                "client",
                "",         // sessionId
                "",         // checkpointId
                2,
                false,
                true        // restoreClientSession
            );
            UNIT_ASSERT_VALUES_EQUAL("session", response->Record.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("hello", response->Record.GetSessionState());
        }
    }

    Y_UNIT_TEST(ShouldSupportInterhostMigrationScenarioInPresentsOfResets)
    {
        TTestEnvConfig envCfg;
        envCfg.DynamicNodes = 2;
        TTestEnv env(envCfg);

        env.CreateSubDomain("nfs");

        ui32 nodeIdx1 = env.CreateNode("nfs");
        ui32 nodeIdx2 = env.CreateNode("nfs");

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

        tclient1.RebootTablet();
        tclient2.ReconnectPipe();

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

    Y_UNIT_TEST(ShouldSupportInterhostMigrationRollbackScenarioInPresenseOfResets)
    {
        TTestEnvConfig envCfg;
        envCfg.DynamicNodes = 2;
        TTestEnv env(envCfg);

        env.CreateSubDomain("nfs");

        ui32 nodeIdx1 = env.CreateNode("nfs");
        ui32 nodeIdx2 = env.CreateNode("nfs");

        ui64 tabletId = env.BootIndexTablet(nodeIdx1);

        TIndexTabletClient tclient1(env.GetRuntime(), nodeIdx1, tabletId);
        TIndexTabletClient tclient2(env.GetRuntime(), nodeIdx2, tabletId);

        tclient1.InitSession("client1", "session", "", 1, false, true);
        tclient2.InitSession("client1", "session", "", 2, true, true);

        tclient1.WithSessionSeqNo(1);
        tclient2.WithSessionSeqNo(2);

        auto id = CreateNode(tclient1, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tclient1, id);
        tclient1.WriteData(handle, 0, 4_KB, '0');
        tclient1.WriteData(handle, 100, 10, 'a');

        tclient1.RebootTablet();
        tclient2.ReconnectPipe();

        tclient2.DestroySession();
        tclient1.InitSession("client1", "session", "", 1, true, true);

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

    void DoTestShouldReturnFeaturesInCreateSessionResponse(
        const NProto::TStorageConfig& config,
        const NProto::TFileStoreFeatures& expected)
    {
        TTestEnv env({}, config);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto response = tablet.CreateSession("client", "session");
        const auto& features = response->Record.GetFileStore().GetFeatures();
        UNIT_ASSERT_VALUES_EQUAL(features.DebugString(), expected.DebugString());
    }

    Y_UNIT_TEST(ShouldReturnFeaturesInCreateSessionResponse)
    {
        NProto::TStorageConfig config;
        NProto::TFileStoreFeatures features;
        features.SetThreeStageWriteThreshold(64_KB);
        features.SetPreferredBlockSize(4_KB);
        features.SetAsyncHandleOperationPeriod(
            TDuration::MilliSeconds(50).MilliSeconds());
        features.SetHasXAttrs(true);
        features.SetMaxFuseLoopThreads(1);

        DoTestShouldReturnFeaturesInCreateSessionResponse(config, features);

        config.SetTwoStageReadEnabled(true);
        config.SetThreeStageWriteEnabled(true);
        config.SetThreeStageWriteThreshold(10_MB);
        config.SetEntryTimeout(TDuration::Seconds(10).MilliSeconds());
        config.SetNegativeEntryTimeout(TDuration::Seconds(1).MilliSeconds());
        config.SetAttrTimeout(TDuration::Seconds(20).MilliSeconds());
        config.SetPreferredBlockSizeMultiplier(2);
        config.SetAsyncDestroyHandleEnabled(true);
        config.SetAsyncHandleOperationPeriod(
            TDuration::MilliSeconds(100).MilliSeconds());
        config.SetGuestPageCacheDisabled(true);
        config.SetExtendedAttributesDisabled(true);
        config.SetServerWriteBackCacheEnabled(true);
        config.SetParentlessFilesOnly(true);
        config.SetAllowHandlelessIO(true);

        features.SetTwoStageReadEnabled(true);
        features.SetEntryTimeout(TDuration::Seconds(10).MilliSeconds());
        features.SetNegativeEntryTimeout(TDuration::Seconds(1).MilliSeconds());
        features.SetAttrTimeout(TDuration::Seconds(20).MilliSeconds());
        features.SetThreeStageWriteEnabled(true);
        features.SetThreeStageWriteThreshold(10_MB);
        features.SetPreferredBlockSize(4_KB * 2);
        features.SetAsyncDestroyHandleEnabled(true);
        features.SetAsyncHandleOperationPeriod(
            TDuration::MilliSeconds(100).MilliSeconds());
        features.SetGuestPageCacheDisabled(true);
        features.SetExtendedAttributesDisabled(true);
        features.SetServerWriteBackCacheEnabled(true);
        features.SetParentlessFilesOnly(true);
        features.SetAllowHandlelessIO(true);

        DoTestShouldReturnFeaturesInCreateSessionResponse(config, features);
    }

    Y_UNIT_TEST(ShardShouldNotCheckSessionUponCreateNodeAndUnlinkNode)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        const ui64 requestId = 111;
        const ui32 shardNo = 222;
        tablet.ConfigureAsShard(shardNo);

        {
            tablet.SendCreateNodeRequest(
                TCreateNodeArgs::File(RootNodeId, "file1"),
                requestId);

            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }

        {
            tablet.SendCreateNodeRequest(
                TCreateNodeArgs::File(RootNodeId, "file1"),
                requestId);

            auto response = tablet.RecvCreateNodeResponse();
            // DupCache shouldn't be used
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_EXIST,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }

        {
            tablet.InitSession("client", "session");
            auto response = tablet.ListNodes(RootNodeId)->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, response.NamesSize());
            UNIT_ASSERT_VALUES_EQUAL("file1", response.GetNames(0));
            tablet.DestroySession();
        }

        {
            tablet.SendUnlinkNodeRequest(
                RootNodeId,
                "file1",
                false, // unlinkDirectory
                requestId);

            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }

        {
            tablet.SendUnlinkNodeRequest(
                RootNodeId,
                "file1",
                false, // unlinkDirectory
                requestId);

            auto response = tablet.RecvUnlinkNodeResponse();
            // DupCache shouldn't be used
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }

        {
            tablet.InitSession("client", "session");
            auto response = tablet.ListNodes(RootNodeId)->Record;
            UNIT_ASSERT_VALUES_EQUAL(0, response.NamesSize());
            tablet.DestroySession();
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
