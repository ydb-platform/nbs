#include "tablet.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/algorithm.h>
#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Sessions)
{
    Y_UNIT_TEST(ShouldTrackClients)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        tablet.InitSession("client", "session");
        tablet.DestroySession();
    }

    Y_UNIT_TEST(ShouldTrackHandles)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

    Y_UNIT_TEST(ShouldRestoreClientSession)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

    Y_UNIT_TEST(ShouldResetSessionInBatches)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteSessionHandlesPerTx(2);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle1 = CreateHandle(tablet, id1);
        CreateHandle(tablet, id1);
        tablet.AcquireLock(handle1, 1, 0, 1_KB);

        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "xxx"));
        CreateHandle(tablet, id2);
        CreateHandle(tablet, id2);
        tablet.UnlinkNode(RootNodeId, "xxx", false);

        auto id3 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "yyy"));
        CreateHandle(tablet, id3);
        tablet.UnlinkNode(RootNodeId, "yyy", false);

        auto statsBefore = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(5, statsBefore.GetUsedHandlesCount());

        ui32 commitCount = 0;
        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<NActors::IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == NKikimr::TEvTablet::EvCommit &&
                    event->Get<NKikimr::TEvTablet::TEvCommit>()->TabletID ==
                        tabletId)
                {
                    ++commitCount;
                }
                return NKikimr::TTestActorRuntime::DefaultObserverFunc(event);
            });

        tablet.ResetSession("client", "session", 0, "state");

        env.GetRuntime().SetObserverFunc(
            NKikimr::TTestActorRuntime::DefaultObserverFunc);

        // 5 handles, 2 per tx => 3 chained transactions
        UNIT_ASSERT_VALUES_EQUAL(3, commitCount);

        auto statsAfter = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.GetUsedHandlesCount());
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.GetUsedLocksCount());
        UNIT_ASSERT_VALUES_EQUAL(
            statsBefore.GetUsedNodesCount() - 2,
            statsAfter.GetUsedNodesCount());

        // check that unlinked nodes are cleaned up
        tablet.AssertAccessNodeFailed(id2);
        tablet.AssertAccessNodeFailed(id3);

        // check that handles and locks are invalidated
        auto lock = tablet.AssertAcquireLockFailed(handle1, 1, 0, 1_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            E_FS_BADHANDLE,
            lock->Record.GetError().GetCode());

        // check that state is properly returned
        auto create = tablet.CreateSession("client", "session");
        UNIT_ASSERT_VALUES_EQUAL("state", create->Record.GetSessionState());
    }

    Y_UNIT_TEST(ShouldResumeResetSessionInterruptedByReboot)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteSessionHandlesPerTx(1);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        for (ui32 i = 0; i < 10; ++i) {
            CreateHandle(tablet, id);
        }

        // drop commits after the first 3 to freeze reset progress
        ui32 commitCount = 0;
        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<NActors::IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == NKikimr::TEvTablet::EvCommit &&
                    event->Get<NKikimr::TEvTablet::TEvCommit>()->TabletID ==
                        tabletId &&
                    ++commitCount > 3)
                {
                    return NKikimr::TTestActorRuntime::EEventAction::DROP;
                }
                return NKikimr::TTestActorRuntime::DefaultObserverFunc(event);
            });

        tablet.SendResetSessionRequest("client", "session", 0, "state");

        env.GetRuntime().DispatchEvents(
            NActors::TDispatchOptions{
                .CustomFinalCondition = [&] { return commitCount > 3; }});

        env.GetRuntime().SetObserverFunc(
            NKikimr::TTestActorRuntime::DefaultObserverFunc);

        tablet.RebootTablet();

        // interrupted reset is rejected upon tablet shutdown
        auto response = tablet.RecvResetSessionResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response->Record.GetError().GetCode());

        tablet.InitSession("client", "session");

        // 3 committed batches => 3 of 10 handles destroyed before the reboot
        UNIT_ASSERT_VALUES_EQUAL(
            7,
            GetStorageStats(tablet).GetUsedHandlesCount());

        // interrupted reset must not update session state
        auto create = tablet.CreateSession("client", "session");
        UNIT_ASSERT_VALUES_EQUAL("", create->Record.GetSessionState());
        UNIT_ASSERT_VALUES_EQUAL(
            7,
            GetStorageStats(tablet).GetUsedHandlesCount());

        tablet.ResetSession("client", "session", 0, "state");

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetStorageStats(tablet).GetUsedHandlesCount());

        create = tablet.CreateSession("client", "session");
        UNIT_ASSERT_VALUES_EQUAL("state", create->Record.GetSessionState());
    }

    Y_UNIT_TEST(ShouldRejectResetSessionInterruptedBySessionRecovery)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteSessionHandlesPerTx(1);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        for (ui32 i = 0; i < 10; ++i) {
            CreateHandle(tablet, id);
        }

        // hold commits after the first 3 to freeze reset progress
        ui32 commitCount = 0;
        TVector<TAutoPtr<NActors::IEventHandle>> heldCommits;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, TAutoPtr<NActors::IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == NKikimr::TEvTablet::EvCommit &&
                    event->Get<NKikimr::TEvTablet::TEvCommit>()->TabletID ==
                        tabletId &&
                    ++commitCount > 3)
                {
                    heldCommits.push_back(std::move(event));
                    return true;
                }
                return false;
            });

        tablet.SendResetSessionRequest("client", "session", 0, "state");

        env.GetRuntime().DispatchEvents(
            NActors::TDispatchOptions{
                .CustomFinalCondition = [&] { return !heldCommits.empty(); }});

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto resetSessionInterrupted = counters->GetCounter(
            "AppCriticalEvents/ResetSessionInterrupted",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, resetSessionInterrupted->Val());

        // migration: the session is recovered with a newer seq no while
        // the reset is frozen mid-chain
        tablet.SendCreateSessionRequest("client", "session", "", 1, false);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        env.GetRuntime().SetEventFilter(
            NKikimr::TTestActorRuntimeBase::DefaultFilterFunc);
        for (auto& ev: heldCommits) {
            env.GetRuntime().Send(ev.Release(), nodeIdx);
        }

        {
            auto response = tablet.RecvCreateSessionResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode());
        }

        auto response = tablet.RecvResetSessionResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response->Record.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, resetSessionInterrupted->Val());

        // batches 1-4 committed => 4 of 10 handles destroyed
        UNIT_ASSERT_VALUES_EQUAL(
            6,
            GetStorageStats(tablet).GetUsedHandlesCount());

        // interrupted reset must not update session state
        auto create = tablet.CreateSession("client", "session", "", 1, false);
        UNIT_ASSERT_VALUES_EQUAL("", create->Record.GetSessionState());
        UNIT_ASSERT_VALUES_EQUAL(
            6,
            GetStorageStats(tablet).GetUsedHandlesCount());

        tablet.ResetSession("client", "session", 1, "state");

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetStorageStats(tablet).GetUsedHandlesCount());

        create = tablet.CreateSession("client", "session", "", 1, false);
        UNIT_ASSERT_VALUES_EQUAL("state", create->Record.GetSessionState());
    }

    Y_UNIT_TEST(ShouldRejectResetSessionInterruptedByDestroySession)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteSessionHandlesPerTx(1);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        for (ui32 i = 0; i < 10; ++i) {
            CreateHandle(tablet, id);
        }

        // hold commits after the first 3 to freeze destroy progress
        ui32 commitCount = 0;
        TVector<TAutoPtr<NActors::IEventHandle>> heldCommits;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, TAutoPtr<NActors::IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == NKikimr::TEvTablet::EvCommit &&
                    event->Get<NKikimr::TEvTablet::TEvCommit>()->TabletID ==
                        tabletId &&
                    ++commitCount > 3)
                {
                    heldCommits.push_back(std::move(event));
                    return true;
                }
                return false;
            });

        tablet.SendDestroySessionRequest();

        env.GetRuntime().DispatchEvents(
            NActors::TDispatchOptions{
                .CustomFinalCondition = [&] { return !heldCommits.empty(); }});

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto resetSessionInterrupted = counters->GetCounter(
            "AppCriticalEvents/ResetSessionInterrupted",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, resetSessionInterrupted->Val());

        // the reset joins while the destroy is frozen mid-chain, the destroy
        // wins the race and removes the session from under the reset
        tablet.SendResetSessionRequest("client", "session", 0, "state");
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        env.GetRuntime().SetEventFilter(
            NKikimr::TTestActorRuntimeBase::DefaultFilterFunc);
        for (auto& ev: heldCommits) {
            env.GetRuntime().Send(ev.Release(), nodeIdx);
        }

        {
            auto response = tablet.RecvDestroySessionResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode());
        }

        auto response = tablet.RecvResetSessionResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response->Record.GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, resetSessionInterrupted->Val());

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetStorageStats(tablet).GetUsedHandlesCount());

        auto create = tablet.CreateSession("client", "session");
        UNIT_ASSERT_VALUES_EQUAL("", create->Record.GetSessionState());
    }

    Y_UNIT_TEST(ShouldDestroySessionInBatches)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteSessionHandlesPerTx(2);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle1 = CreateHandle(tablet, id1);
        CreateHandle(tablet, id1);
        tablet.AcquireLock(handle1, 1, 0, 1_KB);

        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "xxx"));
        CreateHandle(tablet, id2);
        CreateHandle(tablet, id2);
        tablet.UnlinkNode(RootNodeId, "xxx", false);

        auto id3 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "yyy"));
        CreateHandle(tablet, id3);
        tablet.UnlinkNode(RootNodeId, "yyy", false);

        auto statsBefore = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(5, statsBefore.GetUsedHandlesCount());

        ui32 commitCount = 0;
        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<NActors::IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == NKikimr::TEvTablet::EvCommit &&
                    event->Get<NKikimr::TEvTablet::TEvCommit>()->TabletID ==
                        tabletId)
                {
                    ++commitCount;
                }
                return NKikimr::TTestActorRuntime::DefaultObserverFunc(event);
            });

        tablet.DestroySession();

        env.GetRuntime().SetObserverFunc(
            NKikimr::TTestActorRuntime::DefaultObserverFunc);

        // 5 handles, 2 per tx => 3 chained transactions
        UNIT_ASSERT_VALUES_EQUAL(3, commitCount);

        auto statsAfter = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.GetUsedHandlesCount());
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.GetUsedLocksCount());
        UNIT_ASSERT_VALUES_EQUAL(0, statsAfter.GetUsedSessionsCount());
        UNIT_ASSERT_VALUES_EQUAL(
            statsBefore.GetUsedNodesCount() - 2,
            statsAfter.GetUsedNodesCount());

        // check that unlinked nodes are cleaned up
        tablet.InitSession("client", "session");
        tablet.AssertAccessNodeFailed(id2);
        tablet.AssertAccessNodeFailed(id3);
    }

    Y_UNIT_TEST(ShouldResumeDestroySessionInterruptedByReboot)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteSessionHandlesPerTx(1);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        for (ui32 i = 0; i < 10; ++i) {
            CreateHandle(tablet, id);
        }

        // drop commits after the first 3 to freeze destroy progress
        ui32 commitCount = 0;
        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<NActors::IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == NKikimr::TEvTablet::EvCommit &&
                    event->Get<NKikimr::TEvTablet::TEvCommit>()->TabletID ==
                        tabletId &&
                    ++commitCount > 3)
                {
                    return NKikimr::TTestActorRuntime::EEventAction::DROP;
                }
                return NKikimr::TTestActorRuntime::DefaultObserverFunc(event);
            });

        tablet.SendDestroySessionRequest();

        env.GetRuntime().DispatchEvents(
            NActors::TDispatchOptions{
                .CustomFinalCondition = [&] { return commitCount > 3; }});

        env.GetRuntime().SetObserverFunc(
            NKikimr::TTestActorRuntime::DefaultObserverFunc);

        tablet.RebootTablet();

        // interrupted destroy is rejected upon tablet shutdown
        auto response = tablet.RecvDestroySessionResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response->GetError().GetCode());

        tablet.InitSession("client", "session");

        // 3 committed batches => 3 of 10 handles destroyed before the reboot
        UNIT_ASSERT_VALUES_EQUAL(
            7,
            GetStorageStats(tablet).GetUsedHandlesCount());

        tablet.DestroySession();

        auto stats = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedHandlesCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedSessionsCount());
    }

    Y_UNIT_TEST(ShouldRejectDestroySessionInterruptedBySessionRecovery)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteSessionHandlesPerTx(1);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        for (ui32 i = 0; i < 10; ++i) {
            CreateHandle(tablet, id);
        }

        // hold commits after the first 3 to freeze destroy progress
        ui32 commitCount = 0;
        TVector<TAutoPtr<NActors::IEventHandle>> heldCommits;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, TAutoPtr<NActors::IEventHandle>& event)
            {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() == NKikimr::TEvTablet::EvCommit &&
                    event->Get<NKikimr::TEvTablet::TEvCommit>()->TabletID ==
                        tabletId &&
                    ++commitCount > 3)
                {
                    heldCommits.push_back(std::move(event));
                    return true;
                }
                return false;
            });

        tablet.SendDestroySessionRequest();

        env.GetRuntime().DispatchEvents(
            NActors::TDispatchOptions{
                .CustomFinalCondition = [&] { return !heldCommits.empty(); }});

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto destroySessionInterrupted = counters->GetCounter(
            "AppCriticalEvents/DestroySessionInterrupted",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, destroySessionInterrupted->Val());

        // migration: the session is recovered with a newer seq no while
        // the destroy is frozen mid-chain
        tablet.SendCreateSessionRequest("client", "session", "", 1, false);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        env.GetRuntime().SetEventFilter(
            NKikimr::TTestActorRuntimeBase::DefaultFilterFunc);
        for (auto& ev: heldCommits) {
            env.GetRuntime().Send(ev.Release(), nodeIdx);
        }

        {
            auto response = tablet.RecvCreateSessionResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                S_OK,
                response->GetError().GetCode());
        }

        auto response = tablet.RecvDestroySessionResponse();
        UNIT_ASSERT_VALUES_EQUAL(
            E_REJECTED,
            response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(1, destroySessionInterrupted->Val());

        // batches 1-4 committed => 4 of 10 handles destroyed
        UNIT_ASSERT_VALUES_EQUAL(
            6,
            GetStorageStats(tablet).GetUsedHandlesCount());

        // the recovered session must survive the interrupted destroy
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            GetStorageStats(tablet).GetUsedSessionsCount());

        tablet.WithSessionSeqNo(1);
        tablet.DestroySession();

        auto stats = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedHandlesCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedSessionsCount());
    }

    Y_UNIT_TEST(ShouldRejectDestroySessionInterruptedByAnotherDestroySession)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxDeleteSessionHandlesPerTx(1);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        for (ui32 i = 0; i < 10; ++i) {
            CreateHandle(tablet, id);
        }

        NMonitoring::TDynamicCountersPtr counters =
            new NMonitoring::TDynamicCounters();
        InitCriticalEventsCounter(counters);
        auto destroySessionInterrupted = counters->GetCounter(
            "AppCriticalEvents/DestroySessionInterrupted",
            true);
        UNIT_ASSERT_VALUES_EQUAL(0, destroySessionInterrupted->Val());

        ui32 commitCount = 0;
        env.GetRuntime().SetObserverFunc(
            [&](TAutoPtr<NActors::IEventHandle>& event)
            {
                if (event->GetTypeRewrite() == NKikimr::TEvTablet::EvCommit &&
                    event->Get<NKikimr::TEvTablet::TEvCommit>()->TabletID ==
                        tabletId)
                {
                    ++commitCount;
                }
                return NKikimr::TTestActorRuntime::DefaultObserverFunc(event);
            });

        tablet.SendDestroySessionRequest();

        env.GetRuntime().DispatchEvents(
            NActors::TDispatchOptions{
                .CustomFinalCondition = [&] { return commitCount >= 3; }});

        env.GetRuntime().SetObserverFunc(
            NKikimr::TTestActorRuntime::DefaultObserverFunc);

        // the second destroy interleaves with the first one, the loser
        // observes the session destroyed by the winner mid-chain
        tablet.SendDestroySessionRequest();

        TVector<ui32> codes;
        codes.push_back(
            tablet.RecvDestroySessionResponse()->GetError().GetCode());
        codes.push_back(
            tablet.RecvDestroySessionResponse()->GetError().GetCode());
        Sort(codes);
        UNIT_ASSERT_VALUES_EQUAL(S_OK, codes[0]);
        UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, codes[1]);
        UNIT_ASSERT_VALUES_EQUAL(1, destroySessionInterrupted->Val());

        auto stats = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedHandlesCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedSessionsCount());
    }

    Y_UNIT_TEST(ShouldCleanupHandlesAndLocksKeptBySession)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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
        NKikimr::NSharedCache::TSharedCacheConfig sharedCacheConfig;
        sharedCacheConfig.SetMemoryLimit(0);
        TTestEnv env({}, {}, &sharedCacheConfig);
        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

        ui32 nodeIdx = env.AddDynamicNode();
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

    Y_UNIT_TEST(ShouldLoadSessionStateWithNonUtf8Ids)
    {
        // ClientId, SessionId and OriginFqdn come from request headers and may
        // hold arbitrary bytes. The tablet persists them, and LoadState must be
        // able to parse them back: when these fields were proto `string`s, a
        // non-UTF-8 value was stored fine but made LoadState VERIFY on every
        // restart.
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        // Non-UTF-8 values.
        const TString clientId =
            "^\xa7" "H\x16" "NSLN\xa5\x83\xe4\xf6\xf4@\xcc\xa5";
        const TString sessionId = "\xff\xfe" "session";
        const TString originFqdn = "\xc3\x28" "host";

        {
            auto request =
                tablet.CreateCreateSessionRequest(clientId, sessionId);
            request->Record.MutableHeaders()->SetOriginFqdn(originFqdn);
            tablet.SendRequest(std::move(request));

            auto response = tablet.RecvCreateSessionResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(
                sessionId,
                response->Record.GetSessionId());
        }
        tablet.SetHeaders(clientId, sessionId, 0);

        // Persist a handle, a lock and a dup cache entry for the session too.
        const ui64 id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const ui64 handle = CreateHandle(tablet, id);
        tablet.AcquireLock(handle, 1, 0, 4_KB);

        auto createDedupRequest = [&] () {
            auto request = tablet.CreateCreateHandleRequest(
                RootNodeId,
                "dedup",
                TCreateHandleArgs::CREATE_EXL);
            request->Record.MutableHeaders()->SetRequestId(100500);
            return request;
        };

        ui64 dedupHandle = 0;
        tablet.SendRequest(createDedupRequest());
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            dedupHandle = response->Record.GetHandle();
            UNIT_ASSERT(dedupHandle);
        }

        // Sessions, session history, handles, locks and the dup cache are all
        // read back here.
        tablet.RebootTablet();
        tablet.InitSession(clientId, sessionId);

        // The lock survived the reboot...
        auto lockResponse = tablet.AssertAcquireLockFailed(handle, 2, 0, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(
            E_FS_WOULDBLOCK,
            lockResponse->GetError().GetCode());

        // ...as did the dup cache entry...
        tablet.SendRequest(createDedupRequest());
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetErrorReason());
            UNIT_ASSERT_VALUES_EQUAL(dedupHandle, response->Record.GetHandle());
        }

        // ...and both handles.
        tablet.DestroyHandle(handle);
        tablet.DestroyHandle(dedupHandle);
        tablet.DestroySession();
    }

    Y_UNIT_TEST(ShouldSupportInterhostMigrationScenarioInPresentsOfResets)
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


        ui32 nodeIdx1 = env.AddDynamicNode();
        ui32 nodeIdx2 = env.AddDynamicNode();

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

        ui32 nodeIdx = env.AddDynamicNode();
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
        features.SetHasXAttrs(true);
        features.SetMaxFuseLoopThreads(1);
        features.SetTabletDirectRdmaEnabled(false);
        features.SetAsyncHandleOperationIdlePeriod(
            TDuration::MilliSeconds(50).MilliSeconds());
        features.SetAsyncHandleOperationBatchSize(32);

        DoTestShouldReturnFeaturesInCreateSessionResponse(config, features);

        config.SetTwoStageReadEnabled(true);
        config.SetTwoStageReadThreshold(64_KB);
        config.SetThreeStageWriteEnabled(true);
        config.SetThreeStageWriteThreshold(10_MB);
        config.SetEntryTimeout(TDuration::Seconds(10).MilliSeconds());
        config.SetNegativeEntryTimeout(TDuration::Seconds(1).MilliSeconds());
        config.SetAttrTimeout(TDuration::Seconds(20).MilliSeconds());
        config.SetXAttrCacheTimeout(TDuration::Seconds(30).MilliSeconds());
        config.SetXAttrCacheLimit(1024);
        config.SetNodeRefCacheLimit(2048);
        config.SetNodeAttrsCacheLimit(4096);
        config.SetPreferredBlockSizeMultiplier(2);
        config.SetAsyncDestroyHandleEnabled(true);
        config.SetAsyncDestroyReadOnlyHandleEnabled(true);
        config.SetAsyncHandleOperationIdlePeriod(
            TDuration::MilliSeconds(100).MilliSeconds());
        config.SetAsyncHandleOperationDrainPeriod(
            TDuration::MilliSeconds(10).MilliSeconds());
        config.SetAsyncHandleOperationBatchSize(32);
        config.SetGuestPageCacheDisabled(true);
        config.SetExtendedAttributesDisabled(true);
        config.SetServerWriteBackCacheEnabled(true);
        config.SetServerWriteBackCacheStateFileSize(2_MB);
        config.SetServerWriteBackCacheFlushWritesInParallelEnabled(true);
        config.SetParentlessFilesOnly(true);
        config.SetAllowHandlelessIO(true);
        config.SetDirectoryHandlesStorageEnabled(true);
        config.SetDirectoryHandlesTableSize(1000);
        config.SetDirectoryHandlesPersistentHandleMaxSize(4_GB);
        config.SetZeroCopyWriteEnabled(true);
        config.SetGuestHandleKillPrivV2Enabled(true);
        config.SetGuestPosixAclEnabled(true);
        config.SetZeroCopyReadEnabled(true);
        config.SetBlockChecksumsInProfileLogEnabled(true);
        config.SetReadBlobDisabled(true);
        config.SetWriteBlobDisabled(true);
        config.SetUseCustomReadDataResponseParser(true);
        config.SetExternalReadDataPayload(true);
        config.SetTabletDirectRdmaEnabled(true);
        config.SetAvailabilityTrackingEnabled(true);
        config.SetAvailabilityTrackingInterval(
            TDuration::Seconds(15).MilliSeconds());
        config.SetMaxRequestSize(4_MB);

        features.SetTwoStageReadEnabled(true);
        features.SetTwoStageReadThreshold(64_KB);
        features.SetEntryTimeout(TDuration::Seconds(10).MilliSeconds());
        features.SetNegativeEntryTimeout(TDuration::Seconds(1).MilliSeconds());
        features.SetAttrTimeout(TDuration::Seconds(20).MilliSeconds());
        features.SetXAttrCacheTimeout(TDuration::Seconds(30).MilliSeconds());
        features.SetXAttrCacheLimit(1024);
        features.SetNodeRefCacheLimit(2048);
        features.SetNodeAttrsCacheLimit(4096);
        features.SetThreeStageWriteEnabled(true);
        features.SetThreeStageWriteThreshold(10_MB);
        features.SetPreferredBlockSize(4_KB * 2);
        features.SetAsyncDestroyHandleEnabled(true);
        features.SetAsyncDestroyReadOnlyHandleEnabled(true);
        features.SetAsyncHandleOperationIdlePeriod(
            TDuration::MilliSeconds(100).MilliSeconds());
        features.SetAsyncHandleOperationDrainPeriod(
            TDuration::MilliSeconds(10).MilliSeconds());
        features.SetAsyncHandleOperationBatchSize(32);
        features.SetGuestPageCacheDisabled(true);
        features.SetExtendedAttributesDisabled(true);
        features.SetServerWriteBackCacheEnabled(true);
        features.SetServerWriteBackCacheStateFileSize(2_MB);
        features.SetServerWriteBackCacheFlushWritesInParallelEnabled(true);
        features.SetParentlessFilesOnly(true);
        features.SetAllowHandlelessIO(true);
        features.SetDirectoryHandlesStorageEnabled(true);
        features.SetDirectoryHandlesTableSize(1000);
        features.SetDirectoryHandlesPersistentHandleMaxSize(4_GB);
        features.SetZeroCopyWriteEnabled(true);
        features.SetGuestHandleKillPrivV2Enabled(true);
        features.SetGuestPosixAclEnabled(true);
        features.SetZeroCopyReadEnabled(true);
        features.SetBlockChecksumsInProfileLogEnabled(true);
        features.SetReadBlobDisabled(true);
        features.SetWriteBlobDisabled(true);
        features.SetUseCustomReadDataResponseParser(true);
        features.SetExternalReadDataPayload(true);
        features.SetTabletDirectRdmaEnabled(true);
        features.SetAvailabilityTrackingEnabled(true);
        features.SetAvailabilityTrackingInterval(
            TDuration::Seconds(15).MilliSeconds());
        features.SetMaxRequestSize(4_MB);

        DoTestShouldReturnFeaturesInCreateSessionResponse(config, features);
    }

    Y_UNIT_TEST(
        ShouldReturnServerWriteBackCacheStateFileSizeFromStorageConfigOverride)
    {
        NProto::TStorageConfig config;
        config.SetServerWriteBackCacheStateFileSize(2_MB);
        TTestEnv env({}, config);

        const ui32 nodeIdx = env.AddDynamicNode();
        const ui64 tabletId = env.BootIndexTablet(nodeIdx);
        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        const auto checkStateFileSize = [&](ui64 expectedStateFileSize)
        {
            auto response = tablet.CreateSession("client", "session");
            UNIT_ASSERT_VALUES_EQUAL(
                expectedStateFileSize,
                response->Record.GetFileStore()
                    .GetFeatures()
                    .GetServerWriteBackCacheStateFileSize());
        };

        checkStateFileSize(2_MB);

        NProto::TStorageConfig patch;
        patch.SetServerWriteBackCacheStateFileSize(4_GB + 100500);
        tablet.ChangeStorageConfig(patch);
        tablet.RebootTablet();
        checkStateFileSize(4_GB + 100500);

        // Explicit zero uses the client default instead of the server-wide size.
        patch.SetServerWriteBackCacheStateFileSize(0);
        tablet.ChangeStorageConfig(patch);
        tablet.RebootTablet();
        checkStateFileSize(0);
    }

    Y_UNIT_TEST(ShouldHandleCommitIdOverflowInDestroySession)
    {
        const ui32 maxTabletStep = 4;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        TString sessionId = "good_session";
        tablet.InitSession("client", sessionId);

        auto id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, sessionId));
        auto handle = CreateHandle(tablet, id);
        Y_UNUSED(handle);

        tablet.UnlinkNode(RootNodeId, sessionId, false);

        bool destroySessionFailed = false;

        do {
            tablet.SendDestroySessionRequest();
            auto response = tablet.RecvDestroySessionResponse();
            reconnectIfNeeded();

            if (!HasError(response->GetError())) {
                break;
            }

            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                response->GetError().GetCode());
            destroySessionFailed = true;
        } while (true);

        tablet.InitSession("client", sessionId);
        tablet.AssertGetNodeAttrFailed(id);
        tablet.DestroySession();

        UNIT_ASSERT_VALUES_EQUAL(2, rebootTracker.GetGenerationCount());
        UNIT_ASSERT(destroySessionFailed);
    }

    Y_UNIT_TEST(ShouldHandleCommitIdOverflowInDestroySessionInBatches)
    {
        const ui32 maxTabletStep = 16;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);
        storageConfig.SetMaxDeleteSessionHandlesPerTx(1);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        TString sessionId = "good_session";
        tablet.InitSession("client", sessionId);

        auto id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, sessionId));
        for (ui32 i = 0; i < 10; ++i) {
            CreateHandle(tablet, id);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            GetStorageStats(tablet).GetUsedHandlesCount());

        bool destroySessionFailed = false;

        do {
            tablet.SendDestroySessionRequest();
            auto response = tablet.RecvDestroySessionResponse();
            reconnectIfNeeded();

            if (!HasError(response->GetError())) {
                break;
            }

            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                response->GetError().GetCode());
            destroySessionFailed = true;
        } while (true);

        auto stats = GetStorageStats(tablet);
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedHandlesCount());
        UNIT_ASSERT_VALUES_EQUAL(0, stats.GetUsedSessionsCount());

        UNIT_ASSERT_VALUES_EQUAL(2, rebootTracker.GetGenerationCount());
        UNIT_ASSERT(destroySessionFailed);
    }

    Y_UNIT_TEST(ShouldHandleCommitIdOverflowInResetSession)
    {
        const ui32 maxTabletStep = 4;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        TString sessionId = "good_session";
        tablet.InitSession("client", sessionId);

        auto id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, sessionId));
        auto handle = CreateHandle(tablet, id);
        Y_UNUSED(handle);

        tablet.UnlinkNode(RootNodeId, sessionId, false);

        bool resetSessionFailed = false;

        do {
            tablet.SendResetSessionRequest("client", sessionId, 0, "state");
            auto response = tablet.RecvResetSessionResponse();
            reconnectIfNeeded();

            if (!HasError(response->GetError())) {
                break;
            }

            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                response->GetError().GetCode());
            resetSessionFailed = true;
        } while (true);

        tablet.InitSession("client", sessionId);
        tablet.AssertGetNodeAttrFailed(id);
        tablet.DestroySession();

        UNIT_ASSERT_VALUES_EQUAL(2, rebootTracker.GetGenerationCount());
        UNIT_ASSERT(resetSessionFailed);
    }

    Y_UNIT_TEST(ShouldHandleCommitIdOverflowInResetSessionInBatches)
    {
        const ui32 maxTabletStep = 16;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);
        storageConfig.SetMaxDeleteSessionHandlesPerTx(1);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        TString sessionId = "good_session";
        tablet.InitSession("client", sessionId);

        auto id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, sessionId));
        for (ui32 i = 0; i < 10; ++i) {
            CreateHandle(tablet, id);
        }
        UNIT_ASSERT_VALUES_EQUAL(
            10,
            GetStorageStats(tablet).GetUsedHandlesCount());

        bool resetSessionFailed = false;

        do {
            tablet.SendResetSessionRequest("client", sessionId, 0, "state");
            auto response = tablet.RecvResetSessionResponse();
            reconnectIfNeeded();

            if (!HasError(response->GetError())) {
                break;
            }

            UNIT_ASSERT_VALUES_EQUAL(
                E_REJECTED,
                response->GetError().GetCode());
            resetSessionFailed = true;
        } while (true);

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            GetStorageStats(tablet).GetUsedHandlesCount());

        auto create = tablet.CreateSession("client", sessionId);
        UNIT_ASSERT_VALUES_EQUAL("state", create->Record.GetSessionState());

        UNIT_ASSERT_VALUES_EQUAL(2, rebootTracker.GetGenerationCount());
        UNIT_ASSERT(resetSessionFailed);
    }

    Y_UNIT_TEST(ShardShouldNotCheckSessionUponCreateNodeAndUnlinkNode)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        const ui64 requestId = 111;
        const ui32 shardNo = 222;
        const TString mainFsId = "main_fs";
        const TString shardId = TStringBuilder() << mainFsId << "_s" << shardNo;
        tablet.ConfigureAsShard(shardNo, mainFsId, shardId);

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

    Y_UNIT_TEST(ShouldProvideRequestedSessions)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session", {}, 3);

        {
            auto response = tablet.RecoverShardSessions(
                TVector<TString>{"session", "unknown"});
            const auto& record = response->Record;
            // unknown sessions are skipped
            UNIT_ASSERT_VALUES_EQUAL(1, record.SessionsSize());

            const auto& info = record.GetSessions(0);
            UNIT_ASSERT_VALUES_EQUAL("session", info.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("client", info.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(3, info.GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(3, info.GetMaxRwSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(1, info.SubSessionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                3,
                info.GetSubSessions(0).GetSeqNo());
            UNIT_ASSERT(!info.GetSubSessions(0).GetReadOnly());
        }

        {
            auto response = tablet.RecoverShardSessions(
                TVector<TString>{"unknown"});
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.SessionsSize());
        }

        // the session becomes orphan after the restart - no one is allowed
        // to use it until its client recovers it
        tablet.RebootTablet();

        {
            auto response = tablet.RecoverShardSessions(
                TVector<TString>{"session"});
            const auto& record = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, record.SessionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                record.GetSessions(0).SubSessionsSize());
        }

        tablet.InitSession("client", "session", {}, 3);

        {
            auto response = tablet.RecoverShardSessions(
                TVector<TString>{"session"});
            const auto& record = response->Record;
            UNIT_ASSERT_VALUES_EQUAL(1, record.SessionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                record.GetSessions(0).SubSessionsSize());
        }

        tablet.DestroySession();
    }

    Y_UNIT_TEST(ShardShouldNotProvideSessions)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.ConfigureAsShard(1, "main_fs", "main_fs_s1");
        tablet.InitSession("client", "session");

        // the sessions are owned by the main tablet
        auto response = tablet.AssertRecoverShardSessionsFailed(
            TVector<TString>{"session"});
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetError().GetCode(),
            FormatError(response->GetError()));
    }

    Y_UNIT_TEST(ShouldRejectSyncShardSessionsInMainTablet)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        NProtoPrivate::TShardSessionInfo info;
        info.SetClientId("client");
        info.SetSessionId("session");
        info.AddSubSessions()->SetSeqNo(1);

        auto response = tablet.AssertSyncShardSessionsFailed(
            TVector<NProtoPrivate::TShardSessionInfo>{info});
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_ARGUMENT,
            response->GetError().GetCode(),
            FormatError(response->GetError()));

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet.DescribeSessions()->Record.SessionsSize());
    }

    Y_UNIT_TEST(ShardShouldCreateAndRecoverSessionsInBatches)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        const ui32 shardNo = 1;
        const TString mainFsId = "main_fs";
        const TString shardId = TStringBuilder() << mainFsId << "_s" << shardNo;
        tablet.ConfigureAsShard(shardNo, mainFsId, shardId);

        // seqno + readonly flag
        using TSubSessionList = TVector<std::pair<ui64, bool>>;
        auto makeSessionInfos = [] (const TSubSessionList& subSessions) {
            NProtoPrivate::TShardSessionInfo info;
            info.SetClientId("client");
            info.SetSessionId("session");

            ui64 maxSeqNo = 0;
            ui64 maxRwSeqNo = 0;
            for (const auto& [seqNo, readOnly]: subSessions) {
                auto* subSession = info.AddSubSessions();
                subSession->SetSeqNo(seqNo);
                subSession->SetReadOnly(readOnly);

                maxSeqNo = Max(maxSeqNo, seqNo);
                if (!readOnly) {
                    maxRwSeqNo = Max(maxRwSeqNo, seqNo);
                }
            }
            info.SetMaxSeqNo(maxSeqNo);
            info.SetMaxRwSeqNo(maxRwSeqNo);

            return TVector<NProtoPrivate::TShardSessionInfo>{info};
        };

        auto describeSessions = [&] () {
            return tablet.DescribeSessions()->Record;
        };

        auto assertInvalidSession = [&] (ui64 nodeId) {
            auto response = tablet.AssertCreateHandleFailed(
                nodeId,
                TString(),
                TCreateHandleArgs::RDWR);
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_INVALID_SESSION,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        };

        UNIT_ASSERT_VALUES_EQUAL(0, describeSessions().SessionsSize());

        // nodes are created in the shards without sessions
        const ui64 id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "file"));

        tablet.SetHeaders("client", "session", 1);
        assertInvalidSession(id);

        // an info without subsessions describes an inactive session -
        // there's no need to create it
        {
            auto response = tablet.SyncShardSessions(
                makeSessionInfos(TSubSessionList{}));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetCreatedSessionCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetRecoveredSessionCount());
            UNIT_ASSERT_VALUES_EQUAL(0, describeSessions().SessionsSize());
        }

        {
            auto response = tablet.SyncShardSessions(
                makeSessionInfos(TSubSessionList{{1, false}}));
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetCreatedSessionCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetRecoveredSessionCount());
        }

        {
            const auto sessions = describeSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.SessionsSize());

            const auto& session = sessions.GetSessions(0);
            UNIT_ASSERT_VALUES_EQUAL("session", session.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("client", session.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(1, session.GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(1, session.GetMaxRwSeqNo());
            UNIT_ASSERT(!session.GetIsOrphan());
        }

        // the session can be used now
        const ui64 handle = CreateHandle(tablet, id);

        // but only by the subsessions which are listed in the info
        tablet.SetHeaders("client", "session", 2);
        assertInvalidSession(id);

        // the session becomes orphan after the restart
        tablet.RebootTablet();

        {
            const auto sessions = describeSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.SessionsSize());
            UNIT_ASSERT(sessions.GetSessions(0).GetIsOrphan());
        }

        tablet.SetHeaders("client", "session", 1);
        assertInvalidSession(id);

        // recovering the session, a new subsession appears at the same time
        {
            auto response = tablet.SyncShardSessions(
                makeSessionInfos(TSubSessionList{{1, false}, {2, true}}));
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetCreatedSessionCount());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.GetRecoveredSessionCount());
        }

        {
            const auto sessions = describeSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.SessionsSize());

            const auto& session = sessions.GetSessions(0);
            UNIT_ASSERT_VALUES_EQUAL(2, session.GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(1, session.GetMaxRwSeqNo());
            UNIT_ASSERT(!session.GetIsOrphan());
        }

        // the handle survived, both subsessions are able to use the session
        tablet.SetHeaders("client", "session", 1);
        tablet.WriteData(handle, 0, 4_KB, 'a');
        tablet.SetHeaders("client", "session", 2);
        tablet.DestroyHandle(handle);

        // the subsessions are replaced with the ones from the info
        tablet.SyncShardSessions(
            makeSessionInfos(TSubSessionList{{2, false}}));

        tablet.SetHeaders("client", "session", 1);
        assertInvalidSession(id);

        {
            const auto sessions = describeSessions();
            UNIT_ASSERT_VALUES_EQUAL(1, sessions.SessionsSize());

            const auto& session = sessions.GetSessions(0);
            UNIT_ASSERT_VALUES_EQUAL(2, session.GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(2, session.GetMaxRwSeqNo());
        }
    }

    Y_UNIT_TEST(ShardShouldDescribeItsSessionsForMainTablet)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        // a main tablet is not a shard
        {
            auto response = tablet.AssertDescribeShardSessionsFailed();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_ARGUMENT,
                response->GetStatus(),
                response->GetErrorReason());
        }

        const ui32 shardNo = 1;
        const TString mainFsId = "main_fs";
        const TString shardId = TStringBuilder() << mainFsId << "_s" << shardNo;
        tablet.ConfigureAsShard(shardNo, mainFsId, shardId);

        // the main tablet learns about the restarts of the shard from this
        ui32 generation = 0;

        {
            auto response = tablet.DescribeShardSessions();
            UNIT_ASSERT(!response->Record.GetAdapterModeEnabled());
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.SessionsSize());
            generation = response->Record.GetGeneration();
        }

        // a read-write subsession and a read-only one
        NProtoPrivate::TShardSessionInfo info;
        info.SetClientId("client");
        info.SetSessionId("session");
        info.SetMaxSeqNo(2);
        info.SetMaxRwSeqNo(1);
        info.AddSubSessions()->SetSeqNo(1);
        {
            auto* subSession = info.AddSubSessions();
            subSession->SetSeqNo(2);
            subSession->SetReadOnly(true);
        }

        {
            auto response = tablet.SyncShardSessions(
                TVector<NProtoPrivate::TShardSessionInfo>{info});
            UNIT_ASSERT_VALUES_EQUAL(
                generation,
                response->Record.GetGeneration());
        }

        // the shard describes the session the way it was described to it
        {
            auto response = tablet.DescribeShardSessions();
            UNIT_ASSERT_VALUES_EQUAL(
                generation,
                response->Record.GetGeneration());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.SessionsSize());

            const auto& session = response->Record.GetSessions(0);
            UNIT_ASSERT_VALUES_EQUAL("session", session.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("client", session.GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(2, session.GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(1, session.GetMaxRwSeqNo());

            // seqno -> read-only
            TMap<ui64, bool> subSessions;
            for (const auto& subSession: session.GetSubSessions()) {
                subSessions[subSession.GetSeqNo()] = subSession.GetReadOnly();
            }
            UNIT_ASSERT_VALUES_EQUAL(2, subSessions.size());
            UNIT_ASSERT(subSessions.contains(1));
            UNIT_ASSERT(!subSessions[1]);
            UNIT_ASSERT(subSessions.contains(2));
            UNIT_ASSERT(subSessions[2]);
        }

        // the session is not active after the restart of the shard - it's
        // described without the subsessions
        tablet.RebootTablet();

        {
            auto response = tablet.DescribeShardSessions();
            UNIT_ASSERT_C(
                response->Record.GetGeneration() > generation,
                response->Record.GetGeneration());
            UNIT_ASSERT_VALUES_EQUAL(1, response->Record.SessionsSize());

            const auto& session = response->Record.GetSessions(0);
            UNIT_ASSERT_VALUES_EQUAL("session", session.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(2, session.GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(1, session.GetMaxRwSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(0, session.SubSessionsSize());
        }
    }

    Y_UNIT_TEST(ShardShouldResetAndDestroySessionsInBatches)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        const ui32 shardNo = 1;
        const TString mainFsId = "main_fs";
        const TString shardId = TStringBuilder() << mainFsId << "_s" << shardNo;
        tablet.ConfigureAsShard(shardNo, mainFsId, shardId);

        using TSessionInfos = TVector<NProtoPrivate::TShardSessionInfo>;
        using TRequests = TVector<NProtoPrivate::TShardSessionRequest>;

        // a session with read-write subsessions
        auto makeSessionInfo = [] (const TString& no, TVector<ui64> seqNos) {
            NProtoPrivate::TShardSessionInfo info;
            info.SetClientId("client" + no);
            info.SetSessionId("session" + no);
            for (const ui64 seqNo: seqNos) {
                info.SetMaxSeqNo(Max<ui64>(info.GetMaxSeqNo(), seqNo));
                info.SetMaxRwSeqNo(Max<ui64>(info.GetMaxRwSeqNo(), seqNo));
                info.AddSubSessions()->SetSeqNo(seqNo);
            }
            return info;
        };

        // ResetSession or DestroySession of the main tablet
        auto makeRequest = [] (const TString& sessionId, ui64 seqNo) {
            NProtoPrivate::TShardSessionRequest request;
            request.SetSessionId(sessionId);
            request.SetSessionSeqNo(seqNo);
            return request;
        };

        // the sessions of the tablet and their seqnos
        auto describeSessions = [&] () -> TString {
            TSet<TString> sessions;
            const auto response = tablet.DescribeSessions();
            for (const auto& session: response->Record.GetSessions()) {
                sessions.insert(TStringBuilder()
                    << session.GetSessionId() << ":" << session.GetMaxSeqNo());
            }

            TStringBuilder sb;
            for (const auto& session: sessions) {
                if (!sb.empty()) {
                    sb << ",";
                }
                sb << session;
            }
            return sb;
        };

        tablet.SyncShardSessions(TSessionInfos{
            makeSessionInfo("1", {1}),
            makeSessionInfo("2", {1}),
            makeSessionInfo("3", {1})});
        UNIT_ASSERT_VALUES_EQUAL(
            "session1:1,session2:1,session3:1",
            describeSessions());

        const ui64 id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "file"));

        tablet.SetHeaders("client1", "session1", 1);
        const ui64 handle1 = CreateHandle(tablet, id);
        tablet.SetHeaders("client2", "session2", 1);
        CreateHandle(tablet, id);
        tablet.SetHeaders("client3", "session3", 1);
        const ui64 handle3 = CreateHandle(tablet, id);

        // the requests are processed the regular way: a seqno which is not
        // the seqno of the writer changes nothing
        tablet.SyncShardSessions(
            TSessionInfos{},
            TRequests{makeRequest("session1", 7)},
            TRequests{makeRequest("session2", 7)});

        UNIT_ASSERT_VALUES_EQUAL(
            "session1:1,session2:1,session3:1",
            describeSessions());

        tablet.SetHeaders("client1", "session1", 1);
        tablet.WriteData(handle1, 0, 4_KB, 'a');

        // session1 is reset, session2 is destroyed, session3 is not touched,
        // the sessions which the shard doesn't have are skipped
        {
            auto response = tablet.SyncShardSessions(
                TSessionInfos{},
                TRequests{
                    makeRequest("session1", 1),
                    makeRequest("unknown", 1)},
                TRequests{
                    makeRequest("session2", 1),
                    makeRequest("unknown", 1)});
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetCreatedSessionCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetRecoveredSessionCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(
            "session1:1,session3:1",
            describeSessions());

        // session1 is alive but it has lost its handle
        tablet.SetHeaders("client1", "session1", 1);
        tablet.AssertWriteDataFailed(handle1, 0, 4_KB, 'a');
        const ui64 newHandle1 = CreateHandle(tablet, id);
        tablet.WriteData(newHandle1, 0, 4_KB, 'a');

        tablet.SetHeaders("client3", "session3", 1);
        tablet.WriteData(handle3, 0, 4_KB, 'a');

        // the shard doesn't know about the subsessions with seqno 2 yet, so
        // the requests of these subsessions change nothing...
        tablet.SyncShardSessions(
            TSessionInfos{},
            TRequests{makeRequest("session1", 2)},
            TRequests{makeRequest("session3", 2)});

        UNIT_ASSERT_VALUES_EQUAL(
            "session1:1,session3:1",
            describeSessions());

        tablet.SetHeaders("client3", "session3", 1);
        tablet.WriteData(handle3, 0, 4_KB, 'a');

        // ...unless a request comes with the session the way the main tablet
        // sees it: the shard makes the subsessions of its session the same
        // and comes to the same decision as the main tablet
        {
            auto resetRequest = makeRequest("session1", 2);
            *resetRequest.MutableSession() = makeSessionInfo("1", {1, 2});

            // the subsession might be already gone from the main tablet -
            // MaxSeqNo and MaxRwSeqNo are enough
            auto destroyRequest = makeRequest("session3", 2);
            auto* session = destroyRequest.MutableSession();
            *session = makeSessionInfo("3", {});
            session->SetMaxSeqNo(2);
            session->SetMaxRwSeqNo(2);

            // the sessions which the shard doesn't have are not created
            auto unknownRequest = makeRequest("session4", 2);
            *unknownRequest.MutableSession() = makeSessionInfo("4", {1, 2});

            auto response = tablet.SyncShardSessions(
                TSessionInfos{},
                TRequests{resetRequest},
                TRequests{destroyRequest, unknownRequest});
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetCreatedSessionCount());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                response->Record.GetRecoveredSessionCount());
        }

        UNIT_ASSERT_VALUES_EQUAL("session1:2", describeSessions());

        // session1 has been reset once again
        tablet.SetHeaders("client1", "session1", 2);
        tablet.AssertWriteDataFailed(newHandle1, 0, 4_KB, 'a');
        CreateHandle(tablet, id);

        // that's the way the main tablet destroys the sessions which it
        // doesn't have: such a session has neither subsessions nor seqnos
        // there
        {
            auto destroyRequest = makeRequest("session1", 0);
            *destroyRequest.MutableSession() = makeSessionInfo("1", {});

            tablet.SyncShardSessions(
                TSessionInfos{},
                TRequests{},
                TRequests{destroyRequest});
        }

        UNIT_ASSERT_VALUES_EQUAL("", describeSessions());
    }

    Y_UNIT_TEST(ShardShouldReportAndCancelSessionRecoveries)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetShardSessionRecoveryOnDemandEnabled(true);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        const ui32 shardNo = 1;
        const TString mainFsId = "main_fs";
        const TString shardId = TStringBuilder() << mainFsId << "_s" << shardNo;
        tablet.ConfigureAsShard(shardNo, mainFsId, shardId);

        const ui64 id =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "file"));

        // the main tablet doesn't respond to the requests of the shard
        ui32 sessionRequests = 0;
        env.GetRuntime().SetEventFilter(
            [&] (auto& runtime, TAutoPtr<NActors::IEventHandle>& event) {
                Y_UNUSED(runtime);
                if (event->GetTypeRewrite() ==
                        TEvIndexTablet::EvRecoverShardSessionsRequest)
                {
                    ++sessionRequests;
                    return true;
                }
                return false;
            });

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet.DescribeShardSessions()->Record.RecoveringSessionIdsSize());

        // the shard doesn't have the session - the request has to wait for
        // the main tablet
        tablet.SetHeaders("client", "session", 1);
        tablet.SendCreateHandleRequest(id, TString(), TCreateHandleArgs::RDWR);
        env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        UNIT_ASSERT_VALUES_EQUAL(1, sessionRequests);

        {
            const auto response = tablet.DescribeShardSessions();
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.SessionsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                1,
                response->Record.RecoveringSessionIdsSize());
            UNIT_ASSERT_VALUES_EQUAL(
                "session",
                response->Record.GetRecoveringSessionIds(0));
        }

        // the main tablet tells the shard to destroy the session - the
        // response which the shard is waiting for is outdated, the request
        // of the client should be retried
        NProtoPrivate::TShardSessionRequest destroyRequest;
        destroyRequest.SetSessionId("session");
        destroyRequest.SetSessionSeqNo(1);
        tablet.SendSyncShardSessionsRequest(
            TVector<NProtoPrivate::TShardSessionInfo>{},
            TVector<NProtoPrivate::TShardSessionRequest>{},
            TVector<NProtoPrivate::TShardSessionRequest>{destroyRequest});

        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_REJECTED,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }

        {
            auto response = tablet.RecvSyncShardSessionsResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetError().GetCode(),
                FormatError(response->GetError()));
        }

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet.DescribeShardSessions()->Record.RecoveringSessionIdsSize());
    }

    Y_UNIT_TEST(ShouldBuildShardSessionsDiff)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetLazyShardSessionCreationEnabled(true);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        // two read-write subsessions
        tablet.CreateSession("client", "session", TString(), 1, false);
        tablet.CreateSession("client", "session", TString(), 2, false);

        using TSeqNos = TVector<ui64>;
        using TShardSessions = NProtoPrivate::TDescribeShardSessionsResponse;

        // a session of a shard the way the shard describes it: a session
        // which is not active has no subsessions
        auto makeShardSession = [] (
            const TString& sessionId,
            ui64 maxSeqNo,
            ui64 maxRwSeqNo,
            const TSeqNos& seqNos)
        {
            NProtoPrivate::TShardSessionInfo info;
            info.SetSessionId(sessionId);
            info.SetClientId("client");
            info.SetMaxSeqNo(maxSeqNo);
            info.SetMaxRwSeqNo(maxRwSeqNo);
            for (const ui64 seqNo: seqNos) {
                info.AddSubSessions()->SetSeqNo(seqNo);
            }
            return info;
        };

        // the subsessions which should be sent to the shard
        auto seqNosToSync = [] (const auto& response) {
            UNIT_ASSERT_VALUES_EQUAL(1, response->SessionsToSync.size());
            const auto& info = response->SessionsToSync[0];
            UNIT_ASSERT_VALUES_EQUAL("session", info.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL("client", info.GetClientId());

            TSeqNos seqNos;
            for (const auto& subSession: info.GetSubSessions()) {
                seqNos.push_back(subSession.GetSeqNo());
            }
            Sort(seqNos);
            return seqNos;
        };

        // the shard doesn't have the session
        {
            TShardSessions shardSessions;
            auto response =
                tablet.BuildShardSessionsDiff("shard", shardSessions);
            ASSERT_VECTORS_EQUAL(TSeqNos({1, 2}), seqNosToSync(response));
            UNIT_ASSERT_VALUES_EQUAL(0, response->SessionsToDestroy.size());
        }

        // the shard has the same session
        {
            TShardSessions shardSessions;
            *shardSessions.AddSessions() =
                makeShardSession("session", 2, 2, {1, 2});
            auto response =
                tablet.BuildShardSessionsDiff("shard", shardSessions);
            UNIT_ASSERT_VALUES_EQUAL(0, response->SessionsToSync.size());
            UNIT_ASSERT_VALUES_EQUAL(0, response->SessionsToDestroy.size());
        }

        // the session of the shard is not active
        {
            TShardSessions shardSessions;
            *shardSessions.AddSessions() =
                makeShardSession("session", 2, 2, {});
            auto response =
                tablet.BuildShardSessionsDiff("shard", shardSessions);
            ASSERT_VECTORS_EQUAL(TSeqNos({1, 2}), seqNosToSync(response));
        }

        // the session of the shard has outdated seqnos
        {
            TShardSessions shardSessions;
            *shardSessions.AddSessions() =
                makeShardSession("session", 1, 1, {1});
            auto response =
                tablet.BuildShardSessionsDiff("shard", shardSessions);
            ASSERT_VECTORS_EQUAL(TSeqNos({1, 2}), seqNosToSync(response));
        }

        // one of the subsessions of the shard has a different mode
        {
            TShardSessions shardSessions;
            auto* session = shardSessions.AddSessions();
            *session = makeShardSession("session", 2, 2, {1, 2});
            session->MutableSubSessions(0)->SetReadOnly(true);
            auto response =
                tablet.BuildShardSessionsDiff("shard", shardSessions);
            ASSERT_VECTORS_EQUAL(TSeqNos({1, 2}), seqNosToSync(response));
        }

        // one of the subsessions gets destroyed - MaxSeqNo and MaxRwSeqNo
        // stay the same
        tablet.SetHeaders("client", "session", 1);
        tablet.DestroySession();

        // the shard still has the destroyed subsession
        {
            TShardSessions shardSessions;
            *shardSessions.AddSessions() =
                makeShardSession("session", 2, 2, {1, 2});
            auto response =
                tablet.BuildShardSessionsDiff("shard", shardSessions);
            ASSERT_VECTORS_EQUAL(TSeqNos({2}), seqNosToSync(response));
        }

        {
            TShardSessions shardSessions;
            *shardSessions.AddSessions() =
                makeShardSession("session", 2, 2, {2});
            auto response =
                tablet.BuildShardSessionsDiff("shard", shardSessions);
            UNIT_ASSERT_VALUES_EQUAL(0, response->SessionsToSync.size());
        }

        // the sessions which are unknown to the main tablet should be
        // destroyed: both the ones which the shard has (they are described
        // as the sessions without subsessions and seqnos, so DestroySession
        // with a zero seqno destroys them) and the ones which it is
        // requesting right now
        {
            TShardSessions shardSessions;
            *shardSessions.AddSessions() =
                makeShardSession("session", 2, 2, {2});
            *shardSessions.AddSessions() =
                makeShardSession("stale_session", 7, 5, {5, 7});
            shardSessions.AddRecoveringSessionIds("session");
            shardSessions.AddRecoveringSessionIds("stale_session");
            shardSessions.AddRecoveringSessionIds("destroyed_session");

            auto response =
                tablet.BuildShardSessionsDiff("shard", shardSessions);
            UNIT_ASSERT_VALUES_EQUAL(0, response->SessionsToSync.size());
            UNIT_ASSERT_VALUES_EQUAL(2, response->SessionsToDestroy.size());

            const auto& staleSession = response->SessionsToDestroy[0];
            UNIT_ASSERT_VALUES_EQUAL(
                "stale_session",
                staleSession.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(0, staleSession.GetSessionSeqNo());
            UNIT_ASSERT(staleSession.HasSession());
            UNIT_ASSERT_VALUES_EQUAL(
                "stale_session",
                staleSession.GetSession().GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(
                "client",
                staleSession.GetSession().GetClientId());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                staleSession.GetSession().GetMaxSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                staleSession.GetSession().GetMaxRwSeqNo());
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                staleSession.GetSession().SubSessionsSize());

            const auto& destroyedSession = response->SessionsToDestroy[1];
            UNIT_ASSERT_VALUES_EQUAL(
                "destroyed_session",
                destroyedSession.GetSessionId());
            UNIT_ASSERT_VALUES_EQUAL(0, destroyedSession.GetSessionSeqNo());
            UNIT_ASSERT(!destroyedSession.HasSession());
        }
    }
}

}   // namespace NCloud::NFileStore::NStorage
