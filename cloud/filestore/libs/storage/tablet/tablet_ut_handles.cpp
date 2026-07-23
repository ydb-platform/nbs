#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Handles)
{
    Y_UNIT_TEST(ShouldSetGuestKeepCacheProperly)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetGuestKeepCacheAllowed(true);
        TTestEnv env({}, storageConfig);
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        UNIT_ASSERT(!tablet.CreateHandle(id, TCreateHandleArgs::RDNLY)
                         ->Record.GetGuestKeepCache());
        // Second CreateHandle call within this session should be allowed to
        // keep cache
        UNIT_ASSERT(tablet.CreateHandle(id, TCreateHandleArgs::RDNLY)
                        ->Record.GetGuestKeepCache());
        // But not if this request is not read-only
        UNIT_ASSERT(!tablet.CreateHandle(id, TCreateHandleArgs::RDWR)
                         ->Record.GetGuestKeepCache());
        UNIT_ASSERT(!tablet.CreateHandle(id, TCreateHandleArgs::WRNLY)
                         ->Record.GetGuestKeepCache());

        // GuestKeepCache should not be set if there is already a write handle
        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        UNIT_ASSERT(!tablet.CreateHandle(id, TCreateHandleArgs::WRNLY)
                         ->Record.GetGuestKeepCache());
        UNIT_ASSERT(!tablet.CreateHandle(id, TCreateHandleArgs::RDNLY)
                         ->Record.GetGuestKeepCache());
        UNIT_ASSERT(!tablet.CreateHandle(id, TCreateHandleArgs::RDNLY)
                         ->Record.GetGuestKeepCache());
        UNIT_ASSERT(!tablet.CreateHandle(id, TCreateHandleArgs::RDWR)
                         ->Record.GetGuestKeepCache());

        // But when the write handle is closed the keep cache should be set
        id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test3"));
        auto writeHandle =
            CreateHandle(tablet, id, {}, TCreateHandleArgs::WRNLY);
        UNIT_ASSERT(!tablet.CreateHandle(id, TCreateHandleArgs::RDNLY)
                         ->Record.GetGuestKeepCache());
        tablet.DestroyHandle(writeHandle);
        UNIT_ASSERT(tablet.CreateHandle(id, TCreateHandleArgs::RDNLY)
                        ->Record.GetGuestKeepCache());

        tablet.SendRequest(tablet.CreateUpdateCounters());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        TTestRegistryVisitor visitor;
        registry->Visit(TInstant::Zero(), visitor);
        visitor.ValidateExpectedCounters({
            {{{"filesystem", "test"},
              {"sensor", "GuestKeepCacheSet"},
              {"request", "CreateHandle"}},
             2},
        });
    }

    Y_UNIT_TEST(ShouldSetGuestKeepCacheBasedOnMtime)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetGuestKeepCacheAllowed(true);
        TTestEnv env({}, storageConfig);
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.CreateHandle(id, TCreateHandleArgs::RDNLY);

        {
            // If the file was not changed since the last time we opened it, we
            // can keep the cache
            auto createHandleResponse =
                tablet.CreateHandle(id, TCreateHandleArgs::RDNLY);
            UNIT_ASSERT(createHandleResponse->Record.GetGuestKeepCache());
            tablet.DestroyHandle(createHandleResponse->Record.GetHandle());
        }
        {
            // Otherwise we should not keep the cache

            // Emulate file modification by changing its mtime
            auto attrs = GetNodeAttrs(tablet, id);
            tablet.SetNodeAttr(
                TSetNodeAttrArgs(id).SetMTime(attrs.GetMTime() + 1));

            auto createHandleResponse =
                tablet.CreateHandle(id, TCreateHandleArgs::RDNLY);
            UNIT_ASSERT(!createHandleResponse->Record.GetGuestKeepCache());
        }
    }

    Y_UNIT_TEST(ShouldSetHandleCreatedAsyncForEligibleCreateHandle)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        {
            // Read-only open with client opt-in may be acknowledged before the
            // handle is durably persisted.
            auto request =
                tablet.CreateCreateHandleRequest(id, TCreateHandleArgs::RDNLY);
            request->Record.SetAllowAsyncCreateHandle(true);
            tablet.SendRequest(std::move(request));

            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());
            UNIT_ASSERT(response->Record.GetHandleCreatedAsync());
        }

        {
            // Without client opt-in, the tablet must use the synchronous path
            // and must not mark the handle as created asynchronously.
            auto request =
                tablet.CreateCreateHandleRequest(id, TCreateHandleArgs::RDNLY);
            tablet.SendRequest(std::move(request));

            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());
            UNIT_ASSERT(!response->Record.GetHandleCreatedAsync());
        }

        {
            // Write opens are ineligible even when the client sets the async
            // opt-in bit.
            auto request =
                tablet.CreateCreateHandleRequest(id, TCreateHandleArgs::WRNLY);
            request->Record.SetAllowAsyncCreateHandle(true);
            tablet.SendRequest(std::move(request));

            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());
            UNIT_ASSERT(!response->Record.GetHandleCreatedAsync());
        }
    }

    Y_UNIT_TEST(ShouldCacheAsyncCreateHandleFlagForDuplicateRequests)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        constexpr ui64 requestId = 100500;

        auto createRequest = [&]
        {
            auto request = tablet.CreateCreateHandleRequest(
                id,
                TCreateHandleArgs::RDNLY);
            request->Record.SetAllowAsyncCreateHandle(true);
            request->Record.MutableHeaders()->SetRequestId(requestId);
            return request;
        };

        tablet.SendRequest(createRequest());
        auto response = tablet.RecvCreateHandleResponse();
        UNIT_ASSERT(response->Record.GetHandleCreatedAsync());
        const ui64 handle = response->Record.GetHandle();

        // A retry is served from the duplicate cache and must still require
        // confirmation of the handle.
        tablet.SendRequest(createRequest());
        response = tablet.RecvCreateHandleResponse();
        UNIT_ASSERT(response->Record.GetHandleCreatedAsync());
        UNIT_ASSERT_VALUES_EQUAL(handle, response->Record.GetHandle());

        tablet.ConfirmCreateHandle(
            id,
            handle,
            TCreateHandleArgs::RDNLY,
            requestId);
        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldIgnoreAsyncCreateHandleOptInIfFeatureDisabled)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(false);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        // Hold the create tx commit so the async response is acknowledged
        // before the SessionHandles row becomes durable.
        TAutoPtr<IEventHandle> putEvent;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& ev)
            {
                Y_UNUSED(runtime);
                if (!putEvent &&
                    ev->GetTypeRewrite() == TEvBlobStorage::EvPut)
                {
                    putEvent = std::move(ev);
                    return true;
                }

                return false;
            });

        auto request =
            tablet.CreateCreateHandleRequest(id, TCreateHandleArgs::RDNLY);
        request->Record.SetAllowAsyncCreateHandle(true);
        tablet.SendRequest(std::move(request));

        env.GetRuntime().DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return putEvent != nullptr;
            }});

        tablet.AssertCreateHandleNoResponse();

        env.GetRuntime().SetEventFilter(
            TTestActorRuntimeBase::DefaultFilterFunc);
        env.GetRuntime().Send(putEvent.Release(), nodeIdx);

        auto response = tablet.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());
        UNIT_ASSERT(!response->Record.GetHandleCreatedAsync());
        UNIT_ASSERT(response->Record.GetHandle());
    }

    Y_UNIT_TEST(ShouldNotMarkShardRedirectCreateHandleAsAsync)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        const TString shardId = "shard";
        const TString name = "test";
        const TString shardNodeName = CreateGuidAsString();
        // Model an existing namespace entry that lives in a shard. Opening it
        // on the main tablet should return a redirect, not a local handle.
        CreateExternalRef(tablet, RootNodeId, name, shardId, shardNodeName);

        // Even with async opt-in, a redirect response has no handle to confirm.
        auto request = tablet.CreateCreateHandleRequest(
            RootNodeId,
            name,
            TCreateHandleArgs::RDNLY);
        request->Record.SetAllowAsyncCreateHandle(true);
        tablet.SendRequest(std::move(request));

        auto response = tablet.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());

        // HandleCreatedAsync is meaningful only for a real local handle.
        UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetHandle());
        UNIT_ASSERT(!response->Record.GetHandleCreatedAsync());
        UNIT_ASSERT_VALUES_EQUAL(
            shardId,
            response->Record.GetShardFileSystemId());
        UNIT_ASSERT_VALUES_EQUAL(
            shardNodeName,
            response->Record.GetShardNodeName());
    }

    Y_UNIT_TEST(ShouldRecreateExactHandleOnConfirmCreateHandle)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const ui64 handle = 424242;

        tablet.ConfirmCreateHandle(
            id,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);

        tablet.DescribeData(handle, 0, 1_KB);
        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldConfirmCreateHandleIdempotently)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const ui64 handle = 424243;

        tablet.ConfirmCreateHandle(
            id,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);
        tablet.ConfirmCreateHandle(
            id,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);

        tablet.DescribeData(handle, 0, 1_KB);
        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldConfirmAsyncCreateHandleAfterTabletRestart)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        // Hold the create tx commit so the async response is acknowledged
        // before the SessionHandles row becomes durable.
        TAutoPtr<IEventHandle> putEvent;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& ev)
            {
                Y_UNUSED(runtime);
                if (!putEvent &&
                    ev->GetTypeRewrite() == TEvBlobStorage::EvPut)
                {
                    putEvent = std::move(ev);
                    return true;
                }

                return false;
            });

        auto request =
            tablet.CreateCreateHandleRequest(id, TCreateHandleArgs::RDNLY);
        request->Record.SetAllowAsyncCreateHandle(true);
        request->Record.MutableHeaders()->SetRequestId(100500);
        tablet.SendRequest(std::move(request));

        env.GetRuntime().DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return putEvent != nullptr;
            }});

        auto response = tablet.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());
        UNIT_ASSERT(response->Record.GetHandleCreatedAsync());

        const ui64 handle = response->Record.GetHandle();
        UNIT_ASSERT(handle);

        // Drop the uncommitted tx by rebooting the tablet before releasing the
        // captured TEvPut.
        env.GetRuntime().SetEventFilter(
            TTestActorRuntimeBase::DefaultFilterFunc);
        tablet.RebootTablet();
        tablet.RecoverSession();

        // An unlink during the recovery window must only remove the name.
        // The node is retained until the pending create is confirmed.
        tablet.UnlinkNode(RootNodeId, "test", false);

        // The early-returned handle is not persisted yet, so recovery must
        // explicitly confirm that exact handle id.
        auto describeResponse = tablet.AssertDescribeDataFailed(
            handle,
            0,
            1_KB);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FS_BADHANDLE,
            describeResponse->Record.GetError().GetCode(),
            describeResponse->Record.GetError().GetMessage());

        tablet.ConfirmCreateHandle(
            id,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);

        // ConfirmCreateHandle recreates and persists the exact returned handle.
        tablet.DescribeData(handle, 0, 1_KB);

        // Cleanup must retain a node owned by the recovered handle.
        env.GetRuntime().AdvanceCurrentTime(TDuration::Minutes(1));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        tablet.GetNodeAttr(id);

        tablet.DestroyHandle(handle);
        tablet.AssertGetNodeAttrFailed(id);
    }

    Y_UNIT_TEST(ShouldCleanupUnlinkedNodeAfterAsyncCreateRecoveryWindow)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        storageConfig.SetAsyncCreateHandleRecoveryWindow(60'000);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto node =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.UnlinkNode(RootNodeId, "test", false);

        // Restarting before the first deadline must preserve the deferred row
        // and start a new recovery window.
        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(59));
        tablet.RebootTablet();
        tablet.RecoverSession();

        // The original deadline has passed, but the restarted tablet's window
        // has not.
        env.GetRuntime().AdvanceCurrentTime(TDuration::Seconds(2));
        tablet.GetNodeAttr(node);

        env.GetRuntime().AdvanceCurrentTime(TDuration::Minutes(1));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        tablet.AssertGetNodeAttrFailed(node);
    }

    Y_UNIT_TEST(ShouldDrainDeferredZeroLinkNodesWhenAsyncCreateHandleDisabled)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto node =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.UnlinkNode(RootNodeId, "test", false);

        NProto::TStorageConfig patch;
        patch.SetAsyncCreateHandleEnabled(false);
        tablet.ChangeStorageConfig(std::move(patch));
        tablet.RebootTablet();
        tablet.RecoverSession();

        // Disabled async create drains recovery entries without its recovery
        // window.
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        tablet.AssertGetNodeAttrFailed(node);
    }

    Y_UNIT_TEST(ShouldRetainNodeWhenDeferredZeroLinkCleanupFails)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        storageConfig.SetLargeDeletionMarkersEnabled(true);
        storageConfig.SetLargeDeletionMarkersThreshold(1);
        storageConfig.SetLargeDeletionMarkersThresholdForBackpressure(0);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        const auto node =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        TSetNodeAttrArgs attrs(node);
        attrs.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);
        attrs.SetSize(4_KB);
        tablet.SetNodeAttr(attrs);
        tablet.UnlinkNode(RootNodeId, "test", false);

        // The zero backpressure threshold makes RemoveNode fail during cleanup.
        env.GetRuntime().AdvanceCurrentTime(TDuration::Minutes(1));
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        tablet.GetNodeAttr(node);

        // A successful later cleanup would remove a still-deferred node. The
        // retained node proves the failed cleanup moved it to OrphanNodes.
        storageConfig.SetAsyncCreateHandleEnabled(false);
        storageConfig.SetLargeDeletionMarkersThresholdForBackpressure(1);
        tablet.ChangeStorageConfig(storageConfig);
        tablet.RebootTablet();
        tablet.RecoverSession();
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));
        tablet.GetNodeAttr(node);
    }

    Y_UNIT_TEST(ShouldRestoreCreateHandleDupCacheAfterPostRestartConfirm)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto node =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        constexpr ui64 requestId = 100500;

        auto createRequest = [&]
        {
            auto request = tablet.CreateCreateHandleRequest(
                node,
                TCreateHandleArgs::RDNLY);
            request->Record.SetAllowAsyncCreateHandle(true);
            request->Record.MutableHeaders()->SetRequestId(requestId);
            return request;
        };

        // Hold the create tx commit so both the handle and its dup-cache entry
        // are lost when the tablet restarts.
        TAutoPtr<IEventHandle> putEvent;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& ev)
            {
                Y_UNUSED(runtime);
                if (!putEvent &&
                    ev->GetTypeRewrite() == TEvBlobStorage::EvPut)
                {
                    putEvent = std::move(ev);
                    return true;
                }

                return false;
            });

        tablet.SendRequest(createRequest());

        env.GetRuntime().DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return putEvent != nullptr;
            }});

        auto response = tablet.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());
        UNIT_ASSERT(response->Record.GetHandleCreatedAsync());

        const ui64 confirmedHandle = response->Record.GetHandle();
        UNIT_ASSERT(confirmedHandle);

        env.GetRuntime().SetEventFilter(
            TTestActorRuntimeBase::DefaultFilterFunc);
        tablet.RebootTablet();
        tablet.RecoverSession();

        tablet.ConfirmCreateHandle(
            node,
            confirmedHandle,
            TCreateHandleArgs::RDNLY,
            requestId);
        tablet.DescribeData(confirmedHandle, 0, 1_KB);

        // ConfirmCreateHandle restores the lost CreateHandle dup-cache entry.
        // Retrying the original CreateHandle request id returns the same
        // confirmed handle instead of opening another one.
        tablet.SendRequest(createRequest());
        response = tablet.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());
        UNIT_ASSERT(!response->Record.GetHandleCreatedAsync());

        const ui64 rerunHandle = response->Record.GetHandle();
        UNIT_ASSERT_VALUES_EQUAL(confirmedHandle, rerunHandle);

        tablet.DestroyHandle(confirmedHandle);
    }

    Y_UNIT_TEST(ShouldDelayAsyncCreateHandleConfirmUntilCreateCommit)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetAsyncCreateHandleEnabled(true);
        TTestEnv env({}, storageConfig);

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        // Hold the original create tx commit. ConfirmCreateHandle may execute
        // while the handle exists only in tablet memory, but it must not reply.
        TAutoPtr<IEventHandle> putEvent;
        env.GetRuntime().SetEventFilter(
            [&](auto& runtime, auto& ev)
            {
                Y_UNUSED(runtime);
                if (!putEvent &&
                    ev->GetTypeRewrite() == TEvBlobStorage::EvPut)
                {
                    putEvent = std::move(ev);
                    return true;
                }

                return false;
            });

        auto request =
            tablet.CreateCreateHandleRequest(id, TCreateHandleArgs::RDNLY);
        request->Record.SetAllowAsyncCreateHandle(true);
        request->Record.MutableHeaders()->SetRequestId(100500);
        tablet.SendRequest(std::move(request));

        env.GetRuntime().DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return putEvent != nullptr;
            }});

        auto response = tablet.RecvCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());
        UNIT_ASSERT(response->Record.GetHandleCreatedAsync());

        const ui64 handle = response->Record.GetHandle();
        UNIT_ASSERT(handle);

        auto confirmRequest = tablet.CreateConfirmCreateHandleRequest(
            id,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);
        tablet.SendRequest(std::move(confirmRequest));

        // Confirm must not return success while the original async create tx is
        // still uncommitted. Otherwise vhost could pop the durable queue entry
        // before the handle is actually reloadable.
        tablet.AssertConfirmCreateHandleNoResponse();

        env.GetRuntime().SetEventFilter(
            TTestActorRuntimeBase::DefaultFilterFunc);
        env.GetRuntime().Send(putEvent.Release(), nodeIdx);

        auto confirmResponse = tablet.RecvConfirmCreateHandleResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            confirmResponse->Record.GetError().GetCode(),
            confirmResponse->Record.GetError().GetMessage());

        // Once confirm has replied, the preceding create commit is durable
        // enough for the handle to be reloaded.
        tablet.RebootTablet();
        tablet.RecoverSession();

        tablet.DescribeData(handle, 0, 1_KB);
        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldLoadConfirmedCreateHandleAfterTabletRestart)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        const ui64 handle = 424244;

        tablet.ConfirmCreateHandle(
            id,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);

        // A confirmed handle must be present after restart, proving that the
        // SessionHandles row was committed and reloaded.
        tablet.RebootTablet();
        tablet.RecoverSession();

        tablet.DescribeData(handle, 0, 1_KB);
        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldRejectConfirmCreateHandleOnCollision)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto node1 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto node2 =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        auto handle = CreateHandle(tablet, node1, {}, TCreateHandleArgs::RDNLY);

        auto response = tablet.AssertConfirmCreateHandleFailed(
            node2,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());

        tablet.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldRejectConfirmCreateHandleForAnotherSessionHandle)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet1(env.GetRuntime(), nodeIdx, tabletId);
        tablet1.InitSession("client1", "session1");

        TIndexTabletClient tablet2(env.GetRuntime(), nodeIdx, tabletId);
        tablet2.InitSession("client2", "session2");

        auto node =
            CreateNode(tablet1, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle =
            CreateHandle(tablet1, node, {}, TCreateHandleArgs::RDNLY);

        auto response = tablet2.AssertConfirmCreateHandleFailed(
            node,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_INVALID_STATE,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());

        tablet1.DestroyHandle(handle);
    }

    Y_UNIT_TEST(ShouldRejectConfirmCreateHandleForUnlinkedNode)
    {
        TTestEnv env;

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto node =
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.UnlinkNode(RootNodeId, "test", false);
        tablet.AssertGetNodeAttrFailed(node);

        const ui64 handle = 424245;
        auto response = tablet.AssertConfirmCreateHandleFailed(
            node,
            handle,
            TCreateHandleArgs::RDNLY,
            100500);
        UNIT_ASSERT_VALUES_EQUAL_C(
            E_FS_NOENT,
            response->Record.GetError().GetCode(),
            response->Record.GetError().GetMessage());
    }

    Y_UNIT_TEST(ShouldSetGuestKeepCacheProperlyForOffloadedNodes)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetGuestKeepCacheAllowed(true);
        storageConfig.SetSessionHandleOffloadedStatsCapacity(2);
        storageConfig.SetGuestCachingType(NProto::GCT_ANY_READ);
        TTestEnv env({}, storageConfig);
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.AddDynamicNode();
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

#define CHECK_HANDLE_STATS(maxSize, sumSize, maxTotalSize, sumTotalSize,       \
                           keepCacheSet)                                       \
    {                                                                          \
        tablet.SendRequest(tablet.CreateUpdateCounters());                     \
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));            \
        TTestRegistryVisitor visitor;                                          \
        registry->Visit(TInstant::Zero(), visitor);                            \
        visitor.ValidateExpectedCounters({                                     \
            {{{"filesystem", "test"},                                          \
              {"sensor", "HandleStatsByNodeMaxSize"}},                         \
             maxSize},                                                         \
            {{{"filesystem", "test"},                                          \
              {"sensor", "HandleStatsByNodeSumSize"}},                         \
             sumSize},                                                         \
            {{{"filesystem", "test"},                                          \
              {"sensor", "HandleStatsByNodeMaxTotalSize"}},                    \
             maxTotalSize},                                                    \
            {{{"filesystem", "test"},                                          \
              {"sensor", "HandleStatsByNodeSumTotalSize"}},                    \
             sumTotalSize},                                                    \
            {{{"filesystem", "test"},                                          \
              {"sensor", "GuestKeepCacheSet"},                                 \
              {"request", "CreateHandle"}},                                    \
             keepCacheSet},                                                    \
        });                                                                    \
    }

        // Open a file and close it twice. The second create handle should have
        // GuestKeepCache set because its mtime has not changed since the last
        // time we opened it

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto createHandleResponse =
            tablet.CreateHandle(id, TCreateHandleArgs::RDNLY);
        UNIT_ASSERT(!createHandleResponse->Record.GetGuestKeepCache());
        // Stats={test}, Offloaded={}
        CHECK_HANDLE_STATS(1, 1, 1, 1, 0);
        tablet.DestroyHandle(createHandleResponse->Record.GetHandle());
        // Stats={}, Offloaded={test}
        CHECK_HANDLE_STATS(0, 0, 1, 1, 0);

        // Create handle again, should have GuestKeepCache set
        createHandleResponse =
            tablet.CreateHandle(id, TCreateHandleArgs::RDNLY);
        UNIT_ASSERT(createHandleResponse->Record.GetGuestKeepCache());
        tablet.DestroyHandle(createHandleResponse->Record.GetHandle());

        // Two more new nodes should evict the "test" file from the cache
        for (int i = 0; i < 2; ++i) {
            auto id = CreateNode(
                tablet,
                TCreateNodeArgs::File(RootNodeId, Sprintf("test%d", i)));
            createHandleResponse =
                tablet.CreateHandle(id, TCreateHandleArgs::RDNLY);
            UNIT_ASSERT(!createHandleResponse->Record.GetGuestKeepCache());
            tablet.DestroyHandle(createHandleResponse->Record.GetHandle());
        }
        // Stats={}, Offloaded={test0, test1} ("test" was evicted by LRU)
        CHECK_HANDLE_STATS(0, 0, 2, 2, 1);

        // Create handle for the first file again, will not have the
        // GuestKeepCache set because this node was evicted from the cache
        createHandleResponse =
            tablet.CreateHandle(id, TCreateHandleArgs::RDNLY);
        UNIT_ASSERT(!createHandleResponse->Record.GetGuestKeepCache());
        // Stats={test}, Offloaded={test0, test1}
        CHECK_HANDLE_STATS(1, 1, 3, 3, 1);

        tablet.DestroyHandle(createHandleResponse->Record.GetHandle());

#undef CHECK_HANDLE_STATS
    }

    Y_UNIT_TEST(ShouldHandleCommitIdOverflowInCreateDestroyHandle)
    {
        const ui32 block = 4_KB;
        const ui32 maxTabletStep = 5;

        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxTabletStep(maxTabletStep);

        TTestEnv env({}, std::move(storageConfig));

        ui32 nodeIdx = env.AddDynamicNode();

        TTabletRebootTracker rebootTracker;
        env.GetRuntime().SetEventFilter(rebootTracker.GetEventFilter());

        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto reconnectIfNeeded = [&]()
        {
            if (rebootTracker.IsPipeDestroyed()) {
                tablet.ReconnectPipe();
                tablet.WaitReady();
                tablet.RecoverSession();
                rebootTracker.ClearPipeDestroyed();
            }
        };

        TVector<ui64> successfulHandles;
        const size_t targetSuccessfulHandles = 4;

        while (successfulHandles.size() < targetSuccessfulHandles) {
            TString fileName = TStringBuilder()
                               << "file_" << successfulHandles.size();

            tablet.SendCreateHandleRequest(
                RootNodeId,
                fileName,
                TCreateHandleArgs::CREATE);
            auto handleResponse = tablet.RecvCreateHandleResponse();
            reconnectIfNeeded();

            if (FAILED(handleResponse->GetStatus())) {
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    handleResponse->GetError().GetCode());
                continue;
            }

            ui64 handle = handleResponse->Record.GetHandle();

            tablet.WriteData(handle, 0, block, 'a');

            successfulHandles.push_back(handle);
        }

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 2,
            "Expected at least 2 different generations due to tablet reboot, "
            "got "
                << rebootTracker.GetGenerationCount());
        UNIT_ASSERT_VALUES_EQUAL(
            successfulHandles.size(),
            targetSuccessfulHandles);

        for (size_t i = 0; i < successfulHandles.size();) {
            tablet.SendDestroyHandleRequest(successfulHandles[i]);
            auto destroyResponse = tablet.RecvDestroyHandleResponse();
            reconnectIfNeeded();

            if (FAILED(destroyResponse->GetStatus())) {
                UNIT_ASSERT_VALUES_EQUAL(
                    E_REJECTED,
                    destroyResponse->GetError().GetCode());
                continue;
            }

            tablet.SendReadDataRequest(successfulHandles[i], 0, block);
            auto readResponse = tablet.RecvReadDataResponse();
            reconnectIfNeeded();

            UNIT_ASSERT_VALUES_UNEQUAL(S_OK, readResponse->GetStatus());

            ++i;
        }

        UNIT_ASSERT_C(
            rebootTracker.GetGenerationCount() >= 3,
            "Expected at least 3 different generations due to tablet reboot, "
            "got "
                << rebootTracker.GetGenerationCount());
    }
}

}   // namespace NCloud::NFileStore::NStorage
