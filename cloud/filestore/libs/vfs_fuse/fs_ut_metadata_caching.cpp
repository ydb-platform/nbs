#include "fs_ut_bootstrap.h"
#include "node_attrs_cache.h"
#include "node_ref_cache.h"

#include <cloud/filestore/libs/service/filesystem_event.h>
#include <cloud/filestore/libs/vhost/request.h>

#include <cloud/storage/core/libs/common/timer_test.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/system/spin_wait.h>

#include <atomic>

namespace NCloud::NFileStore::NFuse {

using namespace NThreading;
using namespace NCloud::NFileStore::NVhost;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr ui64 FileNodeId = 100;
constexpr ui32 CacheTTLMs = 60'000;
constexpr TDuration CacheTTL = TDuration::MilliSeconds(CacheTTLMs);

////////////////////////////////////////////////////////////////////////////////

template <class F>
bool WaitForCondition(TDuration timeout, F&& predicate)
{
    TSpinWait sw;
    const auto deadline = TInstant::Now() + timeout;
    while (!predicate()) {
        if (TInstant::Now() > deadline) {
            return false;
        }
        sw.Sleep();
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

struct TMetadataCachingEnv
{
    std::shared_ptr<TTestTimer> Timer = std::make_shared<TTestTimer>();
    IMultiFileSystemEventHandlerPtr EventHandler =
        CreateMultiFileSystemEventHandler();
    TBootstrap Bootstrap;

    std::atomic<ui32> LookupCount = 0;
    std::atomic<ui32> GetAttrCount = 0;
    std::atomic<ui32> NodeCacheTTL = CacheTTLMs;
    std::atomic<ui32> NodeRefCacheTTL = CacheTTLMs;

    TMetadataCachingEnv()
        : Bootstrap(
              Timer,
              CreateScheduler(),
              {} /* featuresConfig */,
              1000 /* handleOpsQueueStateFileSize */,
              1000 /* writeBackCacheAutomaticFlushPeriodMs */,
              WriteBackCacheStateFileSize,
              0 /* directoryHandlesInitialDataSize */,
              0 /* directoryHandlesMaxDataAreaStepSize */,
              CreateFileMapMemoryLimiterStub(),
              nullptr /* persistentStateManager */,
              EventHandler)
    {
        Bootstrap.Service->SetHandlerGetNodeAttr(
            [this] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                return MakeFuture(HandleGetNodeAttr(*request));
            });

        Bootstrap.Start();
    }

    ~TMetadataCachingEnv()
    {
        Bootstrap.Stop();
    }

    NProto::TGetNodeAttrResponse HandleGetNodeAttr(
        const NProto::TGetNodeAttrRequest& request)
    {
        NProto::TGetNodeAttrResponse response;
        auto* node = response.MutableNode();
        if (request.GetName()) {
            ++LookupCount;
            node->SetId(FileNodeId);
            node->SetType(NProto::E_REGULAR_NODE);
            response.SetNodeRefCacheTTL(NodeRefCacheTTL);
        } else {
            ++GetAttrCount;
            node->SetId(request.GetNodeId());
            node->SetType(
                request.GetNodeId() == RootNodeId
                    ? NProto::E_DIRECTORY_NODE
                    : NProto::E_REGULAR_NODE);
        }
        node->SetMode(0644);
        response.SetNodeCacheTTL(NodeCacheTTL);
        return response;
    }

    void Lookup(const TString& name)
    {
        auto lookup =
            Bootstrap.Fuse->SendRequest<TLookupRequest>(name, RootNodeId);
        UNIT_ASSERT_VALUES_EQUAL(FileNodeId, lookup.GetValue(WaitTimeout));
    }

    void GetAttr(ui64 nodeId)
    {
        auto getAttr = Bootstrap.Fuse->SendRequest<TGetAttrRequest>(nodeId);
        UNIT_ASSERT_NO_EXCEPTION(getAttr.GetValue(WaitTimeout));
    }

    void SendEvent(NProto::TFileSystemEvent event)
    {
        event.SetFileSystemId(FileSystemId);
        EventHandler->OnEvent(event);
    }
};

NProto::TNodeAttr MakeFileAttr(ui64 nodeId)
{
    NProto::TNodeAttr attrs;
    attrs.SetId(nodeId);
    attrs.SetType(NProto::E_REGULAR_NODE);
    return attrs;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TFileSystemMetadataCachingTest)
{
    Y_UNIT_TEST(ShouldReplyFromCacheWithinTTL)
    {
        TMetadataCachingEnv env;

        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(1, env.LookupCount.load());

        //
        // Both the NodeRef and the Node are cached - Lookup and GetAttr are
        // served from the caches.
        //

        env.Lookup("f");
        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.LookupCount.load());
        UNIT_ASSERT_VALUES_EQUAL(0, env.GetAttrCount.load());

        //
        // The expired entries are not used.
        //

        env.Timer->AdvanceTime(CacheTTL);
        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(2, env.LookupCount.load());

        env.Timer->AdvanceTime(CacheTTL);
        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetAttrCount.load());

        //
        // GetAttr responses are cached as well.
        //

        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetAttrCount.load());
    }

    Y_UNIT_TEST(ShouldNotCacheWithoutTTL)
    {
        TMetadataCachingEnv env;
        env.NodeCacheTTL = 0;

        env.Lookup("f");
        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(2, env.LookupCount.load());

        env.GetAttr(FileNodeId);
        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetAttrCount.load());

        //
        // A cached NodeRef alone is not enough to reply to Lookup.
        //

        env.Timer->AdvanceTime(CacheTTL);
        env.NodeCacheTTL = CacheTTLMs;
        env.NodeRefCacheTTL = 0;
        env.Lookup("f");
        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(4, env.LookupCount.load());
    }

    Y_UNIT_TEST(ShouldInvalidateCachesUponEvents)
    {
        TMetadataCachingEnv env;

        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(1, env.LookupCount.load());

        NProto::TFileSystemEvent event;
        auto* invalidate = event.MutableInvalidateNodeRef();
        invalidate->SetNodeId(RootNodeId);
        invalidate->SetName("f");
        env.SendEvent(event);

        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(2, env.LookupCount.load());

        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(0, env.GetAttrCount.load());

        event.Clear();
        event.MutableInvalidateNode()->SetNodeId(FileNodeId);
        env.SendEvent(event);

        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetAttrCount.load());
    }

    Y_UNIT_TEST(ShouldInvalidateCachesUponDisconnect)
    {
        TMetadataCachingEnv env;

        env.Lookup("f");
        env.EventHandler->OnDisconnect(1 /* tabletId */);

        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(2, env.LookupCount.load());

        env.EventHandler->OnDisconnect(1 /* tabletId */);
        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetAttrCount.load());
    }

    Y_UNIT_TEST(ShouldNotCacheResponsesRacingWithInvalidations)
    {
        TMetadataCachingEnv env;

        //
        // The invalidation is applied while the request is in flight - the
        // response may be stale.
        //

        auto promise = NewPromise<NProto::TGetNodeAttrResponse>();
        env.Bootstrap.Service->SetHandlerGetNodeAttr(
            [&] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                auto response = env.HandleGetNodeAttr(*request);
                if (env.LookupCount == 1) {
                    return promise.GetFuture().Apply(
                        [response] (const auto&) { return response; });
                }
                return MakeFuture(response);
            });

        auto lookup =
            env.Bootstrap.Fuse->SendRequest<TLookupRequest>("f", RootNodeId);
        UNIT_ASSERT(WaitForCondition(
            WaitTimeout,
            [&] { return env.LookupCount == 1; }));

        NProto::TFileSystemEvent event;
        auto* invalidate = event.MutableInvalidateNodeRef();
        invalidate->SetNodeId(RootNodeId);
        invalidate->SetName("f");
        env.SendEvent(event);

        event.Clear();
        event.MutableInvalidateNode()->SetNodeId(FileNodeId);
        env.SendEvent(event);

        promise.SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(FileNodeId, lookup.GetValue(WaitTimeout));

        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetAttrCount.load());
        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(2, env.LookupCount.load());
    }

    Y_UNIT_TEST(ShouldNotCacheResponsesRacingWithInvalidationAndForget)
    {
        TMetadataCachingEnv env;

        //
        // The node is cached and looked up by the guest. Then the NodeRef is
        // invalidated, so the next Lookup goes to the backend. While it is in
        // flight, the node is invalidated and forgotten by the guest - the
        // invalidation must not be lost along with the forgotten node.
        //

        env.Lookup("f");

        NProto::TFileSystemEvent event;
        auto* invalidate = event.MutableInvalidateNodeRef();
        invalidate->SetNodeId(RootNodeId);
        invalidate->SetName("f");
        env.SendEvent(event);

        auto promise = NewPromise<NProto::TGetNodeAttrResponse>();
        env.Bootstrap.Service->SetHandlerGetNodeAttr(
            [&] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                auto response = env.HandleGetNodeAttr(*request);
                if (env.LookupCount == 2) {
                    return promise.GetFuture().Apply(
                        [response] (const auto&) { return response; });
                }
                return MakeFuture(response);
            });

        auto lookup =
            env.Bootstrap.Fuse->SendRequest<TLookupRequest>("f", RootNodeId);
        UNIT_ASSERT(WaitForCondition(
            WaitTimeout,
            [&] { return env.LookupCount == 2; }));

        event.Clear();
        event.MutableInvalidateNode()->SetNodeId(FileNodeId);
        env.SendEvent(event);

        auto forget = env.Bootstrap.Fuse->SendRequest<TForgetRequest>(
            FileNodeId,
            1 /* nlookup */);
        UNIT_ASSERT_NO_EXCEPTION(forget.GetValue(WaitTimeout));

        promise.SetValue({});
        UNIT_ASSERT_VALUES_EQUAL(FileNodeId, lookup.GetValue(WaitTimeout));

        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetAttrCount.load());
    }

    Y_UNIT_TEST(ShouldKeepCachedAttrsAfterForget)
    {
        TMetadataCachingEnv env;

        //
        // The cached attrs don't depend on the guest's lookup count.
        //

        env.Lookup("f");
        auto forget = env.Bootstrap.Fuse->SendRequest<TForgetRequest>(
            FileNodeId,
            1 /* nlookup */);
        UNIT_ASSERT_NO_EXCEPTION(forget.GetValue(WaitTimeout));

        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(0, env.GetAttrCount.load());
        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(1, env.LookupCount.load());
    }

    Y_UNIT_TEST(ShouldCacheAttrsOfNodesNotLookedUp)
    {
        TMetadataCachingEnv env;

        //
        // The guest never looks the root up.
        //

        env.GetAttr(RootNodeId);
        env.GetAttr(RootNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetAttrCount.load());
    }

    Y_UNIT_TEST(ShouldInvalidateCachesUponLocalChanges)
    {
        TMetadataCachingEnv env;

        env.Bootstrap.Service->SetHandlerSetNodeAttr(
            [] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                NProto::TSetNodeAttrResponse response;
                *response.MutableNode() = MakeFileAttr(request->GetNodeId());
                return MakeFuture(response);
            });
        env.Bootstrap.Service->SetHandlerUnlinkNode(
            [] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                Y_UNUSED(request);
                return MakeFuture(NProto::TUnlinkNodeResponse());
            });
        env.Bootstrap.Service->SetHandlerCreateNode(
            [] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                Y_UNUSED(request);
                NProto::TCreateNodeResponse response;
                auto* node = response.MutableNode();
                node->SetId(FileNodeId + 1);
                node->SetType(NProto::E_DIRECTORY_NODE);
                return MakeFuture(response);
            });

        //
        // SetAttr invalidates the node.
        //

        env.Lookup("f");
        auto setAttr = env.Bootstrap.Fuse->SendRequest<TSetAttrRequest>(
            FileNodeId,
            0 /* fh */);
        UNIT_ASSERT_NO_EXCEPTION(setAttr.GetValue(WaitTimeout));
        env.GetAttr(FileNodeId);
        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(1, env.GetAttrCount.load());

        //
        // MkDir invalidates the parent (MTime/CTime/Links change).
        //

        auto mkDir =
            env.Bootstrap.Fuse->SendRequest<TMkDirRequest>("d", FileNodeId);
        UNIT_ASSERT_NO_EXCEPTION(mkDir.GetValue(WaitTimeout));
        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetAttrCount.load());

        //
        // Unlink invalidates the NodeRef and the target node (Links change).
        //

        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(1, env.LookupCount.load());

        auto unlink =
            env.Bootstrap.Fuse->SendRequest<TUnlinkRequest>("f", RootNodeId);
        UNIT_ASSERT_NO_EXCEPTION(unlink.GetValue(WaitTimeout));

        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(3, env.GetAttrCount.load());
        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(2, env.LookupCount.load());
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeAttrsCacheTest)
{
    Y_UNIT_TEST(ShouldCacheNodeAttrsUntilDeadline)
    {
        TNodeAttrsCache cache(10 /* maxEntryCount */, 1 /* shardCount */);
        const auto t0 = TInstant::Seconds(100);

        cache.Add(MakeFileAttr(1), 1 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(1, cache.Get(1, t0)->GetId());
        UNIT_ASSERT(!cache.Get(2, t0));

        UNIT_ASSERT(!cache.Get(1, t0 + CacheTTL));
        UNIT_ASSERT_VALUES_EQUAL(0, cache.GetEntryCount());
    }

    Y_UNIT_TEST(ShouldNotCacheNodeAttrsFetchedBeforeInvalidation)
    {
        TNodeAttrsCache cache(10 /* maxEntryCount */, 1 /* shardCount */);
        const auto t0 = TInstant::Seconds(100);

        //
        // The node is not cached upon invalidation.
        //

        cache.Invalidate(1, 5 /* version */);
        cache.Add(MakeFileAttr(1), 4 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(!cache.Get(1, t0));

        cache.Add(MakeFileAttr(1), 6 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(cache.Get(1, t0));

        //
        // The node is cached upon invalidation.
        //

        cache.Invalidate(1, 8 /* version */);
        UNIT_ASSERT(!cache.Get(1, t0));
        cache.Add(MakeFileAttr(1), 7 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(!cache.Get(1, t0));

        //
        // All the nodes are invalidated.
        //

        cache.Add(MakeFileAttr(1), 9 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(cache.Get(1, t0));
        cache.InvalidateAll(10 /* version */);
        UNIT_ASSERT(!cache.Get(1, t0));
        cache.Add(MakeFileAttr(2), 9 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(!cache.Get(2, t0));
    }

    Y_UNIT_TEST(ShouldRaiseWatermarkUponTombstoneEviction)
    {
        TNodeAttrsCache cache(2 /* maxEntryCount */, 1 /* shardCount */);
        const auto t0 = TInstant::Seconds(100);

        cache.Invalidate(1, 5 /* version */);
        cache.Invalidate(2, 6 /* version */);
        cache.Invalidate(3, 7 /* version */);

        cache.Add(MakeFileAttr(1), 4 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(!cache.Get(1, t0));
        cache.Add(MakeFileAttr(4), 4 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(!cache.Get(4, t0));

        cache.Add(MakeFileAttr(4), 5 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(cache.Get(4, t0));
        cache.Add(MakeFileAttr(2), 5 /* version */, t0 + CacheTTL);
        UNIT_ASSERT(!cache.Get(2, t0));
    }
}

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TNodeRefCacheTest)
{
    Y_UNIT_TEST(ShouldCacheNodeRefsUntilDeadline)
    {
        TNodeRefCache cache(10 /* maxEntryCount */, 1 /* shardCount */);
        const auto t0 = TInstant::Seconds(100);

        cache.Add(1, "a", 10, 1 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(10, cache.Get(1, "a", t0));
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "b", t0));
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(2, "a", t0));

        UNIT_ASSERT_VALUES_EQUAL(
            InvalidNodeId,
            cache.Get(1, "a", t0 + CacheTTL));
        UNIT_ASSERT_VALUES_EQUAL(0, cache.GetEntryCount());
    }

    Y_UNIT_TEST(ShouldNotCacheNodeRefsFetchedBeforeInvalidation)
    {
        TNodeRefCache cache(10 /* maxEntryCount */, 1 /* shardCount */);
        const auto t0 = TInstant::Seconds(100);

        cache.Add(1, "a", 10, 1 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(10, cache.Invalidate(1, "a", 5 /* version */));
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "a", t0));

        cache.Add(1, "a", 10, 4 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "a", t0));

        cache.Add(1, "a", 11, 6 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(11, cache.Get(1, "a", t0));

        cache.InvalidateAll(7 /* version */);
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "a", t0));
        cache.Add(1, "b", 12, 6 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "b", t0));
    }

    Y_UNIT_TEST(ShouldRaiseWatermarkUponTombstoneEviction)
    {
        TNodeRefCache cache(2 /* maxEntryCount */, 1 /* shardCount */);
        const auto t0 = TInstant::Seconds(100);

        //
        // The tombstone of "a" is evicted - all the requests started before
        // its invalidation are not cached anymore.
        //

        UNIT_ASSERT_VALUES_EQUAL(
            InvalidNodeId,
            cache.Invalidate(1, "a", 5 /* version */));
        UNIT_ASSERT_VALUES_EQUAL(
            InvalidNodeId,
            cache.Invalidate(1, "b", 6 /* version */));
        UNIT_ASSERT_VALUES_EQUAL(
            InvalidNodeId,
            cache.Invalidate(1, "c", 7 /* version */));

        cache.Add(1, "a", 10, 4 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "a", t0));
        cache.Add(1, "d", 11, 4 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "d", t0));

        cache.Add(1, "d", 11, 5 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(11, cache.Get(1, "d", t0));
        cache.Add(1, "b", 12, 5 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "b", t0));
    }

    Y_UNIT_TEST(ShouldEvictLeastRecentlyUsedNodeRefs)
    {
        TNodeRefCache cache(2 /* maxEntryCount */, 1 /* shardCount */);
        const auto t0 = TInstant::Seconds(100);

        cache.Add(1, "a", 10, 1 /* version */, t0 + CacheTTL);
        cache.Add(1, "b", 11, 1 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_UNEQUAL(InvalidNodeId, cache.Get(1, "a", t0));
        cache.Add(1, "c", 12, 1 /* version */, t0 + CacheTTL);

        UNIT_ASSERT_VALUES_EQUAL(2, cache.GetEntryCount());
        UNIT_ASSERT_VALUES_UNEQUAL(InvalidNodeId, cache.Get(1, "a", t0));
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "b", t0));
        UNIT_ASSERT_VALUES_UNEQUAL(InvalidNodeId, cache.Get(1, "c", t0));
    }
}

}   // namespace NCloud::NFileStore::NFuse
