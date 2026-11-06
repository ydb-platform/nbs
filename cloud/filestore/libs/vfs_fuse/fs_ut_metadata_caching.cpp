#include "fs_ut_bootstrap.h"
#include "node_attrs_cache.h"
#include "node_cache.h"
#include "node_ref_cache.h"

#include <cloud/filestore/libs/service/error.h>
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
    std::atomic<ui32> XAttrCount = 0;
    std::atomic<ui32> SetXAttrCount = 0;
    std::atomic<ui32> XAttrCacheTTL = CacheTTLMs;
    std::atomic<bool> XAttrExists = true;

    TMetadataCachingEnv()
        : Bootstrap(
              Timer,
              CreateScheduler(),
              {} /* featuresConfig */,
              1000 /* handleOpsQueueStateFileSize */,
              1000 /* writeBackCacheAutomaticFlushPeriodMs */,
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
        Bootstrap.Service->SetHandlerGetNodeXAttr(
            [this] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                Y_UNUSED(request);
                return MakeFuture(HandleGetNodeXAttr());
            });
        Bootstrap.Service->SetHandlerSetNodeXAttr(
            [this] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                Y_UNUSED(request);
                NProto::TSetNodeXAttrResponse response;
                response.SetVersion(++SetXAttrCount);
                response.SetXAttrCacheTTL(XAttrCacheTTL);
                return MakeFuture(response);
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

    NProto::TGetNodeXAttrResponse HandleGetNodeXAttr()
    {
        const ui32 count = ++XAttrCount;
        NProto::TGetNodeXAttrResponse response;
        response.SetXAttrCacheTTL(XAttrCacheTTL);
        if (!XAttrExists) {
            *response.MutableError() = ErrorAttributeDoesNotExist("user.a");
            return response;
        }

        response.SetValue(ToString(count));
        response.SetVersion(count);
        return response;
    }

    TString GetXAttr()
    {
        auto getXAttr = Bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>(
            "user.a",
            FileNodeId);
        return getXAttr.GetValue(WaitTimeout);
    }

    void SetXAttr(const TString& value)
    {
        auto setXAttr = Bootstrap.Fuse->SendRequest<TSetXAttrValueRequest>(
            "user.a",
            value,
            FileNodeId);
        UNIT_ASSERT_NO_EXCEPTION(setXAttr.GetValue(WaitTimeout));
    }

    void AssertXAttrAbsent()
    {
        auto getXAttr = Bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>(
            "user.a",
            FileNodeId);
        UNIT_ASSERT_EXCEPTION_SATISFIES(
            getXAttr.GetValue(WaitTimeout),
            TSystemError,
            [] (const TSystemError& e) { return e.Status() == ENODATA; });
    }

    void SendXAttrEvent()
    {
        NProto::TFileSystemEvent event;
        auto* invalidate = event.MutableInvalidateNodeXAttr();
        invalidate->SetNodeId(FileNodeId);
        invalidate->SetName("user.a");
        SendEvent(event);
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

        //
        // No NodeCacheTTL - the node attrs are not cached, so a cached NodeRef
        // alone is not enough to reply to Lookup.
        //

        env.NodeCacheTTL = 0;

        env.Lookup("f");
        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(2, env.LookupCount.load());

        env.GetAttr(FileNodeId);
        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetAttrCount.load());

        //
        // No NodeRefCacheTTL - the NodeRef is not cached, so the cached node
        // attrs alone are not enough to reply to Lookup. They are still used
        // for GetAttr.
        //

        env.Timer->AdvanceTime(CacheTTL);
        env.NodeCacheTTL = CacheTTLMs;
        env.NodeRefCacheTTL = 0;
        env.Lookup("f");
        env.Lookup("f");
        UNIT_ASSERT_VALUES_EQUAL(4, env.LookupCount.load());

        env.GetAttr(FileNodeId);
        UNIT_ASSERT_VALUES_EQUAL(2, env.GetAttrCount.load());
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

    Y_UNIT_TEST(ShouldInvalidateXAttrCacheUponEvents)
    {
        TMetadataCachingEnv env;

        UNIT_ASSERT_VALUES_EQUAL("1", env.GetXAttr());
        UNIT_ASSERT_VALUES_EQUAL("1", env.GetXAttr());
        UNIT_ASSERT_VALUES_EQUAL(1, env.XAttrCount.load());

        env.SendXAttrEvent();
        UNIT_ASSERT_VALUES_EQUAL("2", env.GetXAttr());
        UNIT_ASSERT_VALUES_EQUAL(2, env.XAttrCount.load());

        //
        // The cached absence is invalidated as well.
        //

        env.XAttrExists = false;
        env.SendXAttrEvent();
        env.AssertXAttrAbsent();
        env.AssertXAttrAbsent();
        UNIT_ASSERT_VALUES_EQUAL(3, env.XAttrCount.load());

        env.XAttrExists = true;
        env.SendXAttrEvent();
        UNIT_ASSERT_VALUES_EQUAL("4", env.GetXAttr());

        //
        // A disconnect invalidates everything.
        //

        env.EventHandler->OnDisconnect(1 /* tabletId */);
        UNIT_ASSERT_VALUES_EQUAL("5", env.GetXAttr());
        UNIT_ASSERT_VALUES_EQUAL(5, env.XAttrCount.load());
    }

    Y_UNIT_TEST(ShouldInvalidateXAttrsCachedUponSetXAttr)
    {
        TMetadataCachingEnv env;

        env.SetXAttr("x");
        UNIT_ASSERT_VALUES_EQUAL("x", env.GetXAttr());
        UNIT_ASSERT_VALUES_EQUAL(0, env.XAttrCount.load());

        env.SendXAttrEvent();
        UNIT_ASSERT_VALUES_EQUAL("1", env.GetXAttr());
        UNIT_ASSERT_VALUES_EQUAL(1, env.XAttrCount.load());
    }

    Y_UNIT_TEST(ShouldCacheXAttrsForXAttrCacheTTLOrXAttrCacheTimeout)
    {
        TMetadataCachingEnv env;

        //
        // A nonzero XAttrCacheTTL is used instead of XAttrCacheTimeout (15s
        // by default).
        //

        UNIT_ASSERT_VALUES_EQUAL("1", env.GetXAttr());
        env.Timer->AdvanceTime(TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL("1", env.GetXAttr());
        env.Timer->AdvanceTime(CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL("2", env.GetXAttr());

        //
        // Zero XAttrCacheTTL - the backend doesn't support FileSystemEvents,
        // XAttrCacheTimeout is used.
        //

        env.XAttrCacheTTL = 0;
        env.Timer->AdvanceTime(CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL("3", env.GetXAttr());
        UNIT_ASSERT_VALUES_EQUAL("3", env.GetXAttr());
        env.Timer->AdvanceTime(TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL("4", env.GetXAttr());
    }

    Y_UNIT_TEST(ShouldNotCacheXAttrsRacingWithInvalidations)
    {
        TMetadataCachingEnv env;

        auto promise = NewPromise<NProto::TGetNodeXAttrResponse>();
        env.Bootstrap.Service->SetHandlerGetNodeXAttr(
            [&] (auto callContext, auto request)
            {
                Y_UNUSED(callContext);
                Y_UNUSED(request);
                auto response = env.HandleGetNodeXAttr();
                if (env.XAttrCount == 1) {
                    return promise.GetFuture().Apply(
                        [response] (const auto&) { return response; });
                }
                return MakeFuture(response);
            });

        auto getXAttr = env.Bootstrap.Fuse->SendRequest<TGetXAttrValueRequest>(
            "user.a",
            FileNodeId);
        UNIT_ASSERT(WaitForCondition(
            WaitTimeout,
            [&] { return env.XAttrCount == 1; }));

        env.SendXAttrEvent();
        promise.SetValue({});
        UNIT_ASSERT_VALUES_EQUAL("1", getXAttr.GetValue(WaitTimeout));

        UNIT_ASSERT_VALUES_EQUAL("2", env.GetXAttr());
        UNIT_ASSERT_VALUES_EQUAL(2, env.XAttrCount.load());
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

Y_UNIT_TEST_SUITE(TXAttrCacheInvalidationTest)
{
    Y_UNIT_TEST(ShouldExpireXAttrsUponCacheTTLOrTimeout)
    {
        auto timer = std::make_shared<TTestTimer>();
        TXAttrCache cache(timer, 10 /* maxSize */, TDuration::Seconds(15));

        cache.Add(
            1,
            "a",
            "v1",
            1 /* version */,
            1 /* cacheVersion */,
            TDuration::Minutes(1) /* cacheTTL */);
        cache.Add(
            1,
            "b",
            "v2",
            1 /* version */,
            1 /* cacheVersion */,
            {} /* cacheTTL */);

        timer->AdvanceTime(TDuration::Seconds(15));
        UNIT_ASSERT(cache.Get(1, "a"));
        UNIT_ASSERT(!cache.Get(1, "b"));

        timer->AdvanceTime(TDuration::Seconds(45));
        UNIT_ASSERT(!cache.Get(1, "a"));
    }

    Y_UNIT_TEST(ShouldInvalidateAllXAttrsOfNode)
    {
        TXAttrCache cache(
            std::make_shared<TTestTimer>(),
            10 /* maxSize */,
            TDuration::Seconds(15));

        cache.Add(
            1,
            "a",
            "v1",
            1 /* version */,
            1 /* cacheVersion */,
            {} /* cacheTTL */);
        cache.AddAbsent(1, "b", 1 /* cacheVersion */, {} /* cacheTTL */);
        cache.Add(
            2,
            "a",
            "v2",
            1 /* version */,
            1 /* cacheVersion */,
            {} /* cacheTTL */);

        //
        // All the xattrs of the node are dropped, the other nodes are not
        // affected.
        //

        cache.Invalidate(1 /* ino */, 5 /* version */);
        UNIT_ASSERT(!cache.Get(1, "a"));
        UNIT_ASSERT(!cache.Get(1, "b"));
        UNIT_ASSERT(cache.Get(2, "a"));

        //
        // The values fetched by the requests started before the invalidation
        // are not cached.
        //

        cache.Add(
            1,
            "a",
            "v3",
            2 /* version */,
            4 /* cacheVersion */,
            {} /* cacheTTL */);
        UNIT_ASSERT(!cache.Get(1, "a"));
        cache.AddAbsent(1, "b", 4 /* cacheVersion */, {} /* cacheTTL */);
        UNIT_ASSERT(!cache.Get(1, "b"));

        cache.Add(
            1,
            "a",
            "v4",
            3 /* version */,
            5 /* cacheVersion */,
            {} /* cacheTTL */);
        UNIT_ASSERT_VALUES_EQUAL("v4", *cache.Get(1, "a")->Value);
    }

    Y_UNIT_TEST(ShouldNotCacheXAttrsFetchedBeforeInvalidation)
    {
        TXAttrCache cache(
            std::make_shared<TTestTimer>(),
            10 /* maxSize */,
            TDuration::Seconds(15));

        cache.Add(
            1,
            "a",
            "v1",
            1 /* version */,
            1 /* cacheVersion */,
            {} /* cacheTTL */);
        UNIT_ASSERT(cache.Get(1, "a"));

        //
        // The cached value is erased, the values fetched by the requests
        // started before the invalidation are not cached.
        //

        cache.Invalidate(1, "a", 5 /* version */);
        UNIT_ASSERT(!cache.Get(1, "a"));
        cache.Add(
            1,
            "a",
            "v2",
            2 /* version */,
            4 /* cacheVersion */,
            {} /* cacheTTL */);
        UNIT_ASSERT(!cache.Get(1, "a"));
        cache.AddAbsent(1, "a", 4 /* cacheVersion */, {} /* cacheTTL */);
        UNIT_ASSERT(!cache.Get(1, "a"));

        cache.AddAbsent(1, "a", 6 /* cacheVersion */, {} /* cacheTTL */);
        const auto* xattr = cache.Get(1, "a");
        UNIT_ASSERT(xattr && !xattr->Value);

        //
        // Other xattrs are not affected.
        //

        cache.Add(
            1,
            "b",
            "v3",
            3 /* version */,
            4 /* cacheVersion */,
            {} /* cacheTTL */);
        UNIT_ASSERT(cache.Get(1, "b"));

        cache.InvalidateAll(10 /* version */);
        UNIT_ASSERT(!cache.Get(1, "a"));
        UNIT_ASSERT(!cache.Get(1, "b"));
        cache.Add(
            1,
            "b",
            "v4",
            4 /* version */,
            9 /* cacheVersion */,
            {} /* cacheTTL */);
        UNIT_ASSERT(!cache.Get(1, "b"));
        cache.Add(
            1,
            "b",
            "v5",
            5 /* version */,
            10 /* cacheVersion */,
            {} /* cacheTTL */);
        UNIT_ASSERT_VALUES_EQUAL("v5", *cache.Get(1, "b")->Value);
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

    Y_UNIT_TEST(ShouldKeepNewerTombstonesUponInvalidateAll)
    {
        TNodeRefCache cache(10 /* maxEntryCount */, 1 /* shardCount */);
        const auto t0 = TInstant::Seconds(100);

        //
        // An invalidation with version 6 is applied before InvalidateAll with
        // version 5 - the requests started at version 5 must not cache
        // anything for the invalidated NodeRef.
        //

        UNIT_ASSERT_VALUES_EQUAL(
            InvalidNodeId,
            cache.Invalidate(1, "a", 6 /* version */));
        cache.InvalidateAll(5 /* version */);

        cache.Add(1, "a", 10, 5 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(InvalidNodeId, cache.Get(1, "a", t0));
        cache.Add(1, "a", 10, 6 /* version */, t0 + CacheTTL);
        UNIT_ASSERT_VALUES_EQUAL(10, cache.Get(1, "a", t0));
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
