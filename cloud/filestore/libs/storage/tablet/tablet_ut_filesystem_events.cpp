#include "tablet.h"

#include <cloud/filestore/libs/storage/api/tablet_proxy.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

namespace {

////////////////////////////////////////////////////////////////////////////////

i64 GetCounter(
    TTestEnv& env,
    TIndexTabletClient& tablet,
    const TString& sensor)
{
    //
    // Lets the tablet update its metrics.
    //

    tablet.AdvanceTime(TDuration::Seconds(15));
    env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

    TTestRegistryVisitor visitor;
    env.GetRegistry()->Visit(TInstant::Zero(), visitor);

    const TVector<TTestRegistryVisitor::TLabel> labels = {
        {"sensor", sensor},
        {"filesystem", "test"}};
    TVector<i64> values;
    for (const auto& entry: visitor.GetEntries()) {
        if (entry.Matches(labels)) {
            values.push_back(entry.Value);
        }
    }

    UNIT_ASSERT_VALUES_EQUAL_C(1, values.size(), sensor);
    return values[0];
}

struct TFileSystemEventsEnv
{
    TTestEnv Env;
    ui32 NodeIdx = 0;
    ui64 TabletId = 0;
    ui32 SentEventCount = 0;

    explicit TFileSystemEventsEnv(NProto::TStorageConfig config)
        : Env({}, std::move(config))
    {
        NodeIdx = Env.AddDynamicNode();
        TabletId = Env.BootIndexTablet(NodeIdx);

        Env.GetRuntime().SetEventFilter(
            [this] (auto& runtime, TAutoPtr<IEventHandle>& ev)
            {
                Y_UNUSED(runtime);
                if (ev->GetTypeRewrite() ==
                        TEvIndexTabletProxy::EvFileSystemEvent)
                {
                    ++SentEventCount;
                }
                return false;
            });
    }

    std::unique_ptr<TIndexTabletClient> CreateClient(
        const TString& clientId,
        const TString& sessionId)
    {
        auto client = std::make_unique<TIndexTabletClient>(
            Env.GetRuntime(),
            NodeIdx,
            TabletId);
        client->InitSession(clientId, sessionId);
        return client;
    }

    i64 GetCounter(TIndexTabletClient& tablet, const TString& sensor)
    {
        return NStorage::GetCounter(Env, tablet, sensor);
    }
};

////////////////////////////////////////////////////////////////////////////////

void CheckFileSystemEventsAreSentToSubscribersOnly(bool dryRun)
{
    NProto::TStorageConfig config;
    config.SetFileSystemEventsEnabled(true);
    config.SetFileSystemEventsDryRunModeEnabled(dryRun);
    TFileSystemEventsEnv env(std::move(config));

    //
    // Client 1 creates "f". CreateNode doesn't subscribe the creator - only
    // GetNodeAttr, GetNodeAttrBatch, ListNodes and ListNodesInternal do.
    //

    auto tablet1 = env.CreateClient("client1", "session1");
    tablet1->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"));

    //
    // Client 2 looks "f" up - subscribed to the entry and the node. Client 3
    // reads the root attrs - subscribed to the root node only.
    //

    auto tablet2 = env.CreateClient("client2", "session2");
    tablet2->GetNodeAttr(RootNodeId, "f");

    auto tablet3 = env.CreateClient("client3", "session3");
    tablet3->GetNodeAttr(RootNodeId);

    UNIT_ASSERT_VALUES_EQUAL(0, env.SentEventCount);

    //
    // UnlinkNode produces InvalidateNodeRef for the entry and InvalidateNode
    // for the removed node: both go to client 2 only. The root's MTime/CTime
    // changes produce nothing, so client 3 gets nothing.
    //

    tablet1->UnlinkNode(RootNodeId, "f", false /* unlinkDirectory */);

    UNIT_ASSERT_VALUES_EQUAL(
        1,
        env.GetCounter(*tablet1, "InvalidateNodeEventSendCount"));
    UNIT_ASSERT_VALUES_EQUAL(
        1,
        env.GetCounter(*tablet1, "InvalidateNodeRefEventSendCount"));
    UNIT_ASSERT_VALUES_EQUAL(dryRun ? 0 : 2, env.SentEventCount);

    //
    // GenerateFileSystemEvent goes through the same path: InvalidateNode for
    // the root goes to client 3 only.
    //

    NProto::TFileSystemEvent event;
    event.MutableInvalidateNode()->SetNodeId(RootNodeId);
    auto response = tablet1->GenerateFileSystemEvent(event);
    UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GetClientCount());
    UNIT_ASSERT_VALUES_EQUAL(
        2,
        env.GetCounter(*tablet1, "InvalidateNodeEventSendCount"));
    UNIT_ASSERT_VALUES_EQUAL(dryRun ? 0 : 3, env.SentEventCount);

    //
    // Subscriptions are one-shot - client 3 has to read the root again to get
    // the next event.
    //

    response = tablet1->GenerateFileSystemEvent(event);
    UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetClientCount());

    tablet3->GetNodeAttr(RootNodeId);
    response = tablet1->GenerateFileSystemEvent(event);
    UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GetClientCount());
    UNIT_ASSERT_VALUES_EQUAL(dryRun ? 0 : 4, env.SentEventCount);
}

void CheckCacheTTL(
    NProto::TStorageConfig config,
    TDuration sealPeriod,
    TDuration margin,
    ui32 expectedCacheTTL,
    ui32 expectedListingCacheTTL)
{
    config.SetFileSystemEventSubscriptionsSealPeriod(sealPeriod.MilliSeconds());
    config.SetFileSystemEventSubscriptionsCacheTTLMargin(margin.MilliSeconds());
    TFileSystemEventsEnv env(std::move(config));

    auto tablet = env.CreateClient("client", "session");
    tablet->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"));

    const TVector<ui32> cacheTTLs = {
        tablet->GetNodeAttr(RootNodeId, "f")->Record.GetCacheTTL(),
        tablet->GetNodeAttrBatch(RootNodeId, TVector<TString>{"f"})
            ->Record.GetCacheTTL(),
        tablet->ListNodes(RootNodeId)->Record.GetCacheTTL(),
        tablet->ListNodesInternal(RootNodeId)->Record.GetCacheTTL(),
    };

    const TVector<ui32> expected = {
        expectedCacheTTL,
        expectedListingCacheTTL,
        expectedListingCacheTTL,
        expectedListingCacheTTL,
    };
    UNIT_ASSERT_VALUES_EQUAL(expected, cacheTTLs);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_FileSystemEvents)
{
    Y_UNIT_TEST(ShouldSendFileSystemEventsToSubscribersOnly)
    {
        CheckFileSystemEventsAreSentToSubscribersOnly(false /* dryRun */);
    }

    Y_UNIT_TEST(ShouldCountButNotSendFileSystemEventsInDryRunMode)
    {
        CheckFileSystemEventsAreSentToSubscribersOnly(true /* dryRun */);
    }

    Y_UNIT_TEST(ShouldSubscribeUponListNodes)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        config.SetFileSystemListingEventsEnabled(true);
        TFileSystemEventsEnv env(std::move(config));

        auto tablet1 = env.CreateClient("client1", "session1");
        const auto nodeId =
            tablet1->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"))
                ->Record.GetNode()
                .GetId();

        //
        // Client 2 is subscribed to the listed entry and the listed node by
        // ListNodes, client 3 - by ListNodesInternal.
        //

        auto tablet2 = env.CreateClient("client2", "session2");
        tablet2->ListNodes(RootNodeId);

        auto tablet3 = env.CreateClient("client3", "session3");
        tablet3->ListNodesInternal(RootNodeId);

        NProto::TFileSystemEvent event;
        event.MutableInvalidateNode()->SetNodeId(nodeId);
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());

        event.Clear();
        auto* invalidate = event.MutableInvalidateNodeRef();
        invalidate->SetNodeId(RootNodeId);
        invalidate->SetName("f");
        UNIT_ASSERT_VALUES_EQUAL(
            2,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(4, env.SentEventCount);
    }

    Y_UNIT_TEST(ShouldSubscribeUponGetNodeAttrBatch)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        config.SetFileSystemListingEventsEnabled(true);
        TFileSystemEventsEnv env(std::move(config));

        auto tablet1 = env.CreateClient("client1", "session1");
        const auto nodeId =
            tablet1->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"))
                ->Record.GetNode()
                .GetId();

        auto tablet2 = env.CreateClient("client2", "session2");
        tablet2->GetNodeAttrBatch(RootNodeId, TVector<TString>{"f"});

        NProto::TFileSystemEvent event;
        event.MutableInvalidateNode()->SetNodeId(nodeId);
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());

        event.Clear();
        auto* invalidate = event.MutableInvalidateNodeRef();
        invalidate->SetNodeId(RootNodeId);
        invalidate->SetName("f");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(2, env.SentEventCount);
    }

    Y_UNIT_TEST(ShouldNotSubscribeUponCreateNodeAndCreateHandle)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        TFileSystemEventsEnv env(std::move(config));

        auto tablet = env.CreateClient("client", "session");
        const auto nodeId1 =
            tablet->CreateNode(TCreateNodeArgs::File(RootNodeId, "f1"))
                ->Record.GetNode()
                .GetId();
        const auto nodeId2 =
            tablet
                ->CreateHandle(RootNodeId, "f2", TCreateHandleArgs::CREATE)
                ->Record.GetNodeAttr()
                .GetId();

        NProto::TFileSystemEvent event;
        for (const auto nodeId: {nodeId1, nodeId2}) {
            event.Clear();
            event.MutableInvalidateNode()->SetNodeId(nodeId);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                tablet->GenerateFileSystemEvent(event)
                    ->Record.GetClientCount());
        }

        for (const auto* name: {"f1", "f2"}) {
            event.Clear();
            auto* invalidate = event.MutableInvalidateNodeRef();
            invalidate->SetNodeId(RootNodeId);
            invalidate->SetName(name);
            UNIT_ASSERT_VALUES_EQUAL(
                0,
                tablet->GenerateFileSystemEvent(event)
                    ->Record.GetClientCount());
        }

        UNIT_ASSERT_VALUES_EQUAL(0, env.SentEventCount);
    }

    Y_UNIT_TEST(ShouldRemoveSubscriptionsUponPipeDisconnect)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        TFileSystemEventsEnv env(std::move(config));

        auto tablet1 = env.CreateClient("client1", "session1");
        auto tablet2 = env.CreateClient("client2", "session2");
        tablet2->GetNodeAttr(RootNodeId);

        tablet2->DisconnectPipe();
        env.Env.GetRuntime().DispatchEvents({}, TDuration::MilliSeconds(100));

        NProto::TFileSystemEvent event;
        event.MutableInvalidateNode()->SetNodeId(RootNodeId);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(0, env.SentEventCount);
    }

    Y_UNIT_TEST(ShouldDropSubscriptionsAfterTwoSealPeriods)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        config.SetFileSystemEventSubscriptionsSealPeriod(
            TDuration::Minutes(1).MilliSeconds());
        TFileSystemEventsEnv env(std::move(config));

        auto tablet1 = env.CreateClient("client1", "session1");
        auto tablet2 = env.CreateClient("client2", "session2");

        const auto nodeId =
            tablet1->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"))
                ->Record.GetNode()
                .GetId();

        NProto::TFileSystemEvent event;
        event.MutableInvalidateNode()->SetNodeId(RootNodeId);

        //
        // A registration survives one seal - it's in the sealed generation.
        //

        tablet2->GetNodeAttr(RootNodeId);
        tablet1->AdvanceTime(TDuration::Minutes(1));
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());

        //
        // The second seal drops it. The seals are triggered by unrelated
        // registrations (GetNodeAttr of another node) so that the root
        // registration isn't extracted in between.
        //

        tablet2->GetNodeAttr(RootNodeId);
        tablet1->AdvanceTime(TDuration::Minutes(1));
        tablet1->GetNodeAttr(nodeId);
        tablet1->AdvanceTime(TDuration::Minutes(1));
        tablet1->GetNodeAttr(nodeId);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());
    }

    Y_UNIT_TEST(ShouldIgnoreShardInternalNodeRefs)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        TFileSystemEventsEnv env(std::move(config));

        auto tablet1 = env.CreateClient("client1", "session1");
        tablet1->ConfigureAsShard(1, "test", "main");

        //
        // In a shard the root contains the shard-resident nodes under the
        // names generated by the main tablet: neither the subscriptions nor
        // the events are created for such NodeRefs. The nodes themselves are
        // tracked as usual.
        //

        tablet1->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"));

        auto tablet2 = env.CreateClient("client2", "session2");
        tablet2->GetNodeAttr(RootNodeId, "f");

        NProto::TFileSystemEvent event;
        auto* invalidate = event.MutableInvalidateNodeRef();
        invalidate->SetNodeId(RootNodeId);
        invalidate->SetName("f");
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());

        tablet1->UnlinkNode(RootNodeId, "f", false /* unlinkDirectory */);

        //
        // UnlinkNode produces InvalidateNode only, for client 2 (the lookup).
        //

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            env.GetCounter(*tablet1, "InvalidateNodeEventSendCount"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            env.GetCounter(*tablet1, "InvalidateNodeRefEventSendCount"));
        UNIT_ASSERT_VALUES_EQUAL(1, env.SentEventCount);
    }

    Y_UNIT_TEST(ShouldNotSubscribeIfDisabled)
    {
        TFileSystemEventsEnv env({});

        auto tablet1 = env.CreateClient("client1", "session1");
        tablet1->GetNodeAttr(RootNodeId);

        NProto::TFileSystemEvent event;
        event.MutableInvalidateNode()->SetNodeId(RootNodeId);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(0, env.SentEventCount);
    }

    Y_UNIT_TEST(ShouldNotCountFileSystemEventsIfDisabled)
    {
        //
        // Dry run mode alone doesn't enable FileSystemEvents generation.
        //

        NProto::TStorageConfig config;
        config.SetFileSystemEventsDryRunModeEnabled(true);
        TFileSystemEventsEnv env(std::move(config));

        auto tablet = env.CreateClient("client", "session");
        tablet->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"));
        tablet->UnlinkNode(RootNodeId, "f", false /* unlinkDirectory */);

        UNIT_ASSERT_VALUES_EQUAL(
            0,
            env.GetCounter(*tablet, "InvalidateNodeEventSendCount"));
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            env.GetCounter(*tablet, "InvalidateNodeRefEventSendCount"));
        UNIT_ASSERT_VALUES_EQUAL(0, env.SentEventCount);
    }

    Y_UNIT_TEST(ShouldReturnCacheTTL)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        config.SetFileSystemListingEventsEnabled(true);
        CheckCacheTTL(
            std::move(config),
            TDuration::Minutes(10),
            TDuration::Minutes(1),
            TDuration::Minutes(9).MilliSeconds(),
            TDuration::Minutes(9).MilliSeconds());
    }

    Y_UNIT_TEST(ShouldReturnCacheTTLForGetNodeAttrOnlyIfListingDisabled)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        CheckCacheTTL(
            std::move(config),
            TDuration::Minutes(10),
            TDuration::Minutes(1),
            TDuration::Minutes(9).MilliSeconds(),
            0 /* expectedListingCacheTTL */);
    }

    Y_UNIT_TEST(ShouldReturnZeroCacheTTLIfDisabled)
    {
        //
        // FileSystemListingEventsEnabled alone doesn't enable anything.
        //

        NProto::TStorageConfig config;
        config.SetFileSystemListingEventsEnabled(true);
        CheckCacheTTL(
            std::move(config),
            TDuration::Minutes(10),
            TDuration::Minutes(1),
            0 /* expectedCacheTTL */,
            0 /* expectedListingCacheTTL */);
    }

    Y_UNIT_TEST(ShouldReturnZeroCacheTTLInDryRunMode)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        config.SetFileSystemListingEventsEnabled(true);
        config.SetFileSystemEventsDryRunModeEnabled(true);
        CheckCacheTTL(
            std::move(config),
            TDuration::Minutes(10),
            TDuration::Minutes(1),
            0 /* expectedCacheTTL */,
            0 /* expectedListingCacheTTL */);
    }

    Y_UNIT_TEST(ShouldReturnZeroCacheTTLIfMarginExceedsSealPeriod)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        config.SetFileSystemListingEventsEnabled(true);
        CheckCacheTTL(
            std::move(config),
            TDuration::Minutes(1),
            TDuration::Minutes(2),
            0 /* expectedCacheTTL */,
            0 /* expectedListingCacheTTL */);
    }

    Y_UNIT_TEST(ShouldNotSubscribeUponListingIfListingDisabled)
    {
        NProto::TStorageConfig config;
        config.SetFileSystemEventsEnabled(true);
        TFileSystemEventsEnv env(std::move(config));

        auto tablet1 = env.CreateClient("client1", "session1");
        const auto nodeId =
            tablet1->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"))
                ->Record.GetNode()
                .GetId();

        auto tablet2 = env.CreateClient("client2", "session2");
        tablet2->GetNodeAttrBatch(RootNodeId, TVector<TString>{"f"});
        tablet2->ListNodes(RootNodeId);
        tablet2->ListNodesInternal(RootNodeId);

        NProto::TFileSystemEvent event;
        event.MutableInvalidateNode()->SetNodeId(nodeId);
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());

        event.Clear();
        auto* invalidate = event.MutableInvalidateNodeRef();
        invalidate->SetNodeId(RootNodeId);
        invalidate->SetName("f");
        UNIT_ASSERT_VALUES_EQUAL(
            0,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());

        //
        // GetNodeAttr subscribes the client regardless of
        // FileSystemListingEventsEnabled.
        //

        tablet2->GetNodeAttr(RootNodeId, "f");
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            tablet1->GenerateFileSystemEvent(event)->Record.GetClientCount());
        UNIT_ASSERT_VALUES_EQUAL(1, env.SentEventCount);
    }
}

}   // namespace NCloud::NFileStore::NStorage
