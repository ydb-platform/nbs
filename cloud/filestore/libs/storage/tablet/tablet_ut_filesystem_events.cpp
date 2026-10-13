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

void CheckFileSystemEventSendsAreCounted(bool dryRun)
{
    NProto::TStorageConfig config;
    config.SetFileSystemEventsEnabled(true);
    config.SetFileSystemEventsDryRunModeEnabled(dryRun);
    TFileSystemEventsEnv env(std::move(config));

    //
    // CreateNode registers the first client and produces InvalidateNodeRef
    // for the new entry: 1 send.
    //

    auto tablet1 = env.CreateClient("client1", "session1");
    tablet1->CreateNode(TCreateNodeArgs::File(RootNodeId, "f"));

    //
    // GetNodeAttr registers the second client.
    //

    auto tablet2 = env.CreateClient("client2", "session2");
    tablet2->GetNodeAttr(RootNodeId);

    //
    // UnlinkNode produces InvalidateNodeRef for the entry and InvalidateNode
    // for the removed node: 2 sends each. The parent's MTime/CTime changes
    // produce nothing.
    //

    tablet1->UnlinkNode(RootNodeId, "f", false /* unlinkDirectory */);

    UNIT_ASSERT_VALUES_EQUAL(
        2,
        env.GetCounter(*tablet1, "InvalidateNodeEventSendCount"));
    UNIT_ASSERT_VALUES_EQUAL(
        3,
        env.GetCounter(*tablet1, "InvalidateNodeRefEventSendCount"));
    UNIT_ASSERT_VALUES_EQUAL(dryRun ? 0 : 5, env.SentEventCount);

    //
    // GenerateFileSystemEvent goes through the same path.
    //

    NProto::TFileSystemEvent event;
    event.MutableInvalidateNode()->SetNodeId(42);
    const auto response = tablet1->GenerateFileSystemEvent(event);
    UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetClientCount());

    UNIT_ASSERT_VALUES_EQUAL(
        4,
        env.GetCounter(*tablet1, "InvalidateNodeEventSendCount"));
    UNIT_ASSERT_VALUES_EQUAL(dryRun ? 0 : 7, env.SentEventCount);
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_FileSystemEvents)
{
    Y_UNIT_TEST(ShouldCountFileSystemEventSends)
    {
        CheckFileSystemEventSendsAreCounted(false /* dryRun */);
    }

    Y_UNIT_TEST(ShouldCountButNotSendFileSystemEventsInDryRunMode)
    {
        CheckFileSystemEventSendsAreCounted(true /* dryRun */);
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
}

}   // namespace NCloud::NFileStore::NStorage
