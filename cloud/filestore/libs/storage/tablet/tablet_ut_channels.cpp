#include "tablet.h"

#include <cloud/filestore/libs/diagnostics/critical_events.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>
#include <cloud/filestore/libs/storage/testlib/ut_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NMonitoring;

using namespace NKikimr;

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Channels)
{
    Y_UNIT_TEST(ShouldSwitchToBrokenStateIfTabletChannelCountDecreased)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TDynamicCountersPtr counters = new TDynamicCounters();
        InitCriticalEventsCounter(counters);

        auto invalidTabletStorageInfoCounter = counters->GetCounter(
            "AppCriticalEvents/InvalidTabletStorageInfo",
            true);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);

        // decrease channel count
        env.UpdateTabletStorageInfo(tabletId, DefaultChannelCount - 1);
        tablet.RebootTablet();
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        // check critical event
        UNIT_ASSERT_VALUES_EQUAL(1, invalidTabletStorageInfoCounter->Val());

        // check that requests are rejected
        {
            tablet.SendCreateSessionRequest("client", "session");
            auto response = tablet.RecvCreateSessionResponse();
            UNIT_ASSERT_VALUES_EQUAL(E_REJECTED, response->GetStatus());
        }
    }

    Y_UNIT_TEST(ShouldUseProperChannelsDuringResize)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetWriteBlobThreshold(16_KB);

        TTestEnv env({.ChannelCount = 4}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        // only one mixed channel
        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            {.ChannelCount = 4});
        tablet.InitSession("client", "session");
        {
            auto stats = GetStorageStats(tablet);
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetConfigChannelCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetTabletChannelCount());
        }

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        TVector<ui32> channels;

        env.GetRuntime().SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvWriteBlobRequest: {
                        auto* msg = event->Get<
                            TEvIndexTabletPrivate::TEvWriteBlobRequest>();
                        for (const auto& blob: msg->Blobs) {
                            channels.push_back(blob.BlobId.Channel());
                        }
                        break;
                    }
                }

                return false;
            });

        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');

        SortUnique(channels);
        ASSERT_VECTORS_EQUAL(channels, TVector<ui32>({3}));
        channels.clear();

        // simulate situation when we received updateconfig from schemeshard
        // earlier than tablet restart from hive
        tablet.UpdateConfig({.ChannelCount = 5});
        {
            auto stats = GetStorageStats(tablet);
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetConfigChannelCount());
            UNIT_ASSERT_VALUES_EQUAL(4, stats.GetTabletChannelCount());
        }

        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');

        // 4th must be unused: it's not present at tabetInfo yet
        SortUnique(channels);
        ASSERT_VECTORS_EQUAL(channels, TVector<ui32>({3}));
        channels.clear();

        // now restart the tablet with new tabletInfo
        env.UpdateTabletStorageInfo(tabletId, 5);
        tablet.RebootTablet();
        tablet.RecoverSession();
        {
            auto stats = GetStorageStats(tablet);
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetConfigChannelCount());
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetTabletChannelCount());
        }

        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');

        // both 3rd and 4th channels must be used
        SortUnique(channels);
        ASSERT_VECTORS_EQUAL(channels, TVector<ui32>({3, 4}));
        channels.clear();

        // resize the same way but in opposite order
        // restart tablet with new tabletInfo
        env.UpdateTabletStorageInfo(tabletId, 6);
        tablet.RebootTablet();
        tablet.RecoverSession();
        {
            auto stats = GetStorageStats(tablet);
            UNIT_ASSERT_VALUES_EQUAL(5, stats.GetConfigChannelCount());
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetTabletChannelCount());
        }

        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');

        // 5th must be unused: it's not present in filesystem config
        SortUnique(channels);
        ASSERT_VECTORS_EQUAL(channels, TVector<ui32>({3, 4}));
        channels.clear();

        // now update filesystem config
        tablet.UpdateConfig({.ChannelCount = 6});
        {
            auto stats = GetStorageStats(tablet);
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetConfigChannelCount());
            UNIT_ASSERT_VALUES_EQUAL(6, stats.GetTabletChannelCount());
        }

        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');

        // all the channels must be used
        SortUnique(channels);
        ASSERT_VECTORS_EQUAL(channels, TVector<ui32>({3, 4, 5}));
        channels.clear();
    }

    Y_UNIT_TEST(ShouldCollectGarbageForAllUsedMixedChannels)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCompactionThreshold(999'999);
        storageConfig.SetCleanupThreshold(999'999);
        storageConfig.SetFlushThreshold(1_GB);
        storageConfig.SetFlushBytesThreshold(1_GB);
        storageConfig.SetWriteBlobThreshold(16_KB);

        TTestEnv env({.ChannelCount = 5}, std::move(storageConfig));
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        // two mixed channels
        TIndexTabletClient tablet(
            env.GetRuntime(),
            nodeIdx,
            tabletId,
            {.ChannelCount = 5});
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.WriteData(handle, 0, 16_KB, 'a');

        TVector<ui32> channels;

        env.GetRuntime().SetEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                Y_UNUSED(runtime);

                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg =
                            event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        channels.push_back(msg->Channel);
                        break;
                    }
                }

                return false;
            });

        const ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);
        tablet.CollectGarbage();

        ASSERT_VECTORS_EQUAL(channels, TVector<ui32>({3, 4}));
    }

    Y_UNIT_TEST(ShouldProperlyHandleCollectGarbageErrors)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetCollectGarbageThreshold(1);

        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        bool collectRequestObserved = false;
        bool deleteGarbageObserved = false;
        bool sendError = true;

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        auto* msg =
                            event->Get<TEvBlobStorage::TEvCollectGarbage>();
                        collectRequestObserved = true;
                        if (sendError) {
                            auto response = std::make_unique<
                                TEvBlobStorage::TEvCollectGarbageResult>(
                                NKikimrProto::ERROR,
                                0,   // doesn't matter
                                0,   // doesn't matter
                                0,   // doesn't matter
                                msg->Channel);

                            runtime.Send(
                                new IEventHandle(
                                    event->Sender,
                                    event->Recipient,
                                    response.release(),
                                    0,   // flags
                                    0),
                                nodeIdx);

                            return TTestActorRuntime::EEventAction::DROP;
                        }

                        break;
                    }

                    case TEvIndexTabletPrivate::EvDeleteGarbageRequest: {
                        deleteGarbageObserved = true;

                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 16_KB, 'a');
        tablet.Flush();

        const ui32 rangeId = GetMixedRangeIndex(id, 0);
        tablet.Compaction(rangeId);
        tablet.Cleanup(rangeId);

        tablet.SendCollectGarbageRequest();
        {
            auto response = tablet.RecvCollectGarbageResponse();
            UNIT_ASSERT_VALUES_EQUAL(
                MAKE_KIKIMR_ERROR(NKikimrProto::ERROR),
                response->GetStatus());
        }

        UNIT_ASSERT(collectRequestObserved);
        UNIT_ASSERT(!deleteGarbageObserved);

        sendError = false;
        collectRequestObserved = false;
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(collectRequestObserved);
        UNIT_ASSERT(deleteGarbageObserved);
    }

    Y_UNIT_TEST(ShouldExecuteCollectGarbageAtStartup)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetEnableCollectGarbageAtStart(true);

        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");

        bool collectGarbageObserved = false;
        bool deleteGarbageObserved = false;

        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc(
            [&](TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvCollectGarbage: {
                        collectGarbageObserved = true;
                        break;
                    }
                    case TEvIndexTabletPrivate::EvDeleteGarbageRequest: {
                        deleteGarbageObserved = true;
                        break;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            });

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT(collectGarbageObserved);
        UNIT_ASSERT(!deleteGarbageObserved);
    }
}

}   // namespace NCloud::NFileStore::NStorage
