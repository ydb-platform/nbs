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
              {"sensor", "CreateHandle.GuestKeepCacheSet"}},
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
              {"sensor", "CreateHandle.GuestKeepCacheSet"}},                   \
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
