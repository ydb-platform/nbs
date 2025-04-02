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
        env.CreateSubDomain("nfs");
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.CreateNode("nfs");
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
    }

    Y_UNIT_TEST(ShouldSetGuestKeepCacheBasedOnMtime)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetGuestKeepCacheAllowed(true);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.CreateNode("nfs");
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
}

}   // namespace NCloud::NFileStore::NStorage
