#include "tablet.h"
#include "tablet_schema.h"

#include <cloud/filestore/libs/storage/tablet/model/block.h>
#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <cloud/storage/core/libs/api/hive_proxy.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/size_literals.h>

namespace NCloud::NFileStore::NStorage {

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Data_Truncate)
{
    using namespace NActors;

    using namespace NCloud::NStorage;

    Y_UNIT_TEST(ShouldOpenAndTruncateFiles)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);
        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        ui64 handle = CreateHandle(tablet, id);

        // unaligned
        const ui64 size = 5_KB;
        tablet.WriteData(handle, 0, size, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), size);

        tablet.CreateHandle(id, "", TCreateHandleArgs::CREATE | TCreateHandleArgs::TRUNC);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 0);

        tablet.AssertCreateHandleFailed(id, "", TCreateHandleArgs::RDNLY | TCreateHandleArgs::TRUNC);
    }

    Y_UNIT_TEST(ShouldTruncateFiles)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 4_KB + 5, '1');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 4_KB + 5);

        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);

        args.SetSize(4_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), TString(4_KB, '1'));

        args.SetSize(4_KB + 5);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 4_KB + 5);

        {
            TString expected = TString(4_KB, '1') + TString(5, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), expected);
        }

        args.SetSize(2_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 2_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), TString(2_KB, '1'));

        args.SetSize(4_KB + 5);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 4_KB + 5);

        {
            TString expected = TString(2_KB, '1');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 2_KB), expected);
        }

        {
            TString expected = TString(2_KB, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 2_KB, 2_KB), expected);
        }

        {
            TString expected = TString(5, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 5, 4_KB), expected);
        }

        {
            TString expected = TString(2_KB, '1') + TString(2_KB + 5, 0);
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), expected);
        }

        args.SetSize(0);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 0);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), TString());

        args.SetSize(GetDefaultMaxFileBlocks() * DefaultBlockSize + 1);
        tablet.AssertSetNodeAttrFailed(args);

        args.SetSize(18_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 18_KB);

        tablet.WriteData(handle, 0, 18_KB, '1');

        {
            TString expected = TString(18_KB, '1');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 18_KB), expected);
        }

        args.SetSize(14_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 14_KB);

        {
            TString expected = TString(14_KB, '1');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 14_KB), expected);
        }

        args.SetSize(6_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 6_KB);

        {
            TString expected = TString(6_KB, '1');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 6_KB), expected);
        }

        args.SetSize(16_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 16_KB);

        {
            TString expected = TString(6_KB, '1') + TString(10_KB, '\0');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 16_KB), expected);
        }

        args.SetSize(6_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 6_KB);

        {
            TString expected = TString(6_KB, '1');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 6_KB), expected);
        }

        args.SetSize(0);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 0);

        args.SetSize(6_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 6_KB);

        {
            TString expected = TString(6_KB, '\0');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 6_KB), expected);
        }

        // Truncate less than one block size more than once
        tablet.WriteData(handle, 0, 6462, 'a');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 6462);

        {
            TString expected = TString(6462, 'a');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 6462), expected);
        }

        args.SetSize(1795);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 1795);

        args.SetSize(717);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 717);

        args.SetSize(7966);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 7966);

        {
            TString expected = TString(717, 'a') + TString(7249, '\0');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 7966), expected);
        }

        // Truncate head aligned block
        args.SetSize(0);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 0);

        tablet.WriteData(handle, 0, 8_KB, 'a');
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 8_KB);

        {
            TString expected = TString(8_KB, 'a');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), expected);
        }

        args.SetSize(6_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 6_KB);

        {
            TString expected = TString(6_KB, 'a');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), expected);
        }

        args.SetSize(4_KB);
        tablet.SetNodeAttr(args);

        {
            TString expected = TString(4_KB, 'a');
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 4_KB), expected);
        }

        args.SetSize(8_KB);
        tablet.SetNodeAttr(args);

        {
            TString expected = TString(4_KB, 'a') + TString(4_KB, '\0');;
            UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), expected);
        }

        // TODO more test cases
    }

    Y_UNIT_TEST(ShouldTruncateRange)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // aligned
        tablet.WriteData(handle, 0, 8_KB, '1');
        // non aligned for each block
        tablet.WriteData(handle, 0, 1_KB, '2');
        tablet.WriteData(handle, 4_KB, 1_KB, '2');

        TString expected = TString(1_KB, '2') + TString(3_KB, '1');
        expected += expected;
        UNIT_ASSERT_EQUAL(ReadData(tablet, handle, 8_KB), expected);

        tablet.TruncateRange(id, 3_KB, 4_KB);

        // Here we have 5_KB of 0s instead of 4_KB, because TruncateRange
        // must be used only on the tail of the node with resizing.
        memset(&expected[3_KB], 0, 5_KB);
        UNIT_ASSERT_EQUAL(ReadData(tablet, handle, 4_KB), expected.substr(0, 4_KB));
        UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 4_KB, 4_KB), expected.substr(4_KB, 4_KB));
    }

    Y_UNIT_TEST(ShouldTruncateFreshBytes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        // aligned
        tablet.WriteData(handle, 0, 8_KB, '1');
        // non aligned for each block
        tablet.WriteData(handle, 0, 1_KB, '2');
        tablet.WriteData(handle, 4_KB, 1_KB, '2');

        TString expected(8_KB, '1');

        TString pattern(1_KB, '2');
        memcpy(&expected[0], &pattern[0], 1_KB);
        memcpy(&expected[4_KB], &pattern[0], 1_KB);

        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 8_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 8_KB), expected);

        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);

        args.SetSize(4_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 4_KB), expected.substr(0, 4_KB));

        args.SetSize(8_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 8_KB);

        auto data = ReadData(tablet, handle, 4_KB, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(data.size(), 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(data, TString(4_KB, 0));

        args.SetSize(9_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 9_KB);

        data = ReadData(tablet, handle, 6_KB, 4_KB);
        UNIT_ASSERT_VALUES_EQUAL(data.size(), 5_KB);
        UNIT_ASSERT_VALUES_EQUAL(data, TString(5_KB, 0));
    }

    Y_UNIT_TEST(ShouldTruncateEmptyFile)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        ui64 handle = CreateHandle(tablet, id);

        TSetNodeAttrArgs args(id);
        args.SetFlag(NProto::TSetNodeAttrRequest::F_SET_ATTR_SIZE);

        args.SetSize(5_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 5_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 6_KB), TString(5_KB, 0));

        args.SetSize(2_KB);
        tablet.SetNodeAttr(args);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 2_KB);
        UNIT_ASSERT_VALUES_EQUAL(ReadData(tablet, handle, 5_KB), TString(2_KB, 0));
    }

    Y_UNIT_TEST(ShouldSplitTruncateForBigFiles)
    {
        const ui32 limit = 16;
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxBlocksPerTruncateTx(limit);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        ui32 requests = 0;
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvTruncateRangeRequest: {
                        ++requests;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            }
        );

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        {
            // under limit
            ui32 size = limit * DefaultBlockSize;
            tablet.WriteData(handle, 0, size, 'a');

            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(0));
            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size));

            UNIT_ASSERT_VALUES_EQUAL(requests, 0);
            UNIT_ASSERT_EQUAL(ReadData(tablet, handle, size), TString(size, 0));
        }

        {
            // over the limit
            ui32 size = 6 * limit * DefaultBlockSize;
            tablet.WriteData(handle, 0, size, 'a');

            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(0));
            env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size));

            UNIT_ASSERT_VALUES_EQUAL(requests, 6);
            UNIT_ASSERT_EQUAL(ReadData(tablet, handle, size), TString(size, 0));
        }

        {
            requests = 0;
            // over the limit unaligned
            ui32 size = 6 * limit * DefaultBlockSize + 100;
            tablet.WriteData(handle, 0, size, 'a');

            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(0));
            env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size));

            UNIT_ASSERT_VALUES_EQUAL(requests, 7);
            UNIT_ASSERT_EQUAL(ReadData(tablet, handle, size), TString(size, 0));

            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(0));
        }

        {
            requests = 0;
            // over the limit unaligned
            ui32 size = limit * DefaultBlockSize + 200;
            tablet.WriteData(handle, 0, size, 'a');

            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(100));
            env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

            tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size));

            UNIT_ASSERT_VALUES_EQUAL(requests, 2);

            TString expected = TString(100, 'a') + TString(size - 100, 0);
            UNIT_ASSERT_EQUAL(ReadData(tablet, handle, size), expected);
        }
    }

    Y_UNIT_TEST(ShouldSplitTruncateForBigFilesByHandle)
    {
        const ui32 limit = 16;
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxBlocksPerTruncateTx(limit);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        ui32 requests = 0;
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvTruncateRangeRequest: {
                        ++requests;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            }
        );

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        ui32 size = 6 * limit * DefaultBlockSize;
        tablet.WriteData(handle, 0, size, 'a');

        auto h = CreateHandle(tablet, id, {}, TCreateHandleArgs::TRUNC | TCreateHandleArgs::RDWR);
        tablet.DestroyHandle(h);

        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(requests, 6);
        UNIT_ASSERT_VALUES_EQUAL(GetNodeAttrs(tablet, id).GetSize(), 0);

        tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(size));
        UNIT_ASSERT_EQUAL(ReadData(tablet, handle, size), TString(size, 0));
    }

    Y_UNIT_TEST(ShouldSplitTruncateForBigFilesByRename)
    {
        const ui32 limit = 16;
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxBlocksPerTruncateTx(limit);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        ui32 requests = 0;
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvTruncateRangeRequest: {
                        ++requests;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            }
        );

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));

        // over the limit
        ui32 size = 6 * limit * DefaultBlockSize;
        auto handle = CreateHandle(tablet, id1);
        tablet.WriteData(handle, 0, size, 'a');
        tablet.DestroyHandle(handle);

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        tablet.RenameNode(RootNodeId, "test2", RootNodeId, "test1");
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        tablet.AssertAccessNodeFailed(id1);
        UNIT_ASSERT_VALUES_EQUAL(requests, 6);
    }

    Y_UNIT_TEST(ShouldSplitTruncateForBigFilesByUnlink)
    {
        const ui32 limit = 16;
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxBlocksPerTruncateTx(limit);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        ui32 requests = 0;
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvTruncateRangeRequest: {
                        ++requests;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            }
        );

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        // over the limit
        ui32 size = 6 * limit * DefaultBlockSize;
        auto handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, size, 'a');
        tablet.DestroyHandle(handle);

        tablet.UnlinkNode(RootNodeId, "test", false);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        tablet.AssertAccessNodeFailed(id);
        UNIT_ASSERT_VALUES_EQUAL(requests, 6);
    }

    Y_UNIT_TEST(ShouldSplitTruncateForBigFilesByDestroyHandle)
    {
        const ui32 limit = 16;
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxBlocksPerTruncateTx(limit);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        ui32 requests = 0;
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvTruncateRangeRequest: {
                        ++requests;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            }
        );

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        // over the limit
        ui32 size = 6 * limit * DefaultBlockSize;
        auto handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, size, 'a');

        tablet.UnlinkNode(RootNodeId, "test", false);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto attrs = GetNodeAttrs(tablet, id);
        UNIT_ASSERT_VALUES_EQUAL(attrs.GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(attrs.GetSize(), size);
        UNIT_ASSERT_VALUES_EQUAL(requests, 0);

        tablet.DestroyHandle(handle);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(requests, 6);
    }

    Y_UNIT_TEST(ShouldSplitTruncateForBigFilesByResetSession)
    {
        const ui32 limit = 16;
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxBlocksPerTruncateTx(limit);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        ui32 requests = 0;
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvTruncateRangeRequest: {
                        ++requests;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            }
        );

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        // over the limit
        ui32 size = 6 * limit * DefaultBlockSize;
        auto handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, size, 'a');

        tablet.UnlinkNode(RootNodeId, "test", false);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto attrs = GetNodeAttrs(tablet, id);
        UNIT_ASSERT_VALUES_EQUAL(attrs.GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(attrs.GetSize(), size);
        UNIT_ASSERT_VALUES_EQUAL(requests, 0);

        tablet.ResetSession("client", "session", 0, "state");
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        tablet.AssertAccessNodeFailed(id);
        UNIT_ASSERT_VALUES_EQUAL(requests, 6);
    }

    Y_UNIT_TEST(ShouldSplitTruncateForBigFilesByDestroySession)
    {
        const ui32 limit = 16;
        NProto::TStorageConfig storageConfig;
        storageConfig.SetMaxBlocksPerTruncateTx(limit);

        TTestEnv env({}, std::move(storageConfig));

        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        ui32 requests = 0;
        env.GetRuntime().SetObserverFunc([&] (TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& event)
            {
                switch (event->GetTypeRewrite()) {
                    case TEvIndexTabletPrivate::EvTruncateRangeRequest: {
                        ++requests;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(runtime, event);
            }
        );

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        // over the limit
        ui32 size = 6 * limit * DefaultBlockSize;
        auto handle = CreateHandle(tablet, id);
        tablet.WriteData(handle, 0, size, 'a');

        tablet.UnlinkNode(RootNodeId, "test", false);
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        auto attrs = GetNodeAttrs(tablet, id);
        UNIT_ASSERT_VALUES_EQUAL(attrs.GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(attrs.GetSize(), size);
        UNIT_ASSERT_VALUES_EQUAL(requests, 0);

        tablet.DestroySession();
        env.GetRuntime().DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));

        tablet.AssertAccessNodeFailed(id);
        UNIT_ASSERT_VALUES_EQUAL(requests, 6);
    }
}

}   // namespace NCloud::NFileStore::NStorage
