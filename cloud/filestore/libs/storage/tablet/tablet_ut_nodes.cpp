#include "tablet.h"

#include <cloud/filestore/libs/storage/testlib/tablet_client.h>
#include <cloud/filestore/libs/storage/testlib/test_env.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NCloud::NFileStore::NStorage {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TIndexTabletTest_Nodes)
{
    Y_UNIT_TEST(ShouldCreateFileNode)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        auto id3 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test3"));

        tablet.AccessNode(id1);
        tablet.AccessNode(id2);
        tablet.AccessNode(id3);
        tablet.AssertAccessNodeFailed(InvalidNodeId);
        tablet.AssertAccessNodeFailed(id3 + 100500);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedNodesCount(), 3);
        }
    }

    Y_UNIT_TEST(ShouldNotCreateNodeAtInvalidPath)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        tablet.AssertCreateNodeFailed(TCreateNodeArgs::File(RootNodeId + 10, "ttt"));
    }

    Y_UNIT_TEST(ShouldNotCreateInvalidNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        TVector<TString> names = {
            {},
            ".",
            "..",
            "aaa/bbb",
            TString("aaa\0bbb", 7),
            TString(MaxName + 1, 'a'),
        };

        for (auto& name: names) {
            tablet.AssertCreateNodeFailed(TCreateNodeArgs::File(RootNodeId, name));
            tablet.AssertCreateNodeFailed(TCreateNodeArgs::Directory(RootNodeId, name));
            tablet.AssertCreateNodeFailed(TCreateNodeArgs::Link(RootNodeId, name, id));
            tablet.AssertCreateNodeFailed(TCreateNodeArgs::Sock(RootNodeId, name));
            tablet.AssertCreateNodeFailed(TCreateNodeArgs::Fifo(RootNodeId, name));

            tablet.AssertCreateHandleFailed(RootNodeId, name, TCreateHandleArgs::CREATE);
            tablet.AssertRenameNodeFailed(RootNodeId, "test", RootNodeId, name);
        }
    }

    Y_UNIT_TEST(ShouldCreateDirectoryNode)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test1"));
        // /test1/test2
        auto id2 = CreateNode(tablet, TCreateNodeArgs::Directory(id1, "test2"));
        // /test1/test3
        auto id3 = CreateNode(tablet, TCreateNodeArgs::Directory(id1, "test3"));

        tablet.AccessNode(id1);
        tablet.AccessNode(id2);
        tablet.AccessNode(id3);
    }

    Y_UNIT_TEST(ShouldCreateLinkNode)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test1"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(id1, "test2"));
        auto id3 = CreateNode(tablet, TCreateNodeArgs::Link(id1, "test3", id2));
        UNIT_ASSERT_VALUES_EQUAL(id2, id3);

        auto stat = tablet.GetNodeAttr(id3)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), id2);
        UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 2);
    }

    Y_UNIT_TEST(ShouldNotIncrementBlocksForLink)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        tablet.SetNodeAttr(TSetNodeAttrArgs(id1).SetSize(4_KB));
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), 1);

        auto id2 = CreateNode(tablet, TCreateNodeArgs::Link(RootNodeId, "test3", id1));
        Y_UNUSED(id2);
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), 1);
    }

    Y_UNIT_TEST(ShouldNotCreateLinkToInvalidNode)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "file"));

        // non existing node
        tablet.AssertCreateNodeFailed(TCreateNodeArgs::Link(RootNodeId, "xxx", 100500));
        // directory node
        tablet.AssertCreateNodeFailed(TCreateNodeArgs::Link(RootNodeId, "xxx", id1));
        // existing name
        tablet.AssertCreateNodeFailed(TCreateNodeArgs::Link(RootNodeId, "dir", id2));
    }

    Y_UNIT_TEST(ShouldCreateSymLinkNode)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test1"));
        // /test1/test2
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(id1, "test2"));
        // /test1/test3 -> /test1/test2
        auto id3 = CreateNode(tablet, TCreateNodeArgs::SymLink(id1, "test3", "/test1/test2"));

        // it is OK to symlink anything
        auto id4 = CreateNode(tablet, TCreateNodeArgs::SymLink(id1, "test4", "/ttt"));

        tablet.AccessNode(id1);
        tablet.AccessNode(id2);
        tablet.AccessNode(id3);
        tablet.AccessNode(id4);

        tablet.AssertReadLinkFailed(id1);
        tablet.AssertReadLinkFailed(id2);

        auto response = tablet.ReadLink(id3);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetSymLink(), "/test1/test2");

        response = tablet.ReadLink(id4);
        UNIT_ASSERT_VALUES_EQUAL(response->Record.GetSymLink(), "/ttt");
    }

    Y_UNIT_TEST(ShouldRemoveNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test1"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(id1, "test2"));
        auto id3 = CreateNode(tablet, TCreateNodeArgs::File(id1, "test3"));

        tablet.AccessNode(id1);
        tablet.AccessNode(id2);
        tablet.AccessNode(id3);

        tablet.AssertUnlinkNodeFailed(id1, "test2", true);
        tablet.UnlinkNode(id1, "test2", false);

        tablet.AccessNode(id1);
        tablet.AccessNode(id3);
        tablet.AssertAccessNodeFailed(id2);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(stats.GetUsedNodesCount(), 2);
        }
    }

    Y_UNIT_TEST(ShouldNotRemoveNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(id1, "file"));
        auto id3 = CreateNode(tablet, TCreateNodeArgs::Link(id1, "link", id2));

        tablet.AccessNode(id1);
        tablet.AccessNode(id2);
        UNIT_ASSERT_VALUES_EQUAL(id2, id3);

        auto stat = tablet.GetNodeAttr(id2)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 2);

        // no recursive for the directory
        tablet.AssertUnlinkNodeFailed(RootNodeId, "dir", false);
        // non empty directory
        tablet.AssertUnlinkNodeFailed(RootNodeId, "dir", true);
        // non existent
        tablet.AssertUnlinkNodeFailed(RootNodeId, "xxxx", true);
        tablet.AssertUnlinkNodeFailed(RootNodeId, "xxxx", false);

        // should not remove node, but reduce links
        tablet.UnlinkNode(id1, "file", false);

        tablet.AccessNode(id1);
        tablet.AccessNode(id2);

        stat = tablet.GetNodeAttr(id2)->Record.GetNode();
        UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 1);
    }

    Y_UNIT_TEST(ShouldNotRemoveInvalidPath)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.AssertUnlinkNodeFailed(RootNodeId, "", true);
        tablet.AssertUnlinkNodeFailed(RootNodeId, "ttt", false);
        tablet.AssertUnlinkNodeFailed(RootNodeId + 100, "ttt", false);
        tablet.AssertUnlinkNodeFailed(RootNodeId + 100, "", true);
    }

    Y_UNIT_TEST(ShouldRenameDirNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "xxx"));
        CreateNode(tablet, TCreateNodeArgs::SymLink(RootNodeId, "yyy", "zzz"));
        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "a"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "non-empty"));
        CreateNode(tablet, TCreateNodeArgs::File(id2, "file"));

        // existing -> non-existing
        tablet.RenameNode(RootNodeId, "a", RootNodeId, "empty");

        auto response = tablet.ListNodes(RootNodeId);
        const auto& names = response->Record.GetNames();
        UNIT_ASSERT_VALUES_EQUAL(names.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(names[0], "empty");
        UNIT_ASSERT_VALUES_EQUAL(names[1], "non-empty");
        UNIT_ASSERT_VALUES_EQUAL(names[2], "xxx");
        UNIT_ASSERT_VALUES_EQUAL(names[3], "yyy");

        // file -> dir
        {
            auto response = tablet.AssertRenameNodeFailed(RootNodeId, "xxx", RootNodeId, "empty");
            const auto& error = response->GetError();
            UNIT_ASSERT_VALUES_EQUAL(STATUS_FROM_CODE(error.GetCode()), (ui32)NProto::E_FS_ISDIR);
        }

        // dir -> file
        {
            auto response = tablet.AssertRenameNodeFailed(RootNodeId, "empty", RootNodeId, "xxx");
            const auto& error = response->GetError();
            UNIT_ASSERT_VALUES_EQUAL(STATUS_FROM_CODE(error.GetCode()), (ui32)NProto::E_FS_NOTDIR);
        }

        // link -> dir
        {
            auto response = tablet.AssertRenameNodeFailed(RootNodeId, "yyy", RootNodeId, "empty");
            const auto& error = response->GetError();
            UNIT_ASSERT_VALUES_EQUAL(STATUS_FROM_CODE(error.GetCode()), (ui32)NProto::E_FS_ISDIR);
        }

        // dir -> link
        {
            auto response = tablet.AssertRenameNodeFailed(RootNodeId, "empty", RootNodeId, "yyy");
            const auto& error = response->GetError();
            UNIT_ASSERT_VALUES_EQUAL(STATUS_FROM_CODE(error.GetCode()), (ui32)NProto::E_FS_NOTDIR);
        }

        // existing -> existing not empty
        {
            auto response = tablet.AssertRenameNodeFailed(RootNodeId, "empty", RootNodeId, "non-empty");
            const auto& error = response->GetError();
            UNIT_ASSERT_VALUES_EQUAL(STATUS_FROM_CODE(error.GetCode()), (ui32)NProto::E_FS_NOTEMPTY);
        }

        // existing -> existing empty
        tablet.RenameNode(RootNodeId, "non-empty", RootNodeId, "empty");
        tablet.AssertAccessNodeFailed(id1);

        {
            auto response = tablet.ListNodes(RootNodeId);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(names[0], "empty");
            UNIT_ASSERT_VALUES_EQUAL(names[1], "xxx");
            UNIT_ASSERT_VALUES_EQUAL(names[2], "yyy");
        }

        {
            auto response = tablet.ListNodes(id2);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(names[0], "file");
        }
    }

    Y_UNIT_TEST(ShouldRenameFileNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "a"));
        CreateNode(tablet, TCreateNodeArgs::Link(RootNodeId, "b", id1));

        // If oldpath and newpath are existing hard links to the same file, then rename() does nothing
        tablet.RenameNode(RootNodeId, "a", RootNodeId, "b");
        {
            auto response = tablet.ListNodes(RootNodeId);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(names[0], "a");
            UNIT_ASSERT_VALUES_EQUAL(names[1], "b");
        }

        // a b -> b c
        tablet.RenameNode(RootNodeId, "a", RootNodeId, "c");
        {
            auto response = tablet.ListNodes(RootNodeId);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(names[0], "b");
            UNIT_ASSERT_VALUES_EQUAL(names[1], "c");
        }

        // b c d -> b c
        auto id2 = CreateNode(tablet, TCreateNodeArgs::SymLink(RootNodeId, "d", "b"));
        tablet.RenameNode(RootNodeId, "d", RootNodeId, "b");
        {
            auto response = tablet.ReadLink(id2);
            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetSymLink(), "b");

            auto stat = tablet.GetNodeAttr(id1)->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), id1);
            UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 1);
        }

        // b c -> c
        CreateHandle(tablet, id1);
        tablet.RenameNode(RootNodeId, "b", RootNodeId, "c");
        {
            auto response = tablet.ListNodes(RootNodeId);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(names[0], "c");

            auto link = tablet.ReadLink(id2);
            UNIT_ASSERT_VALUES_EQUAL(link->Record.GetSymLink(), "b");

            auto stat = tablet.GetNodeAttr(id1)->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), id1);
            UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 0);
        }

        // non-existing to existing
        tablet.AssertRenameNodeFailed(RootNodeId, "", RootNodeId, "c");
        // existing to invalid
        tablet.AssertRenameNodeFailed(RootNodeId, "c", RootNodeId + 100, "c");

        // rename into different dir
        auto id3 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        tablet.RenameNode(RootNodeId, "c", id3, "c");
        {
            auto response = tablet.ListNodes(RootNodeId);
            auto names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(names[0], "dir");

            response = tablet.ListNodes(id3);
            names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(names[0], "c");

            auto link = tablet.ReadLink(id2);
            UNIT_ASSERT_VALUES_EQUAL(link->Record.GetSymLink(), "b");
        }
    }

    Y_UNIT_TEST(ShouldRenameAccordingToFlags)
    {
        const ui32 noreplace = ProtoFlag(NProto::TRenameNodeRequest::F_NOREPLACE);
        const ui32 exchange = ProtoFlag(NProto::TRenameNodeRequest::F_EXCHANGE);

        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "a"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "b"));

        // if newpath exists we should not perform
        tablet.AssertRenameNodeFailed(RootNodeId, "a", RootNodeId, "b", noreplace);
        // both flags is invalid
        tablet.AssertRenameNodeFailed(RootNodeId, "a", RootNodeId, "b", noreplace | exchange);
        // target should exist for exchange
        tablet.AssertRenameNodeFailed(RootNodeId, "a", RootNodeId, "c", exchange);

        // should keep both
        tablet.RenameNode(RootNodeId, "a", RootNodeId, "b", exchange);
        {
            auto response = tablet.ListNodes(RootNodeId);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(names[0], "a");
            UNIT_ASSERT_VALUES_EQUAL(names[1], "b");

            auto stat = tablet.GetNodeAttr(RootNodeId, "a")->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), id2);
            UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 1);

            stat = tablet.GetNodeAttr(RootNodeId, "b")->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), id1);
            UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 1);
        }

        // non empty dir
        auto id3 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "c"));
        CreateNode(tablet, TCreateNodeArgs::Directory(id3, "d"));

        // should allow to rename any nodes
        tablet.RenameNode(RootNodeId, "a", RootNodeId, "c", exchange);
        {
            auto response = tablet.ListNodes(RootNodeId);
            const auto& names1 = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names1.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(names1[0], "a");
            UNIT_ASSERT_VALUES_EQUAL(names1[1], "b");
            UNIT_ASSERT_VALUES_EQUAL(names1[2], "c");

            auto stat = tablet.GetNodeAttr(RootNodeId, "c")->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), id2);
            UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 1);

            stat = tablet.GetNodeAttr(RootNodeId, "a")->Record.GetNode();
            UNIT_ASSERT_VALUES_EQUAL(stat.GetId(), id3);
            UNIT_ASSERT_VALUES_EQUAL(stat.GetLinks(), 1);

            response = tablet.ListNodes(id3);
            const auto& names2 = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(names2.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(names2[0], "d");
        }
    }

    Y_UNIT_TEST(ShouldListNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test1"));
        CreateNode(tablet, TCreateNodeArgs::Directory(id1, "xxx"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test2"));
        CreateNode(tablet, TCreateNodeArgs::Directory(id2, "yyy"));
        auto id3 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test3"));
        CreateNode(tablet, TCreateNodeArgs::Directory(id3, "zzz"));

        auto response = tablet.ListNodes(RootNodeId);

        const auto& names = response->Record.GetNames();
        UNIT_ASSERT(names.size() == 3);
        UNIT_ASSERT(names[0] == "test1");
        UNIT_ASSERT(names[1] == "test2");
        UNIT_ASSERT(names[2] == "test3");

        const auto& nodes = response->Record.GetNodes();
        UNIT_ASSERT(nodes.size() == 3);
        UNIT_ASSERT(nodes.Get(0).GetId() == id1);
        UNIT_ASSERT(nodes.Get(1).GetId() == id2);
        UNIT_ASSERT(nodes.Get(2).GetId() == id3);
    }

    Y_UNIT_TEST(ShouldLimitListNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test1"));
        CreateNode(tablet, TCreateNodeArgs::Directory(id1, "xxx"));
        auto id2 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test2"));
        CreateNode(tablet, TCreateNodeArgs::Directory(id2, "yyy"));
        auto id3 = CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "test3"));
        CreateNode(tablet, TCreateNodeArgs::Directory(id3, "zzz"));

        {
            auto response = tablet.ListNodes(RootNodeId, 1, TString{});

            const auto& names = response->Record.GetNames();
            UNIT_ASSERT(names.size() == 1);
            UNIT_ASSERT(names[0] == "test1");
            const auto& nodes = response->Record.GetNodes();
            UNIT_ASSERT(nodes.size() == 1);
            UNIT_ASSERT(nodes.Get(0).GetId() == id1);

            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), "test2");
        }

        {
            auto response = tablet.ListNodes(RootNodeId, 1, "test2");

            const auto& names = response->Record.GetNames();
            UNIT_ASSERT(names.size() == 1);
            UNIT_ASSERT(names[0] == "test2");
            const auto& nodes = response->Record.GetNodes();
            UNIT_ASSERT(nodes.size() == 1);
            UNIT_ASSERT(nodes.Get(0).GetId() == id2);

            UNIT_ASSERT_VALUES_EQUAL(response->Record.GetCookie(), "test3");
        }

        {
            auto response = tablet.ListNodes(RootNodeId, 0, "test3");

            const auto& names = response->Record.GetNames();
            UNIT_ASSERT(names.size() == 1);
            UNIT_ASSERT(names[0] == "test3");
            const auto& nodes = response->Record.GetNodes();
            UNIT_ASSERT(nodes.size() == 1);
            UNIT_ASSERT(nodes.Get(0).GetId() == id3);

            UNIT_ASSERT(response->Record.GetCookie().empty());
        }
    }

    Y_UNIT_TEST(ShouldStoreNodeAttrs)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test3"));
        TSetNodeAttrArgs arg(id);
        arg.SetMode(777);
        arg.SetUid(100500);
        arg.SetGid(500100);
        arg.SetATime(111111);
        arg.SetMTime(222222);
        arg.SetCTime(333333);
        arg.SetSize(77);

        tablet.SetNodeAttr(arg);

        // lookup
        auto response = tablet.GetNodeAttr(id);
        auto node = response->Record.GetNode();

        UNIT_ASSERT_VALUES_EQUAL(node.GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(node.GetMode(), 777);
        UNIT_ASSERT_VALUES_EQUAL(node.GetUid(), 100500);
        UNIT_ASSERT_VALUES_EQUAL(node.GetGid(), 500100);
        UNIT_ASSERT_VALUES_EQUAL(node.GetATime(), 111111);
        UNIT_ASSERT_VALUES_EQUAL(node.GetMTime(), 222222);
        UNIT_ASSERT_VALUES_EQUAL(node.GetCTime(), 333333);
        UNIT_ASSERT_VALUES_EQUAL(node.GetSize(), 77);

        // get node attr
        response = tablet.GetNodeAttr(RootNodeId, "test3");
        node = response->Record.GetNode();

        UNIT_ASSERT_VALUES_EQUAL(node.GetId(), id);
        UNIT_ASSERT_VALUES_EQUAL(node.GetMode(), 777);
        UNIT_ASSERT_VALUES_EQUAL(node.GetUid(), 100500);
        UNIT_ASSERT_VALUES_EQUAL(node.GetGid(), 500100);
        UNIT_ASSERT_VALUES_EQUAL(node.GetATime(), 111111);
        UNIT_ASSERT_VALUES_EQUAL(node.GetMTime(), 222222);
        UNIT_ASSERT_VALUES_EQUAL(node.GetCTime(), 333333);
        UNIT_ASSERT_VALUES_EQUAL(node.GetSize(), 77);

        arg.Node = 100500;
        tablet.AssertSetNodeAttrFailed(arg);
    }

    Y_UNIT_TEST(ShouldNotStoreNodeAttrsForInvalidNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));

        tablet.AssertGetNodeAttrFailed(RootNodeId, "xxxx");
        tablet.AssertGetNodeAttrFailed(id + 100500);

        tablet.AssertGetNodeAttrFailed(RootNodeId, "xxxx");
        tablet.AssertGetNodeAttrFailed(id + 100500);
    }

    Y_UNIT_TEST(ShouldStoreNodeXAttrs)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.SetNodeXAttr(id, "user.name1", "value1");
        tablet.AssertSetNodeXAttrFailed(id, "user.name1", "value2", NProto::TSetNodeXAttrRequest::F_CREATE);

        tablet.AssertSetNodeXAttrFailed(id, "user.name2", "value1", NProto::TSetNodeXAttrRequest::F_REPLACE);
        tablet.SetNodeXAttr(id, "user.name2", "value2");

        tablet.SetNodeXAttr(id, "user.name3", "value3");

        {
            auto response = tablet.GetNodeXAttr(id, "user.name1");
            const auto& value = response->Record.GetValue();
            UNIT_ASSERT(value == "value1");
        }

        tablet.RemoveNodeXAttr(id, "user.name2");

        {
            auto response = tablet.ListNodeXAttr(id);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT(names.size() == 2);
            UNIT_ASSERT(names[0] == "user.name1");
            UNIT_ASSERT(names[1] == "user.name3");
        }

        // should fail if attribute doesn't exist, but differently to an invalid attr
        auto response1 = tablet.AssertRemoveNodeXAttrFailed(id, "user.name2");
        auto response2 = tablet.AssertRemoveNodeXAttrFailed(id, "invalid attr");
        UNIT_ASSERT(response1->Record.GetError().GetCode() != response2->Record.GetError().GetCode());
    }

    Y_UNIT_TEST(ShouldIncrementXAttrVersionOnUpdate)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.SetNodeXAttr(id, "user.name1", "value1");

        ui64 initialVersion;
        {
            auto response = tablet.GetNodeXAttr(id, "user.name1");
            initialVersion = response->Record.GetVersion();
        }

        tablet.SetNodeXAttr(id, "user.name1", "value2");
        {
            auto response = tablet.GetNodeXAttr(id, "user.name1");
            UNIT_ASSERT(initialVersion < response->Record.GetVersion());
        }
    }

    Y_UNIT_TEST(ShouldNotStoreNodeXAttrsForInvalidNodes)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.AssertSetNodeXAttrFailed(id + 100, "user.name1", "value1");
        tablet.AssertGetNodeXAttrFailed(id + 100, "user.name1");
        tablet.AssertRemoveNodeXAttrFailed(id + 100, "user.name1");
        tablet.AssertListNodeXAttrFailed(id + 100);
    }

    Y_UNIT_TEST(ShouldNotStoreXAttrsWithFlagsRestrictions)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.SetNodeXAttr(id, "user.name1", "value1");
        tablet.AssertSetNodeXAttrFailed(id, "user.name1", "value1", NProto::TSetNodeXAttrRequest::F_CREATE);
        tablet.AssertSetNodeXAttrFailed(id, "user.name2", "value2", NProto::TSetNodeXAttrRequest::F_REPLACE);
    }

    Y_UNIT_TEST(ShouldValidateXAttrNameAndValue)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        tablet.AssertSetNodeXAttrFailed(id, "invalid_name_without_namespace", "value1");
        tablet.AssertSetNodeXAttrFailed(id, "invalid_namespace.name", "value1");

        TStringBuilder longStr;
        for (int i = 0; i < 256; ++i) {
            longStr << "a";
        }
        tablet.AssertSetNodeXAttrFailed(id, "user." + longStr, "value");

        for (int i = 0; i < 16 * 1024 * 1024; ++i) {
            longStr << "vvvv";
        }
        tablet.AssertSetNodeXAttrFailed(id, "user.name", longStr);
    }

    Y_UNIT_TEST(ShouldPayRespectToInodeLimits)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId, {
            .NodeCount = 3
        });
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));
        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test2"));
        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test3"));

        // no more nodes
        auto response = tablet.AssertCreateNodeFailed(TCreateNodeArgs::File(RootNodeId, "test4"));
        auto error = STATUS_FROM_CODE(response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(error, (ui32)NProto::E_FS_NOSPC);

        response = tablet.AssertCreateNodeFailed(TCreateNodeArgs::Directory(RootNodeId, "test4"));
        error = STATUS_FROM_CODE(response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(error, (ui32)NProto::E_FS_NOSPC);

        response = tablet.AssertCreateNodeFailed(TCreateNodeArgs::SymLink(RootNodeId, "test4", "xxx"));
        error = STATUS_FROM_CODE(response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(error, (ui32)NProto::E_FS_NOSPC);

        response = tablet.AssertCreateNodeFailed(TCreateNodeArgs::Link(RootNodeId, "test4", id));
        error = STATUS_FROM_CODE(response->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(error, (ui32)NProto::E_FS_NOSPC);

        // you can still open existing files
        CreateHandle(tablet, id);

        // but not create ones
        auto handle = tablet.AssertCreateHandleFailed(RootNodeId, "test", TCreateHandleArgs::CREATE);
        error = STATUS_FROM_CODE(handle->GetError().GetCode());
        UNIT_ASSERT_VALUES_EQUAL(error, (ui32)NProto::E_FS_NOSPC);
    }

    Y_UNIT_TEST(ShouldTrackUsedBlocks)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        constexpr ui64 maxBlocks = 64;
        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId, {
            .BlockCount = maxBlocks
        });
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test1"));

        tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(4_KB));
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), 1);

        tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(maxBlocks * DefaultBlockSize));
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), maxBlocks);

        auto response = tablet.AssertSetNodeAttrFailed(
            TSetNodeAttrArgs(id).SetSize(maxBlocks * DefaultBlockSize + 1));

        auto error = response->GetError();
        UNIT_ASSERT_VALUES_EQUAL(STATUS_FROM_CODE(error.GetCode()), (ui32)NProto::E_FS_NOSPC);

        tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(4_KB));
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), 1);

        tablet.SetNodeAttr(TSetNodeAttrArgs(id).SetSize(0));
        UNIT_ASSERT_VALUES_EQUAL(GetStorageStats(tablet).GetUsedBlocksCount(), 0);
    }

    Y_UNIT_TEST(ShouldDeduplicateCreateNodeRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto createRequest = [&] (ui64 reqId) {
            auto request = tablet.CreateCreateNodeRequest(TCreateNodeArgs::File(RootNodeId, "xxx"));
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return std::move(request);
        };

        ui64 nodeId = InvalidNodeId;

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT(!HasError(response->Record.GetError()));

            nodeId = response->Record.GetNode().GetId();
            UNIT_ASSERT(nodeId != InvalidNodeId);
        }

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(nodeId, response->Record.GetNode().GetId());
        }

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL(nodeId, response->Record.GetNode().GetId());
        }

        tablet.SendRequest(createRequest(100501));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT(HasError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldDeduplicateRenameNodeRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "xxx"));

        auto createRequest = [&] (ui64 reqId) {
            auto request = tablet.CreateRenameNodeRequest(RootNodeId, "xxx", RootNodeId, "yyy");
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return std::move(request);
        };

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT(!HasError(response->Record.GetError()));
        }

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
        }

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
        }


        tablet.SendRequest(createRequest(100501));
        {
            auto response = tablet.RecvRenameNodeResponse();
            UNIT_ASSERT(HasError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldDeduplicateUnlinkNodeRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "xxx"));

        auto createRequest = [&] (ui64 reqId) {
            auto request = tablet.CreateUnlinkNodeRequest(RootNodeId, "xxx", false);
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return std::move(request);
        };

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT(!HasError(response->Record.GetError()));
        }

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
        }

        tablet.RebootTablet();
        tablet.InitSession("client", "session");

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT(!HasError(response->GetError()));
        }

        tablet.SendRequest(createRequest(100501));
        {
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT(HasError(response->GetError()));
        }
    }

    Y_UNIT_TEST(ShouldValidateDupRequests)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto createRequest = [&] (ui64 reqId) {
            auto request = tablet.CreateCreateNodeRequest(TCreateNodeArgs::File(RootNodeId, "xxx"));
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return request;
        };

        auto createOther = [&] (ui64 reqId) {
            auto request = tablet.CreateUnlinkNodeRequest(RootNodeId, "xxx", false);
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return request;
        };

        ui64 nodeId = InvalidNodeId;
        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT(!HasError(response->Record.GetError()));

            nodeId = response->Record.GetNode().GetId();
            UNIT_ASSERT_VALUES_UNEQUAL(InvalidNodeId, nodeId);
        }

        {
            auto response = tablet.ListNodes(RootNodeId);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(1, names.size());
            UNIT_ASSERT_VALUES_EQUAL("xxx", names[0]);
        }

        tablet.SendRequest(createOther(100500));
        {
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());
        }

        {
            auto response = tablet.ListNodes(RootNodeId);
            const auto& names = response->Record.GetNames();
            UNIT_ASSERT_VALUES_EQUAL(0, names.size());
        }
    }

    Y_UNIT_TEST(ShouldWaitForDupRequestToBeCommitted)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto createRequest = [&] (ui64 reqId) {
            auto request = tablet.CreateCreateNodeRequest(TCreateNodeArgs::File(RootNodeId, "xxx"));
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return std::move(request);
        };

        bool putObserved = false;
        auto& runtime = env.GetRuntime();
        runtime.SetObserverFunc([&] (TAutoPtr<IEventHandle>& event) {
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut: {
                        putObserved = true;
                        return TTestActorRuntime::EEventAction::DROP;
                    }
                }

                return TTestActorRuntime::DefaultObserverFunc(event);
            }
        );

        tablet.SendRequest(createRequest(100500));

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(1));
        UNIT_ASSERT(putObserved);

        tablet.SendRequest(createRequest(100500));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT(HasError(response->GetError()));
            UNIT_ASSERT_VALUES_EQUAL((ui32)E_REJECTED, response->Record.GetError().GetCode());
        }
    }

    Y_UNIT_TEST(ShouldUseNodeIndexCacheForGetNodeAttr)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNodeIndexCacheMaxNodes(32);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        auto handle = CreateHandle(tablet, id);

        tablet.WriteData(handle, 0, 1, '1');
        UNIT_ASSERT_VALUES_EQUAL(
            1,
            tablet.GetNodeAttr(RootNodeId, "test")->Record.GetNode().GetSize());

        tablet.WriteData(handle, 1, 1024, '2');
        UNIT_ASSERT_VALUES_EQUAL(
            1025,
            tablet.GetNodeAttr(RootNodeId, "test")->Record.GetNode().GetSize());
        UNIT_ASSERT_VALUES_EQUAL(
            1025,
            tablet.GetNodeAttr(RootNodeId, "test")->Record.GetNode().GetSize());
        UNIT_ASSERT_VALUES_EQUAL(
            1025,
            tablet.GetNodeAttr(RootNodeId, "test")->Record.GetNode().GetSize());

        tablet.SendRequest(tablet.CreateUpdateCounters());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        {
            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            // clang-format off
            visitor.ValidateExpectedCounters({
                {{{"filesystem", "test"}, {"sensor", "NodeIndexCacheHitCount"}}, 2},
                {{{"filesystem", "test"}, {"sensor", "NodeIndexCacheNodeCount"}}, 1},
            });
            // clang-format on
        }
    }

// See #2737 for more details
#define INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL(name, createHandle, ...)       \
    Y_UNIT_TEST(IndexCacheShouldProperlyHandleConcurrentModifying##name)       \
    {                                                                          \
        NProto::TStorageConfig storageConfig;                                  \
        storageConfig.SetNodeIndexCacheMaxNodes(32);                           \
        TTestEnv env({}, storageConfig);                                       \
        env.CreateSubDomain("nfs");                                            \
        auto registry = env.GetRegistry();                                     \
                                                                               \
        ui32 nodeIdx = env.CreateNode("nfs");                                  \
        ui64 tabletId = env.BootIndexTablet(nodeIdx);                          \
                                                                               \
        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);        \
        tablet.InitSession("client", "session");                               \
                                                                               \
        auto id =                                                              \
            CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));     \
        auto handle = InvalidHandle;                                           \
        if (createHandle) {                                                    \
            handle = CreateHandle(tablet, id);                                 \
            /* Just write some data to the file to make it non-empty */        \
            tablet.WriteData(handle, 0, 16, '0');                              \
        }                                                                      \
                                                                               \
        auto& runtime = env.GetRuntime();                                      \
                                                                               \
        TAutoPtr<IEventHandle> rwTxPutRequest;                                 \
                                                                               \
        runtime.SetEventFilter(                                                \
            [&](auto& runtime, auto& event)                                    \
            {                                                                  \
                Y_UNUSED(runtime);                                             \
                switch (event->GetTypeRewrite()) {                             \
                    case TEvBlobStorage::EvPut:                                \
                        if (!rwTxPutRequest) {                                 \
                            rwTxPutRequest = std::move(event);                 \
                            return true;                                       \
                        }                                                      \
                }                                                              \
                return false;                                                  \
            });                                                                \
                                                                               \
        /* Execute stage of RW tx will produce a TEvPut request, which is      \
           dropped to postpone the completion of the transaction */            \
        tablet.SendSetNodeAttrRequest(TSetNodeAttrArgs(RootNodeId).SetUid(2)); \
                                                                               \
        runtime.DispatchEvents(TDispatchOptions{                               \
            .CustomFinalCondition = [&]()                                      \
            {                                                                  \
                return rwTxPutRequest != nullptr;                              \
            }});                                                               \
                                                                               \
        /* Now the GetNodeAttr operation is supposed to start a new            \
           transaction and hang because it accesses the same data as the       \
           previous one. This operation has a potential to populate the        \
           node attributes cache */                                            \
        tablet.SendGetNodeAttrRequest(RootNodeId, "test");                     \
                                                                               \
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));     \
                                                                               \
        /* Ensure that both operations are still in progress */                \
        tablet.AssertSetNodeAttrNoResponse();                                  \
        tablet.AssertGetNodeAttrNoResponse();                                  \
                                                                               \
        /* However, Prepare stages are already completed, and GetNodeAttr has  \
           stale data in its structure */                                      \
                                                                               \
        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);      \
                                                                               \
        /* Now let's start the new transaction that will update attributes of  \
           the node. It is also expected to hang */                            \
        tablet.Send##name##Request(__VA_ARGS__);                               \
                                                                               \
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(2));     \
                                                                               \
        /* Let us complete the initial transaction that will release the lock  \
           and let the GetNodeAttr complete */                                 \
        runtime.Send(rwTxPutRequest.Release(), nodeIdx);                       \
                                                                               \
        /* Now the initial SetNodeAttr should complete */                      \
        {                                                                      \
            auto response = tablet.RecvSetNodeAttrResponse();                  \
            UNIT_ASSERT(!HasError(response->GetError()));                      \
        }                                                                      \
                                                                               \
        /* The GetNodeAttr should also complete with initial data */           \
        {                                                                      \
            auto response = tablet.RecvGetNodeAttrResponse();                  \
            UNIT_ASSERT(!HasError(response->GetError()));                      \
            UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetNode().GetUid());  \
        }                                                                      \
                                                                               \
        /* Now the sent modifying operation is expected to complete */         \
        {                                                                      \
            auto response = tablet.Recv##name##Response();                     \
            UNIT_ASSERT(!HasError(response->GetError()));                      \
        }                                                                      \
                                                                               \
        /* Now let us ensure that GetNodeAttr using node id and using          \
           parent + name are consistent */                                     \
        {                                                                      \
            auto response1 = tablet.SendAndRecvGetNodeAttr(id);                \
            auto response2 =                                                   \
                tablet.SendAndRecvGetNodeAttr(RootNodeId, "test");             \
            if (HasError(response1->GetError())) {                             \
                UNIT_ASSERT(HasError(response2->GetError()));                  \
            } else {                                                           \
                UNIT_ASSERT(!HasError(response2->GetError()));                 \
                UNIT_ASSERT_VALUES_EQUAL(                                      \
                    response1->Record.DebugString(),                           \
                    response2->Record.DebugString());                          \
            }                                                                  \
        }                                                                      \
    }
    // INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL

    // IndexCacheShouldProperlyHandleConcurrentModifyingSetNodeAttr
    INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL(
        SetNodeAttr,
        false,
        TSetNodeAttrArgs(id).SetUid(1))

    // IndexCacheShouldProperlyHandleConcurrentModifyingWriteData
    INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL(WriteData, true, handle, 0, 1, '1')

    // IndexCacheShouldProperlyHandleConcurrentModifyingCreateNode
    INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL(
        CreateNode,
        true,
        TCreateNodeArgs::Link(RootNodeId, "test2", id))

    // IndexCacheShouldProperlyHandleConcurrentModifyingAllocateData
    INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL(
        AllocateData,
        true,
        handle,
        0,
        1024,
        ProtoFlag(NProto::TAllocateDataRequest::F_ZERO_RANGE))

    // IndexCacheShouldProperlyHandleConcurrentModifyingCreateHandle
    INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL(
        CreateHandle,
        true,
        id,
        TCreateHandleArgs::TRUNC | TCreateHandleArgs::RDWR)

    // IndexCacheShouldProperlyHandleConcurrentModifyingUnlinkNode
    INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL(
        UnlinkNode,
        false,
        RootNodeId,
        "test",
        false)

#undef INDEX_CACHE_CONSISTENCY_CACHE_TEST_IMPL

    Y_UNIT_TEST(ShouldStatOpenedFiles)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNodeIndexCacheMaxNodes(32);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

#define COUNTERS_VALIDATE_WS_WM_RS_RM(wo, wm, ro, rm)               \
    {                                                               \
        tablet.SendRequest(tablet.CreateUpdateCounters());          \
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1)); \
        TTestRegistryVisitor visitor;                               \
        registry->Visit(TInstant::Zero(), visitor);                 \
        visitor.ValidateExpectedCounters({                          \
            {{{"filesystem", "test"},                               \
              {"sensor", "NodesOpenForWritingBySingleSession"}},    \
             wo},                                                   \
            {{{"filesystem", "test"},                               \
              {"sensor", "NodesOpenForWritingByMultipleSessions"}}, \
             wm},                                                   \
            {{{"filesystem", "test"},                               \
              {"sensor", "NodesOpenForReadingBySingleSession"}},    \
             ro},                                                   \
            {{{"filesystem", "test"},                               \
              {"sensor", "NodesOpenForReadingByMultipleSessions"}}, \
             rm},                                                   \
        });                                                         \
    }

        auto id = CreateNode(tablet, TCreateNodeArgs::File(RootNodeId, "test"));
        {
            auto handleS1W = CreateHandle(tablet, id);

            tablet.WriteData(handleS1W, 0, 1, '1');
            tablet.WriteData(handleS1W, 1, 1024, '2');

            {
                auto handleS1R =
                    CreateHandle(tablet, id, {}, TCreateHandleArgs::RDNLY);
                tablet.ReadData(handleS1R, 10, 10);
                COUNTERS_VALIDATE_WS_WM_RS_RM(1, 0, 0, 0);
                tablet.DestroyHandle(handleS1R);
            }

            COUNTERS_VALIDATE_WS_WM_RS_RM(1, 0, 0, 0);
            tablet.DestroyHandle(handleS1W);

            {
                auto handleS1R =
                    CreateHandle(tablet, id, {}, TCreateHandleArgs::RDNLY);
                tablet.ReadData(handleS1R, 10, 10);
                COUNTERS_VALIDATE_WS_WM_RS_RM(0, 0, 1, 0);

                auto handleS1W = CreateHandle(tablet, id);
                COUNTERS_VALIDATE_WS_WM_RS_RM(1, 0, 0, 0);

                {
                    TIndexTabletClient tablet2(
                        env.GetRuntime(),
                        nodeIdx,
                        tabletId);
                    tablet2.InitSession("client2", "session2");
                    auto handleS2R =
                        CreateHandle(tablet2, id, {}, TCreateHandleArgs::RDNLY);
                    COUNTERS_VALIDATE_WS_WM_RS_RM(1, 0, 0, 0);

                    tablet2.DestroyHandle(handleS2R);
                    auto handleS2W = CreateHandle(tablet2, id, {});
                    COUNTERS_VALIDATE_WS_WM_RS_RM(0, 1, 0, 0);
                    tablet2.DestroyHandle(handleS2W);
                }

                tablet.DestroyHandle(handleS1W);
                COUNTERS_VALIDATE_WS_WM_RS_RM(0, 0, 1, 0);

                {
                    auto node2 = CreateNode(
                        tablet,
                        TCreateNodeArgs::File(RootNodeId, "test2"));
                    auto handleS1W2 = CreateHandle(tablet, node2);
                    tablet.WriteData(handleS1W2, 0, 1, '1');
                    COUNTERS_VALIDATE_WS_WM_RS_RM(1, 0, 1, 0);
                    tablet.DestroyHandle(handleS1W2);

                    {
                        auto handleS1R = CreateHandle(
                            tablet,
                            node2,
                            {},
                            TCreateHandleArgs::RDNLY);
                        COUNTERS_VALIDATE_WS_WM_RS_RM(0, 0, 2, 0);
                        tablet.DestroyHandle(handleS1R);
                    }
                }

                {
                    TIndexTabletClient tablet2(
                        env.GetRuntime(),
                        nodeIdx,
                        tabletId);
                    tablet2.InitSession("client2", "session2");
                    auto handleS2R =
                        CreateHandle(tablet2, id, {}, TCreateHandleArgs::RDNLY);
                    COUNTERS_VALIDATE_WS_WM_RS_RM(0, 0, 0, 1);
                    tablet2.DestroyHandle(handleS2R);
                }

                tablet.DestroyHandle(handleS1R);
            }
        }

        COUNTERS_VALIDATE_WS_WM_RS_RM(0, 0, 0, 0);

        {
            auto handleS1R =
                CreateHandle(tablet, id, {}, TCreateHandleArgs::RDNLY);
            tablet.ReadData(handleS1R, 10, 10);
            COUNTERS_VALIDATE_WS_WM_RS_RM(0, 0, 1, 0);
            tablet.RebootTablet();
            tablet.RecoverSession();
            COUNTERS_VALIDATE_WS_WM_RS_RM(0, 0, 1, 0);
            tablet.DestroyHandle(handleS1R);
            COUNTERS_VALIDATE_WS_WM_RS_RM(0, 0, 0, 0);
        }

#undef COUNTERS_VALIDATE_WS_WM_RS_RM
    }

    Y_UNIT_TEST(ShouldInvalidateNodeIndexCacheUponIndexOps)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNodeIndexCacheMaxNodes(32);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto dir =
            CreateNode(tablet, TCreateNodeArgs::Directory(RootNodeId, "dir"));
        auto file = CreateNode(tablet, TCreateNodeArgs::File(dir, "file"));

        auto handle = CreateHandle(tablet, file);
        tablet.WriteData(handle, 0, 1024, '1');

        tablet.GetNodeAttr(RootNodeId, "dir");
        tablet.GetNodeAttr(dir, "file");

        // UpdateNodeAttr
        TSetNodeAttrArgs arg(file);
        arg.SetMode(123);
        tablet.SetNodeAttr(arg);
        UNIT_ASSERT_VALUES_EQUAL(
            123,
            tablet.GetNodeAttr(file)->Record.GetNode().GetMode());

        // UnlinkNode (open handle exists)
        tablet.UnlinkNode(dir, "file", false);
        file = CreateNode(tablet, TCreateNodeArgs::File(dir, "file"));
        tablet.GetNodeAttr(dir, "file");

        // RemoveNode (no open handles)
        tablet.UnlinkNode(dir, "file", false);
        file = CreateNode(tablet, TCreateNodeArgs::File(dir, "file"));

        // RenameNode
        tablet.GetNodeAttr(dir, "file");

        tablet.RenameNode(dir, "file", dir, "file2");
        tablet.AssertGetNodeAttrFailed(dir, "file");

        UNIT_ASSERT_VALUES_EQUAL(
            file,
            tablet.GetNodeAttr(dir, "file2")->Record.GetNode().GetId());

        {
            tablet.SendRequest(tablet.CreateUpdateCounters());
            env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            // clang-format off
            visitor.ValidateExpectedCounters({
                {{{"filesystem", "test"}, {"sensor", "NodeIndexCacheHitCount"}}, 0},
                {{{"filesystem", "test"}, {"sensor", "NodeIndexCacheNodeCount"}}, 1},
            });
            // clang-format on
        }
    }

    Y_UNIT_TEST(ShouldGetNodeAttrBatch)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(
            tablet,
            TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 = CreateNode(
            tablet,
            TCreateNodeArgs::File(RootNodeId, "test2"));

        // testing successful response
        {
            const TVector<TString> names = {"test1", "test2"};
            auto response = tablet.GetNodeAttrBatch(RootNodeId, names);
            const auto nodeResponses = response->Record.GetResponses();
            UNIT_ASSERT_VALUES_EQUAL(2, nodeResponses.size());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                nodeResponses[0].GetError().GetCode(),
                nodeResponses[0].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(id1, nodeResponses[0].GetNode().GetId());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                nodeResponses[1].GetError().GetCode(),
                nodeResponses[1].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(id2, nodeResponses[1].GetNode().GetId());
        }

        // testing the case when one of the requested names doesn't exist
        {
            const TVector<TString> names = {"test1", "test3"};
            auto response = tablet.GetNodeAttrBatch(RootNodeId, names);
            const auto nodeResponses = response->Record.GetResponses();
            UNIT_ASSERT_VALUES_EQUAL(2, nodeResponses.size());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                nodeResponses[0].GetError().GetCode(),
                nodeResponses[0].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(id1, nodeResponses[0].GetNode().GetId());
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                nodeResponses[1].GetError().GetCode(),
                nodeResponses[1].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(
                InvalidNodeId,
                nodeResponses[1].GetNode().GetId());
        }

        // parent missing
        {
            const TVector<TString> names = {"test1", "test3"};
            const ui64 someOtherNodeId = 777;
            tablet.SendGetNodeAttrBatchRequest(someOtherNodeId, names);
            auto response = tablet.RecvGetNodeAttrBatchResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                response->GetStatus(),
                response->GetErrorReason());
            const auto nodeResponses = response->Record.GetResponses();
            UNIT_ASSERT_VALUES_EQUAL(0, nodeResponses.size());
        }

        // invalid parameters
        {
            const TVector<TString> names = {"test1", "///"};
            tablet.SendGetNodeAttrBatchRequest(RootNodeId, names);
            auto response = tablet.RecvGetNodeAttrBatchResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_INVAL,
                response->GetStatus(),
                response->GetErrorReason());
            const auto nodeResponses = response->Record.GetResponses();
            UNIT_ASSERT_VALUES_EQUAL(0, nodeResponses.size());
        }
    }

    Y_UNIT_TEST(ShouldGetNodeAttrBatchWithCache)
    {
        NProto::TStorageConfig storageConfig;
        storageConfig.SetNodeIndexCacheMaxNodes(32);
        TTestEnv env({}, storageConfig);
        env.CreateSubDomain("nfs");
        auto registry = env.GetRegistry();

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto id1 = CreateNode(
            tablet,
            TCreateNodeArgs::File(RootNodeId, "test1"));
        auto id2 = CreateNode(
            tablet,
            TCreateNodeArgs::File(RootNodeId, "test2"));

        // no cache
        {
            const TVector<TString> names = {"test1"};
            auto response = tablet.GetNodeAttrBatch(RootNodeId, names);
            const auto nodeResponses = response->Record.GetResponses();
            UNIT_ASSERT_VALUES_EQUAL(1, nodeResponses.size());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                nodeResponses[0].GetError().GetCode(),
                nodeResponses[0].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(id1, nodeResponses[0].GetNode().GetId());
        }

        // partial cache
        {
            const TVector<TString> names = {"test1", "test2", "test3"};
            auto response = tablet.GetNodeAttrBatch(RootNodeId, names);
            const auto nodeResponses = response->Record.GetResponses();
            UNIT_ASSERT_VALUES_EQUAL(3, nodeResponses.size());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                nodeResponses[0].GetError().GetCode(),
                nodeResponses[0].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(id1, nodeResponses[0].GetNode().GetId());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                nodeResponses[1].GetError().GetCode(),
                nodeResponses[1].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(
                id2,
                nodeResponses[1].GetNode().GetId());
            UNIT_ASSERT_VALUES_EQUAL_C(
                E_FS_NOENT,
                nodeResponses[2].GetError().GetCode(),
                nodeResponses[2].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(
                InvalidNodeId,
                nodeResponses[2].GetNode().GetId());
        }

        // everything from cache
        {
            const TVector<TString> names = {"test1", "test2"};
            auto response = tablet.GetNodeAttrBatch(RootNodeId, names);
            const auto nodeResponses = response->Record.GetResponses();
            UNIT_ASSERT_VALUES_EQUAL(2, nodeResponses.size());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                nodeResponses[0].GetError().GetCode(),
                nodeResponses[0].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(id1, nodeResponses[0].GetNode().GetId());
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                nodeResponses[1].GetError().GetCode(),
                nodeResponses[1].GetError().GetMessage());
            UNIT_ASSERT_VALUES_EQUAL(
                id2,
                nodeResponses[1].GetNode().GetId());
        }

        tablet.SendRequest(tablet.CreateUpdateCounters());
        env.GetRuntime().DispatchEvents({}, TDuration::Seconds(1));

        {
            TTestRegistryVisitor visitor;
            registry->Visit(TInstant::Zero(), visitor);
            visitor.ValidateExpectedCounters({
                {{
                    {"filesystem", "test"},
                    {"sensor", "NodeIndexCacheHitCount"}
                }, 3},
                {{
                    {"filesystem", "test"},
                    {"sensor", "NodeIndexCacheNodeCount"}
                }, 2},
            });
        }
    }

    Y_UNIT_TEST(ShouldNotGenerateDeletionMarkersUponSymLinkRemoval)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        CreateNode(
            tablet,
            TCreateNodeArgs::SymLink(RootNodeId, "l", "target/path"));
        tablet.UnlinkNode(RootNodeId, "l", false);

        {
            auto response = tablet.GetStorageStats();
            const auto& stats = response->Record.GetStats();
            UNIT_ASSERT_VALUES_EQUAL(0, stats.GetDeletionMarkersCount());
        }
    }

    Y_UNIT_TEST(ShouldIdentifyRequestIdCollision)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto createCreateHandleRequest = [&] (ui64 reqId, ui64 nodeId) {
            auto request = tablet.CreateCreateHandleRequest(
                nodeId, TCreateHandleArgs::RDWR);
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return request;
        };

        auto createCreateNodeRequest = [&] (ui64 reqId, TString name) {
            auto request = tablet.CreateCreateNodeRequest(
                TCreateNodeArgs::File(RootNodeId, std::move(name)));
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return request;
        };

        const TString name1 = "file1";
        const TString name2 = "file2";
        const TString name3 = "file3";

        const auto nodeId1 = tablet.CreateNode(TCreateNodeArgs::File(
            RootNodeId,
            name1))->Record.GetNode().GetId();
        const auto nodeId2 = tablet.CreateNode(TCreateNodeArgs::File(
            RootNodeId,
            name2))->Record.GetNode().GetId();

        UNIT_ASSERT_VALUES_UNEQUAL(InvalidNodeId, nodeId1);
        UNIT_ASSERT_VALUES_UNEQUAL(InvalidNodeId, nodeId2);
        UNIT_ASSERT_VALUES_UNEQUAL(nodeId1, nodeId2);

        const ui64 requestId = 100500;

        tablet.SendRequest(createCreateHandleRequest(requestId, nodeId1));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(
                nodeId1,
                response->Record.GetNodeAttr().GetId());
        }

        tablet.SendRequest(createCreateHandleRequest(requestId, nodeId2));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(
                nodeId2,
                response->Record.GetNodeAttr().GetId());
        }

        tablet.SendRequest(createCreateNodeRequest(requestId, name3));
        {
            auto response = tablet.RecvCreateNodeResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());

            UNIT_ASSERT_VALUES_UNEQUAL(
                nodeId1,
                response->Record.GetNode().GetId());
            UNIT_ASSERT_VALUES_UNEQUAL(
                nodeId2,
                response->Record.GetNode().GetId());
            UNIT_ASSERT_VALUES_UNEQUAL(0, response->Record.GetNode().GetId());
        }
    }

    Y_UNIT_TEST(ShouldIdentifyStaleHandlesInDupCache)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        auto createCreateHandleRequest = [&] (ui64 reqId, ui64 nodeId) {
            auto request = tablet.CreateCreateHandleRequest(
                nodeId, TCreateHandleArgs::RDWR);
            request->Record.MutableHeaders()->SetRequestId(reqId);

            return request;
        };

        const TString name = "file";

        const auto nodeId = tablet.CreateNode(TCreateNodeArgs::File(
            RootNodeId,
            name))->Record.GetNode().GetId();

        const ui64 requestId = 100500;
        ui64 handle = 0;

        tablet.SendRequest(createCreateHandleRequest(requestId, nodeId));
        {
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(
                nodeId,
                response->Record.GetNodeAttr().GetId());

            handle = response->Record.GetHandle();
            tablet.DescribeData(handle, 0, 1_KB);
            tablet.DestroyHandle(handle);
        }

        // opening the same file again using the same requestId
        tablet.SendRequest(createCreateHandleRequest(requestId, nodeId));
        {
            // DupCache shouldn't be used this time
            auto response = tablet.RecvCreateHandleResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->Record.GetError().GetCode(),
                response->Record.GetError().GetMessage());

            UNIT_ASSERT_VALUES_EQUAL(
                nodeId,
                response->Record.GetNodeAttr().GetId());

            const auto handle2 = response->Record.GetHandle();
            UNIT_ASSERT_VALUES_UNEQUAL(handle, handle2);

            // DescribeData should succeed
            tablet.DescribeData(handle2, 0, 1_KB);
            tablet.DestroyHandle(handle2);
        }
    }

    // This test enforces the fact that if some data has been modified by a RW
    // transaction, but it has not been completed yet, that will not be visible
    // to other transactions.
    Y_UNIT_TEST(ShouldNotReadPhantomData)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "file"));
        tablet.SetNodeAttr(TSetNodeAttrArgs(RootNodeId).SetUid(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            tablet.GetNodeAttr(RootNodeId)->Record.GetNode().GetUid());

        TAutoPtr<IEventHandle> putEvent;
        auto& runtime = env.GetRuntime();
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut:
                        if (!putEvent) {
                            putEvent = std::move(event);
                            return true;
                        }
                }
                return false;
            });

        tablet.SendSetNodeAttrRequest(TSetNodeAttrArgs(RootNodeId).SetUid(2));
        // Execute stage of RW tx will produce a TEvPut request, which is
        // dropped to postpone the completion of the transaction
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return putEvent != nullptr;
            }});

        // Ensure that if the Execute stage of RW tx has written some data, RO
        // tx will not complete until the RW tx is completed

        // Thus, the following order is to be observed:
        //
        // RW Prepare
        // RW Execute
        // RO Prepare
        // RO Execute
        // * RO tx can wait indefinitely
        // RW Complete
        // RO Complete

        tablet.SendGetNodeAttrRequest(RootNodeId);

        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(10));

        tablet.AssertSetNodeAttrNoResponse();
        tablet.AssertGetNodeAttrNoResponse();

        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);

        runtime.Send(putEvent.Release(), nodeIdx);
        {
            auto response = tablet.RecvSetNodeAttrResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetStatus());
        }

        auto response = tablet.RecvGetNodeAttrResponse();
        UNIT_ASSERT_VALUES_EQUAL_C(
            S_OK,
            response->GetStatus(),
            response->GetStatus());

        UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetNode().GetUid());
    }

    Y_UNIT_TEST(DataObservedByROTxUponCompletionShouldNeverBeStale)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");

        tablet.CreateNode(TCreateNodeArgs::File(RootNodeId, "file"));
        tablet.SetNodeAttr(TSetNodeAttrArgs(RootNodeId).SetUid(1));

        UNIT_ASSERT_VALUES_EQUAL(
            1,
            tablet.GetNodeAttr(RootNodeId)->Record.GetNode().GetUid());

        TAutoPtr<IEventHandle> putEvent;
        auto& runtime = env.GetRuntime();

        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvBlobStorage::EvPut:
                        putEvent = std::move(event);
                        return true;
                }
                return false;
            });

        // Write(2)
        tablet.SendSetNodeAttrRequest(TSetNodeAttrArgs(RootNodeId).SetUid(2));
        runtime.DispatchEvents(TDispatchOptions{
            .CustomFinalCondition = [&]()
            {
                return putEvent != nullptr;
            }});
        // Write should hang

        // Read
        runtime.SetEventFilter(TTestActorRuntimeBase::DefaultFilterFunc);
        tablet.SendGetNodeAttrRequest(RootNodeId);
        // Read should hang
        runtime.DispatchEvents(TDispatchOptions(), TDuration::Seconds(10));
        tablet.AssertSetNodeAttrNoResponse();
        tablet.AssertGetNodeAttrNoResponse();

        // Write(3)
        tablet.SendSetNodeAttrRequest(TSetNodeAttrArgs(RootNodeId).SetUid(3));

        TString observedOrder = "";
        runtime.SetEventFilter(
            [&](auto& runtime, auto& event)
            {
                Y_UNUSED(runtime);
                switch (event->GetTypeRewrite()) {
                    case TEvService::EvSetNodeAttrResponse: {
                        using TResponse = TEvService::TEvSetNodeAttrResponse;
                        auto* msg = event->template Get<TResponse>();
                        observedOrder +=
                            "W" + ToString(msg->Record.GetNode().GetUid());
                        return false;
                    }
                    case TEvService::EvGetNodeAttrResponse: {
                        using TResponse = TEvService::TEvGetNodeAttrResponse;
                        auto* msg = event->template Get<TResponse>();
                        observedOrder +=
                            "R" + ToString(msg->Record.GetNode().GetUid());
                        return false;
                    }
                }
                return false;
            });

        runtime.Send(putEvent.Release(), nodeIdx);
        {
            auto response = tablet.RecvSetNodeAttrResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetStatus());
        }
        {
            auto response = tablet.RecvGetNodeAttrResponse();
            UNIT_ASSERT_VALUES_EQUAL_C(
                S_OK,
                response->GetStatus(),
                response->GetStatus());
            UNIT_ASSERT_VALUES_EQUAL_C(
                2,
                response->Record.GetNode().GetUid(),
                response->Record.DebugString());
        }

        UNIT_ASSERT_VALUES_EQUAL("W2R2W3", observedOrder);
    }

    Y_UNIT_TEST(ShouldReturnDataFromReadNodeRefs)
    {
        TTestEnv env;
        env.CreateSubDomain("nfs");

        ui32 nodeIdx = env.CreateNode("nfs");
        ui64 tabletId = env.BootIndexTablet(nodeIdx);

        TIndexTabletClient tablet(env.GetRuntime(), nodeIdx, tabletId);
        tablet.InitSession("client", "session");
        auto id1 = CreateNode(
            tablet,
            TCreateNodeArgs::Directory(RootNodeId, "test1"));
        auto id2 = CreateNode(
            tablet,
            TCreateNodeArgs::Directory(RootNodeId, "test2"));

        CreateNode(
            tablet,
            TCreateNodeArgs::File(id1, "test3"));
        CreateNode(
            tablet,
            TCreateNodeArgs::File(id1, "test4"));
        CreateNode(
            tablet,
            TCreateNodeArgs::File(id2, "test5"));

        auto response = tablet.ReadNodeRefs(0, "", 1);
        UNIT_ASSERT_VALUES_EQUAL(1, response->Record.GetNodeRefs().size());
        UNIT_ASSERT_VALUES_EQUAL(RootNodeId, response->Record.GetNextNodeId());
        UNIT_ASSERT_VALUES_EQUAL("test2", response->Record.GetNextCookie());

        response = tablet.ReadNodeRefs(response->Record.GetNextNodeId(),
                                            response->Record.GetNextCookie(), 2);
        UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetNodeRefs().size());
        UNIT_ASSERT_VALUES_EQUAL(id1, response->Record.GetNextNodeId());
        UNIT_ASSERT_VALUES_EQUAL("test4", response->Record.GetNextCookie());
        response = tablet.ReadNodeRefs(
            response->Record.GetNextNodeId(),
            response->Record.GetNextCookie(),
            10);
        UNIT_ASSERT_VALUES_EQUAL(2, response->Record.GetNodeRefs().size());
        UNIT_ASSERT_VALUES_EQUAL(0, response->Record.GetNextNodeId());
        UNIT_ASSERT_VALUES_EQUAL("", response->Record.GetNextCookie());
    }
}

}   // namespace NCloud::NFileStore::NStorage
