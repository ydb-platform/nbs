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

            return std::move(request);
        };

        auto createOther = [&] (ui64 reqId) {
            auto request = tablet.CreateUnlinkNodeRequest(RootNodeId, "xxx", false);
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

        tablet.SendRequest(createOther(100500));
        {
            auto response = tablet.RecvUnlinkNodeResponse();
            UNIT_ASSERT(HasError(response->Record.GetError()));
        }
    }

    Y_UNIT_TEST(ShouldWaitForDupRequestToBeCommited)
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
        UNIT_ASSERT_VALUES_EQUAL(
            file,
            tablet.GetNodeAttr(dir, "file")->Record.GetNode().GetId());

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
}

}   // namespace NCloud::NFileStore::NStorage
