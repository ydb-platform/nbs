#include <cloud/filestore/libs/service/filestore.h>
#include <cloud/filestore/libs/storage/fastshard/iface/fs.h>
#include <cloud/filestore/libs/storage/fastshard/impl/naive_mirrored/shard.h>
#include <cloud/filestore/libs/storage/fastshard/sn/impl/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/server/server.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>
#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <library/cpp/testing/common/network.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/logger.h>

#include <gtest/gtest.h>

#include <util/system/tempfile.h>

using namespace NCloud;
using namespace NFileStore;
using namespace NFileStore::NProto;
using namespace NStorage::NFastShard;
using silk::FiberScheduler;

////////////////////////////////////////////////////////////////////////////////
// This is deliberately not a unit test now. This test deliberately bootstraps
// the whole stack:
// * several storage nodes on top of files
// * storage node server
// * storage node clients
// * storage group
// * shard implementation on top of this group
//
// This could've been an integration pytest but that would require adding a
// storage node daemon as well and also it's less convenient to debug pytests
// than to debug gtests.

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

////////////////////////////////////////////////////////////////////////////////

constexpr ui32 ShardNo = 1;
constexpr size_t PageSize = 4_KB;
constexpr size_t PageCount = 128_MB / PageSize;
constexpr size_t FileSize = PageSize * PageCount;
constexpr size_t NodesPerGroup = 64;

////////////////////////////////////////////////////////////////////////////////
// Test fixture:
// * allocates a file
// * allocates a port
// * runs a storage node on top of that file
// * and a storage node server on top of that node

struct TStorageNodeFixture
{
    TTempFileHandle File;
    NTesting::TPortHolder Port;
    IStorageNodePtr Node;
    IServerPtr Server;

    TStorageNodeFixture()
        : File(TTempFileHandle::InCurrentDir("impl-naive_mirrored-ut"))
        , Port(NTesting::GetFreePort())
    {
        File.Resize(FileSize);
        File.Close();
        Node = CreateNaiveFileStorageNode(File.Name());
        Server = CreateServer(Port, Node);
        Server->Start();
    }

    ~TStorageNodeFixture()
    {
        Server->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////
// Test fixture:
// * creates N storage node fixtures
// * sets up shard config using the endpoints of those storage nodes

struct TStorageFixture
{
    static constexpr ui32 NodeCount = 3;
    TVector<TStorageNodeFixture> Nodes;
    NProtoPrivate::TPersistentFastShardConfig Config;

    TStorageFixture()
        : Nodes(NodeCount)
    {
        auto* sg = Config.AddStorageGroups();
        for (auto& node: Nodes) {
            auto* d = sg->AddDevices();
            d->SetHost("localhost");
            d->SetPort(node.Port);
            d->SetDeviceId("doesn't-matter");
        }

        Config.SetNodesPerGroup(NodesPerGroup);
        Config.SetExpectedGroupCapacity(FileSize / 2);
    }
};

////////////////////////////////////////////////////////////////////////////////

TString GenerateValidateData(ui32 size, ui32 seed = 0)
{
    TString data(size, 0);
    for (ui32 i = 0; i < size; ++i) {
        data[i] = 'A' + ((i + seed) % ('Z' - 'A' + 1));
    }
    return data;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveMirroredShardTest, CreatesFiles)
{
    silk::Logger::setLevel(silk::LogLevel::DEBUG);

    TStorageFixture fx;

    auto shard = CreateNaiveMirroredFileSystemShard(ShardNo, fx.Config);

    const TString file1 = "file1";
    const ui32 mode = 0644;
    const ui32 expectedMode = S_IFREG | 0644;
    const ui64 uid = 111;
    const ui64 gid = 222;

    ui64 nodeId = 0;
    {
        TCreateNodeRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        request.SetUid(uid);
        request.SetGid(gid);
        request.MutableFile()->SetMode(mode);
        auto f = shard->CreateNode(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        nodeId = response.GetNode().GetId();
    }

    {
        TGetNodeAttrRequest request;
        request.SetNodeId(nodeId);
        auto f = shard->GetNodeAttr(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_EQ(nodeId, response.GetNode().GetId());
        EXPECT_EQ(uid, response.GetNode().GetUid());
        EXPECT_EQ(gid, response.GetNode().GetGid());
        EXPECT_EQ(
            static_cast<ui32>(E_REGULAR_NODE),
            response.GetNode().GetType());
        EXPECT_EQ(expectedMode, response.GetNode().GetMode());
    }

    {
        TUnlinkNodeRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        auto f = shard->UnlinkNode(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }

    {
        TGetNodeAttrRequest request;
        request.SetNodeId(nodeId);
        auto f = shard->GetNodeAttr(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(NCloud::E_FS_NOENT, response.GetError().GetCode())
            << FormatError(response.GetError());
    }
}

TEST(NaiveMirroredShardTest, ValidatesRequests)
{
    silk::Logger::setLevel(silk::LogLevel::DEBUG);

    TStorageFixture fx;

    auto shard = CreateNaiveMirroredFileSystemShard(ShardNo, fx.Config);

    const ui32 mode = 0644;
    const ui32 expectedMode = S_IFREG | 0644;
    const ui64 uid = 111;
    const ui64 gid = 222;

    {
        TCreateNodeRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName("a-very-very-very-very-long-name-for-this-impl");
        request.SetUid(uid);
        request.SetGid(gid);
        request.MutableFile()->SetMode(mode);
        auto f = shard->CreateNode(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(NCloud::E_FS_NAMETOOLONG, response.GetError().GetCode())
            << FormatError(response.GetError());
    }

    ui64 nodeId = 0;
    {
        TCreateNodeRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName("a-normal-name");
        request.SetUid(uid);
        request.SetGid(gid);
        request.MutableFile()->SetMode(mode);
        auto f = shard->CreateNode(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        nodeId = response.GetNode().GetId();
    }

    {
        TGetNodeAttrRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName("a-normal-name");
        auto f = shard->GetNodeAttr(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_EQ(nodeId, response.GetNode().GetId());
        EXPECT_EQ(uid, response.GetNode().GetUid());
        EXPECT_EQ(gid, response.GetNode().GetGid());
        EXPECT_EQ(
            static_cast<ui32>(E_REGULAR_NODE),
            response.GetNode().GetType());
        EXPECT_EQ(expectedMode, response.GetNode().GetMode());
    }

    {
        TGetNodeAttrRequest request;
        request.SetNodeId(RootNodeId + 1);
        request.SetName("a-normal-name");
        auto f = shard->GetNodeAttr(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(NCloud::E_FS_NOENT, response.GetError().GetCode())
            << FormatError(response.GetError());
    }
}

TEST(NaiveMirroredShardTest, CreatesHandles)
{
    silk::Logger::setLevel(silk::LogLevel::DEBUG);

    TStorageFixture fx;

    auto shard = CreateNaiveMirroredFileSystemShard(ShardNo, fx.Config);

    const TString file1 = "file1";
    const ui32 mode = 0644;
    const ui32 expectedMode = S_IFREG | 0644;
    const ui64 uid = 111;
    const ui64 gid = 222;

    const ui32 create = ProtoFlag(TCreateHandleRequest::E_CREATE);
    const ui32 createExcl =
        create | ProtoFlag(TCreateHandleRequest::E_EXCLUSIVE);

    ui64 nodeId = 0;
    ui64 handle1 = 0;
    {
        TCreateHandleRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        request.SetMode(mode);
        request.SetUid(uid);
        request.SetGid(gid);
        request.SetFlags(createExcl);
        auto f = shard->CreateHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_EQ(uid, response.GetNodeAttr().GetUid());
        EXPECT_EQ(gid, response.GetNodeAttr().GetGid());
        EXPECT_EQ(
            static_cast<ui32>(E_REGULAR_NODE),
            response.GetNodeAttr().GetType());
        EXPECT_EQ(expectedMode, response.GetNodeAttr().GetMode());
        nodeId = response.GetNodeAttr().GetId();
        handle1 = response.GetHandle();
    }

    {
        TGetNodeAttrRequest request;
        request.SetNodeId(nodeId);
        auto f = shard->GetNodeAttr(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_EQ(nodeId, response.GetNode().GetId());
        EXPECT_EQ(uid, response.GetNode().GetUid());
        EXPECT_EQ(gid, response.GetNode().GetGid());
        EXPECT_EQ(
            static_cast<ui32>(E_REGULAR_NODE),
            response.GetNode().GetType());
        EXPECT_EQ(expectedMode, response.GetNode().GetMode());
    }

    ui64 handle2 = 0;
    {
        TCreateHandleRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        request.SetMode(mode);
        request.SetUid(uid);
        request.SetGid(gid);
        request.SetFlags(create);
        auto f = shard->CreateHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_EQ(nodeId, response.GetNodeAttr().GetId());
        EXPECT_EQ(uid, response.GetNodeAttr().GetUid());
        EXPECT_EQ(gid, response.GetNodeAttr().GetGid());
        EXPECT_EQ(
            static_cast<ui32>(E_REGULAR_NODE),
            response.GetNodeAttr().GetType());
        EXPECT_EQ(expectedMode, response.GetNodeAttr().GetMode());
        handle2 = response.GetHandle();
        EXPECT_NE(handle2, handle1);
    }

    {
        TDestroyHandleRequest request;
        request.SetHandle(handle1);
        auto f = shard->DestroyHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }

    {
        TDestroyHandleRequest request;
        request.SetHandle(handle1);
        auto f = shard->DestroyHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(NCloud::E_FS_BADHANDLE, response.GetError().GetCode())
            << FormatError(response.GetError());
    }

    {
        TDestroyHandleRequest request;
        request.SetHandle(handle2);
        auto f = shard->DestroyHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }
}

TEST(NaiveMirroredShardTest, WritesAndReadsFiles)
{
    TStorageFixture fx;

    auto shard = CreateNaiveMirroredFileSystemShard(ShardNo, fx.Config);

    const TString file1 = "file1";
    const TString file2 = "file2";
    const TString file3 = "file3";
    const ui32 mode = 0644;

    const ui32 createHandleFlags
        = ProtoFlag(TCreateHandleRequest::E_CREATE)
        | ProtoFlag(TCreateHandleRequest::E_READ)
        | ProtoFlag(TCreateHandleRequest::E_WRITE);

    ui64 nodeId = 0;
    {
        TCreateNodeRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        request.MutableFile()->SetMode(mode);
        auto f = shard->CreateNode(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        nodeId = response.GetNode().GetId();
    }

    ui64 handle = 0;
    {
        TCreateHandleRequest request;
        request.SetNodeId(nodeId);
        request.SetFlags(createHandleFlags);
        auto f = shard->CreateHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        handle = response.GetHandle();
        EXPECT_TRUE(handle != 0);
    }

    const auto expectedData = GenerateValidateData(4_KB);
    {
        TWriteDataRequest request;
        request.SetHandle(handle);
        request.SetOffset(0);
        *request.MutableBuffer() = expectedData;
        auto f = shard->WriteData(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }

    {
        TReadDataRequest request;
        request.SetHandle(handle);
        request.SetOffset(0);
        request.SetLength(4_KB);
        auto f = shard->ReadData(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        EXPECT_STREQ(expectedData.c_str(), response.GetBuffer().c_str());
    }

    {
        TDestroyHandleRequest request;
        request.SetHandle(handle);
        auto f = shard->DestroyHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }
}
