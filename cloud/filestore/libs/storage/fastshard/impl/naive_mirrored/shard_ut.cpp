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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveMirroredShardTest, CreatesFiles)
{
    silk::Logger::setLevel(silk::LogLevel::DEBUG);

    TStorageFixture fx;

    auto shard = CreateNaiveMirroredFileSystemShard(ShardNo, fx.Config);

    const TString file1 = "file1";
    const ui32 mode = 0644;
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
    Y_UNUSED(nodeId);

    {
        TUnlinkNodeRequest request;
        request.SetNodeId(RootNodeId);
        request.SetName(file1);
        auto f = shard->UnlinkNode(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }
}
