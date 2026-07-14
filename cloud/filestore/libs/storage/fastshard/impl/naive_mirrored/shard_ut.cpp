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

#include <gtest/gtest.h>

#include <util/system/tempfile.h>

using namespace NCloud;
using namespace NFileStore;
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
constexpr size_t PageSize = 512;
constexpr size_t PageCount = 16;
constexpr size_t FileSize = PageSize * PageCount;

////////////////////////////////////////////////////////////////////////////////
// Test fixture:
// * allocates N files
// * runs N storage nodes on top of them
// * allocates N ports
// * runs N storage node servers on top of the created storage nodes

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
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveMirroredShardTest, WritesAndReadsFiles)
{
    TStorageFixture fx;

    auto shard = CreateNaiveMirroredFileSystemShard(ShardNo, fx.Config);

    ui64 handle = 0;
    {
        NFileStore::NProto::TCreateHandleRequest request;
        auto f = shard->CreateHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
        handle = response.GetHandle();
    }

    {
        NFileStore::NProto::TDestroyHandleRequest request;
        request.SetHandle(handle);
        auto f = shard->DestroyHandle(request);
        auto response = f.GetValueSync();
        EXPECT_EQ(S_OK, response.GetError().GetCode())
            << FormatError(response.GetError());
    }
}
