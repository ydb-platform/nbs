#include <cloud/filestore/libs/storage/fastshard/client/async_client.h>
#include <cloud/filestore/libs/storage/fastshard/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/impl/mem/memshard.h>
#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>
#include <cloud/filestore/libs/storage/fastshard/server/server.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>
#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <library/cpp/testing/common/network.h>

#include <gtest/gtest.h>

using namespace NCloud::NFileStore::NStorage::NFastShard;
using namespace NCloud::NFileStore::NStorage::NFastShard::NProtoSrv;
using silk::FiberFuture;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr TDuration WaitTimeout = TDuration::Seconds(5);

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

////////////////////////////////////////////////////////////////////////////////
// Test fixture that runs the server in a fiber.

struct TServerFixture
{
    NTesting::TPortHolder Port;
    IServerPtr Server;
    FiberFuture ServerFuture;

    TServerFixture()
        : Port(NTesting::GetFreePort())
        , Server(CreateServer(Port))
    {}

    void StartServer(IFileSystemShardPtr shard)
    {
        Server->RegisterShard("test-fs", std::move(shard));

        struct Params
        {
            IServer* Srv;
        };

        static_assert(sizeof(Params) <= silk::FIBER_PARAMETERS_SIZE);

        (void)FiberScheduler::run(
            +[](Params* p) noexcept -> int
            {
                p->Srv->Start();
                return 0;
            },
            Params{Server.get()},
            &ServerFuture);

        // Give the server a moment to bind.
        FiberScheduler::SleepFuture sf;
        FiberScheduler::sleep(50'000'000, &sf);   // 50ms
        sf.wait();
    }

    void StopServer()
    {
        Server->Stop();
        ServerFuture.wait();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(ServerTest, CreateNodeAndGetAttr)
{
    int result = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            NCloud::NFileStore::NProtoPrivate::TMemFastShardConfig cfg;
            cfg.SetCreateNodeUponAccess(true);
            auto shard = CreateMemFileSystemShard(1, cfg);

            TServerFixture fixture;
            fixture.StartServer(shard);

            TClient client;
            auto endpoint = client.Connect("localhost", fixture.Port);

            // CreateNode
            {
                TRequest req;
                req.SetFileSystemId("test-fs");
                auto* body = req.MutableCreateNode();
                body->SetNodeId(1);
                body->MutableFile()->SetMode(0644);
                body->SetName("hello.txt");
                auto resp = endpoint->Send(req);
                EXPECT_TRUE(resp.HasCreateNode());
            }

            // GetNodeAttr
            {
                TRequest req;
                req.SetFileSystemId("test-fs");
                auto* body = req.MutableGetNodeAttr();
                body->SetNodeId(1);
                body->SetName("hello.txt");
                auto resp = endpoint->Send(req);
                EXPECT_TRUE(resp.HasGetNodeAttr());
            }

            fixture.StopServer();
            return 0;
        },
        0);

    EXPECT_EQ(result, 0);
}

TEST(ServerTest, UnknownShardReturnsEmptyResponse)
{
    int result = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            NCloud::NFileStore::NProtoPrivate::TMemFastShardConfig cfg;
            auto shard = CreateMemFileSystemShard(1, cfg);

            TServerFixture fixture;
            fixture.StartServer(shard);

            TClient client;
            auto endpoint = client.Connect("localhost", fixture.Port);

            TRequest req;
            req.SetFileSystemId("nonexistent-fs");
            auto* body = req.MutableGetNodeAttr();
            body->SetNodeId(1);
            body->SetName("x");
            auto resp = endpoint->Send(req);
            EXPECT_TRUE(resp.HasError());
            EXPECT_EQ(
                resp.GetError().GetCode(),
                static_cast<ui32>(NCloud::E_NOT_FOUND));

            fixture.StopServer();
            return 0;
        },
        0);

    EXPECT_EQ(result, 0);
}

TEST(ServerTest, AsyncClientCreateNodeAndGetAttr)
{
    NCloud::NFileStore::NProtoPrivate::TMemFastShardConfig cfg;
    cfg.SetCreateNodeUponAccess(true);
    auto shard = CreateMemFileSystemShard(1, cfg);

    TServerFixture fixture;
    fixture.StartServer(shard);

    TAsyncClient client;
    auto endpoint =
        client.Connect("localhost", fixture.Port).ExtractValue(WaitTimeout);
    EXPECT_NE(endpoint, nullptr);

    // CreateNode
    {
        TRequest req;
        req.SetFileSystemId("test-fs");
        req.MutableCreateNode()->SetNodeId(1);
        req.MutableCreateNode()->MutableFile()->SetMode(0644);
        req.MutableCreateNode()->SetName("async.txt");
        auto resp = endpoint->Send(std::move(req)).ExtractValue(WaitTimeout);
        EXPECT_TRUE(resp.HasCreateNode());
    }

    // GetNodeAttr
    {
        TRequest req;
        req.SetFileSystemId("test-fs");
        req.MutableGetNodeAttr()->SetNodeId(1);
        req.MutableGetNodeAttr()->SetName("async.txt");
        auto resp = endpoint->Send(std::move(req)).ExtractValue(WaitTimeout);
        EXPECT_TRUE(resp.HasGetNodeAttr());
    }

    fixture.StopServer();
}

TEST(ServerTest, AsyncClientUnknownShardReturnsError)
{
    NCloud::NFileStore::NProtoPrivate::TMemFastShardConfig cfg;
    auto shard = CreateMemFileSystemShard(1, cfg);

    TServerFixture fixture;
    fixture.StartServer(shard);

    TAsyncClient client;
    auto endpoint =
        client.Connect("localhost", fixture.Port).ExtractValue(WaitTimeout);
    EXPECT_NE(endpoint, nullptr);

    TRequest req;
    req.SetFileSystemId("nonexistent-fs");
    req.MutableGetNodeAttr()->SetNodeId(1);
    req.MutableGetNodeAttr()->SetName("x");
    auto resp = endpoint->Send(std::move(req)).ExtractValue(WaitTimeout);
    EXPECT_TRUE(resp.HasError());
    EXPECT_EQ(
        resp.GetError().GetCode(),
        static_cast<ui32>(NCloud::E_NOT_FOUND));

    fixture.StopServer();
}
