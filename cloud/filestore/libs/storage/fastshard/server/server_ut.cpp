#include <cloud/filestore/libs/storage/fastshard/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/server/server.h>
#include <cloud/filestore/libs/storage/fastshard/server/protos/fastshard.pb.h>
#include <cloud/filestore/libs/storage/fastshard/impl/mem/memshard.h>

#include <cloud/filestore/private/api/unsafe_protos/unsafe.pb.h>
#include <cloud/filestore/public/api/protos/node.pb.h>

#include <cloud/storage/core/libs/common/error.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/util/init.h>

#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace NCloud::NFileStore::NStorage::NFastShard;
using namespace NCloud::NFileStore::NStorage::NFastShard::NProtoSrv;
using silk::FiberFuture;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////
// Silk test environment.

class TSilkEnv : public ::testing::Environment
{
public:
    void SetUp() override
    {
        silk::initialize();
        FiberScheduler::initialize();
    }
    void TearDown() override
    {
        FiberScheduler::destroy();
        silk::destroy();
    }
};

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(new TSilkEnv);

////////////////////////////////////////////////////////////////////////////////
// Pick a free port.

ui16 GetFreePort()
{
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;
    ::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    socklen_t len = sizeof(addr);
    ::getsockname(fd, reinterpret_cast<sockaddr*>(&addr), &len);
    ui16 port = ntohs(addr.sin_port);
    ::close(fd);
    return port;
}

////////////////////////////////////////////////////////////////////////////////
// Test fixture that runs the server in a fiber.

struct TServerFixture
{
    ui16 Port;
    IServerPtr Server;
    FiberFuture ServerFuture;

    TServerFixture()
        : Port(GetFreePort())
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
            +[](Params* p) noexcept -> int {
                p->Srv->Start();
                return 0;
            },
            Params{Server.get()},
            &ServerFuture);

        // Give the server a moment to bind.
        FiberScheduler::SleepFuture sf;
        FiberScheduler::sleep(50'000'000, &sf);  // 50ms
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
        +[](int*) noexcept -> int {
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
        +[](int*) noexcept -> int {
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
