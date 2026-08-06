#include <cloud/filestore/libs/storage/fastshard/ipc/ipc.h>
#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/server/server.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <library/cpp/testing/common/network.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <arpa/inet.h>
#include <gtest/gtest.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

using namespace NCloud::NFileStore::NStorage::NFastShard;
using NCloud::NProto::TDeviceProtocolRequest;
using NCloud::NProto::TDeviceProtocolResponse;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

////////////////////////////////////////////////////////////////////////////////
// Fiber-friendly non-blocking connect to loopback.

int ConnectTo(ui16 port)
{
    int fd = ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if (fd < 0) {
        return -1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);
    int r = ::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (r < 0 && errno != EINPROGRESS) {
        ::close(fd);
        return -1;
    }
    if (r < 0) {
        if (FiberScheduler::poll(fd, POLLOUT)) {
            ::close(fd);
            return -1;
        }
        int err = 0;
        socklen_t errLen = sizeof(err);
        ::getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &errLen);
        if (err) {
            ::close(fd);
            return -1;
        }
    }
    int one = 1;
    ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    return fd;
}

////////////////////////////////////////////////////////////////////////////////
// Send a framed request and read a framed response on the same connection.

TDeviceProtocolResponse SendRecv(int fd, const TDeviceProtocolRequest& req)
{
    TString buf;
    Y_PROTOBUF_SUPPRESS_NODISCARD req.SerializeToString(&buf);
    ui32 lenBe = htonl(static_cast<ui32>(buf.size()));
    EXPECT_EQ(0, SendAll(fd, &lenBe, sizeof(lenBe)));
    EXPECT_EQ(0, SendAll(fd, buf.data(), buf.size()));

    ui32 respLenBe = 0;
    EXPECT_EQ(0, RecvAll(fd, &respLenBe, sizeof(respLenBe)));
    ui32 respLen = ntohl(respLenBe);
    TString respBuf;
    respBuf.ReserveAndResize(respLen);
    EXPECT_EQ(0, RecvAll(fd, respBuf.begin(), respLen));

    TDeviceProtocolResponse resp;
    EXPECT_TRUE(resp.ParseFromString(respBuf));
    return resp;
}

////////////////////////////////////////////////////////////////////////////////
// Test fixture: builds a fake storage, starts the server on a free port.
//
// IServer::Start() runs synchronously through bind() + listen() and only then
// spawns the accept fiber, so once the constructor returns the listen socket
// is already accepting connections — no timing dependency.

struct TServerFixture
{
    std::shared_ptr<TFakeStorageNode> Storage;
    NTesting::TPortHolder Port;
    IServerPtr Server;

    TServerFixture()
        : Storage(std::make_shared<TFakeStorageNode>())
        , Port(NTesting::GetFreePort())
        , Server(CreateServer(Port, Storage))
    {
        Server->Start();
    }

    ~TServerFixture()
    {
        Server->Stop();
    }
};

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(ServerTest, RoundTripsAcquireDevices)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TServerFixture fx;

            int fd = ConnectTo(fx.Port);
            EXPECT_GE(fd, 0);

            TDeviceProtocolRequest req;
            req.SetRequestId(7);
            auto* body = req.MutableAcquireDevices();
            body->AddDeviceUUIDs("dev-a");
            body->AddDeviceUUIDs("dev-b");
            const auto resp = SendRecv(fd, req);
            ::close(fd);

            EXPECT_EQ(7u, resp.GetRequestId());
            EXPECT_TRUE(resp.HasAcquireDevices());
            EXPECT_FALSE(resp.HasProtocolError());
            EXPECT_EQ(1u, fx.Storage->AcquireCalls.size());
            EXPECT_EQ(2u, fx.Storage->AcquireCalls[0].DeviceUUIDsSize());
            EXPECT_EQ("dev-a", fx.Storage->AcquireCalls[0].GetDeviceUUIDs(0));
            EXPECT_EQ("dev-b", fx.Storage->AcquireCalls[0].GetDeviceUUIDs(1));

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ServerTest, RoundTripsReleaseDevices)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TServerFixture fx;

            int fd = ConnectTo(fx.Port);
            EXPECT_GE(fd, 0);

            TDeviceProtocolRequest req;
            req.SetRequestId(11);
            req.MutableReleaseDevices()->AddDeviceUUIDs("dev-x");
            const auto resp = SendRecv(fd, req);
            ::close(fd);

            EXPECT_EQ(11u, resp.GetRequestId());
            EXPECT_TRUE(resp.HasReleaseDevices());
            EXPECT_EQ(1u, fx.Storage->ReleaseCalls.size());
            EXPECT_EQ(1u, fx.Storage->ReleaseCalls[0].DeviceUUIDsSize());
            EXPECT_EQ("dev-x", fx.Storage->ReleaseCalls[0].GetDeviceUUIDs(0));

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ServerTest, RoundTripsReadPages)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TServerFixture fx;

            // Preload the canned response with a distinguishable payload.
            // Safe to configure post-Start: no client has connected yet so
            // the fake storage is not being read.
            auto* pg = fx.Storage->ReadResp.AddPageGroups();
            pg->SetFirstPageNo(5);
            pg->AddContent("page-content");

            int fd = ConnectTo(fx.Port);
            EXPECT_GE(fd, 0);

            TDeviceProtocolRequest req;
            req.SetRequestId(42);
            auto* body = req.MutableReadPages();
            body->SetDeviceUUID("dev-42");
            auto* ref = body->AddPageGroupRefs();
            ref->SetFirstPageNo(5);
            ref->SetPageCount(2);
            ref->SetPageSize(4096);
            const auto resp = SendRecv(fd, req);
            ::close(fd);

            EXPECT_EQ(42u, resp.GetRequestId());
            EXPECT_TRUE(resp.HasReadPages());
            EXPECT_EQ(1u, resp.GetReadPages().PageGroupsSize());
            EXPECT_EQ(
                5u,
                resp.GetReadPages().GetPageGroups(0).GetFirstPageNo());
            EXPECT_EQ(
                "page-content",
                resp.GetReadPages().GetPageGroups(0).GetContent(0));

            EXPECT_EQ(1u, fx.Storage->ReadCalls.size());
            EXPECT_EQ("dev-42", fx.Storage->ReadCalls[0].GetDeviceUUID());
            EXPECT_EQ(1u, fx.Storage->ReadCalls[0].PageGroupRefsSize());
            EXPECT_EQ(
                5u,
                fx.Storage->ReadCalls[0].GetPageGroupRefs(0).GetFirstPageNo());
            EXPECT_EQ(
                2u,
                fx.Storage->ReadCalls[0].GetPageGroupRefs(0).GetPageCount());
            EXPECT_EQ(
                4096u,
                fx.Storage->ReadCalls[0].GetPageGroupRefs(0).GetPageSize());

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ServerTest, RoundTripsWriteLogRecord)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TServerFixture fx;

            int fd = ConnectTo(fx.Port);
            EXPECT_GE(fd, 0);

            TDeviceProtocolRequest req;
            req.SetRequestId(99);
            auto* body = req.MutableWriteLogRecord();
            body->SetDeviceUUID("dev-99");
            body->SetLogSequenceNumber(1234);
            auto* pg = body->AddPageGroups();
            pg->SetFirstPageNo(0);
            pg->AddContent("payload");
            const auto resp = SendRecv(fd, req);
            ::close(fd);

            EXPECT_EQ(99u, resp.GetRequestId());
            EXPECT_TRUE(resp.HasWriteLogRecord());
            EXPECT_EQ(1u, fx.Storage->WriteCalls.size());
            EXPECT_EQ("dev-99", fx.Storage->WriteCalls[0].GetDeviceUUID());
            EXPECT_EQ(1234u, fx.Storage->WriteCalls[0].GetLogSequenceNumber());
            EXPECT_EQ(1u, fx.Storage->WriteCalls[0].PageGroupsSize());
            EXPECT_EQ(
                "payload",
                fx.Storage->WriteCalls[0].GetPageGroups(0).GetContent(0));

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ServerTest, ReturnsProtocolErrorForEmptyRequest)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TServerFixture fx;

            int fd = ConnectTo(fx.Port);
            EXPECT_GE(fd, 0);

            TDeviceProtocolRequest req;
            req.SetRequestId(3);
            // No oneof set → server should reply with ProtocolError.
            const auto resp = SendRecv(fd, req);
            ::close(fd);

            EXPECT_EQ(3u, resp.GetRequestId());
            EXPECT_TRUE(resp.HasProtocolError());
            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_ARGUMENT),
                resp.GetProtocolError().GetCode());
            EXPECT_EQ(0u, fx.Storage->AcquireCalls.size());
            EXPECT_EQ(0u, fx.Storage->ReleaseCalls.size());
            EXPECT_EQ(0u, fx.Storage->ReadCalls.size());
            EXPECT_EQ(0u, fx.Storage->WriteCalls.size());

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ServerTest, HandlesMultipleRequestsPerConnection)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TServerFixture fx;

            int fd = ConnectTo(fx.Port);
            EXPECT_GE(fd, 0);

            for (ui64 i = 1; i <= 3; ++i) {
                TDeviceProtocolRequest req;
                req.SetRequestId(i);
                req.MutableAcquireDevices()->AddDeviceUUIDs(
                    TStringBuilder() << "dev-" << i);
                const auto resp = SendRecv(fd, req);
                EXPECT_EQ(i, resp.GetRequestId());
                EXPECT_TRUE(resp.HasAcquireDevices());
            }
            ::close(fd);

            EXPECT_EQ(3u, fx.Storage->AcquireCalls.size());
            EXPECT_EQ("dev-1", fx.Storage->AcquireCalls[0].GetDeviceUUIDs(0));
            EXPECT_EQ("dev-2", fx.Storage->AcquireCalls[1].GetDeviceUUIDs(0));
            EXPECT_EQ("dev-3", fx.Storage->AcquireCalls[2].GetDeviceUUIDs(0));

            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ServerTest, StopClosesIdleConnection)
{
    // A client opens a connection but never sends a request. The handler
    // fiber ends up parked in RecvAll on the length prefix. When the
    // fixture is torn down, Server->Stop() must half-close that fd so the
    // handler can wake, exit, and be waited on -- otherwise the fiber
    // outlives the server and FiberScheduler::destroy() would be running
    // with a live fiber against a freed IStorageNode.
    //
    // Regression guard: without the THandlerRegistry / Stop path this
    // test hangs (RecvAll on the client side never gets EOF because the
    // server never shuts down cfd).
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            int fd;
            {
                TServerFixture fx;
                fd = ConnectTo(fx.Port);
                EXPECT_GE(fd, 0);
                // Do not send anything. Fixture goes out of scope,
                // Server->Stop() runs, and the handler must be woken.
            }

            // After Stop() the server has half-closed our socket, so the
            // next recv on our side must return EOF (RecvAll translates
            // that into EIO).
            char buf = 0;
            const int recvR = RecvAll(fd, &buf, sizeof(buf));
            EXPECT_EQ(EIO, recvR);
            ::close(fd);
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}
