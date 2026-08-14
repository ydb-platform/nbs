#include <cloud/filestore/libs/storage/fastshard/sn/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/server/server.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>

#include <library/cpp/testing/common/network.h>

#include <util/datetime/base.h>
#include <util/generic/string.h>
#include <util/string/cast.h>

#include <gtest/gtest.h>

#include <atomic>

using namespace NCloud::NFileStore::NStorage::NFastShard;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

////////////////////////////////////////////////////////////////////////////////
// Test fixture: builds a fake storage, starts the sn server on a free
// port, and constructs an IStorageNode client pointing at it.

struct TFixture
{
    std::shared_ptr<TFakeStorageNode> Storage;
    NTesting::TPortHolder Port;
    IServerPtr Server;
    TStorageNodeClientMetricsPtr Metrics;
    IStorageNodePtr Client;

    TFixture()
        : Storage(std::make_shared<TFakeStorageNode>())
        , Port(NTesting::GetFreePort())
        , Server(CreateServer(Port, Storage))
        , Metrics(std::make_shared<TStorageNodeClientMetrics>())
        , Client(CreateStorageNodeClient("localhost", Port, Metrics))
    {
        Server->Start();
    }

    ~TFixture()
    {
        Server->Stop();
    }
};

////////////////////////////////////////////////////////////////////////////////
// Fixture for a client pointing at a definitely-unreachable port. Used
// by the connect-failure test.

struct TUnreachableFixture
{
    NTesting::TPortHolder Port;
    IStorageNodePtr Client;

    TUnreachableFixture()
        : Port(NTesting::GetFreePort())
        , Client(CreateStorageNodeClient("localhost", Port))
    {
        //
        // NTesting::GetFreePort returns a port it briefly bound and
        // released — the returned TPortHolder only carries the
        // cross-process file lock, so no socket is listening. connect()
        // will get ECONNREFUSED.
        //
    }
};

////////////////////////////////////////////////////////////////////////////////
// IStorageNode wrapper that delays every call, then delegates. Used to
// prove that concurrent client calls overlap on the wire.

struct TSlowStorageNode: public IStorageNode
{
    IStorageNodePtr Inner;
    ui64 DelayNs = 0;

    TSlowStorageNode(IStorageNodePtr inner, ui64 delayNs)
        : Inner(std::move(inner))
        , DelayNs(delayNs)
    {}

#define SLOW_SN_METHOD(name, ...)                                              \
    NCloud::NProto::T##name##Response name(                                    \
        NCloud::NProto::T##name##Request request) override                     \
    {                                                                          \
        FiberScheduler::SleepFuture sf;                                        \
        FiberScheduler::sleep(DelayNs, &sf);                                   \
        sf.wait();                                                             \
        return Inner->name(std::move(request));                                \
    }                                                                          \
    // SLOW_SN_METHOD

    SN_METHODS(SLOW_SN_METHOD)

#undef SLOW_SN_METHOD
};

////////////////////////////////////////////////////////////////////////////////
// Fires Count concurrent AcquireDevices calls on `client` from separate
// fibers and returns the number of failed responses.

struct TBurstParams
{
    IStorageNode* Client;
    std::atomic<ui32>* ErrorCount;
};

static_assert(sizeof(TBurstParams) <= silk::FIBER_PARAMETERS_SIZE);

int BurstCallFiber(TBurstParams* params) noexcept
{
    NCloud::NProto::TAcquireDevicesRequest req;
    req.AddDeviceUUIDs("dev-burst");
    auto resp = params->Client->AcquireDevices(std::move(req));
    if (resp.GetError().GetCode()) {
        params->ErrorCount->fetch_add(1);
    }
    return 0;
}

ui32 RunBurst(IStorageNode& client, ui32 count)
{
    std::atomic<ui32> errorCount{0};

    constexpr ui32 MaxBurst = 64U;
    Y_ABORT_UNLESS(count <= MaxBurst);
    silk::FiberFuture futures[MaxBurst];

    for (ui32 i = 0; i < count; ++i) {
        const int r = FiberScheduler::run(
            BurstCallFiber,
            TBurstParams{.Client = &client, .ErrorCount = &errorCount},
            &futures[i]);
        Y_ABORT_UNLESS(r == 0);
    }
    for (ui32 i = 0; i < count; ++i) {
        futures[i].wait();
    }

    return errorCount.load();
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(ClientTest, AcquireDevicesRoundTrip)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            NCloud::NProto::TAcquireDevicesRequest req;
            req.AddDeviceUUIDs("dev-a");
            req.AddDeviceUUIDs("dev-b");
            auto resp = fx.Client->AcquireDevices(std::move(req));

            EXPECT_FALSE(resp.HasError() && resp.GetError().GetCode())
                << resp.GetError().GetMessage();
            EXPECT_EQ(1u, fx.Storage->AcquireCalls.size());
            EXPECT_EQ(2u, fx.Storage->AcquireCalls[0].DeviceUUIDsSize());
            EXPECT_EQ("dev-a", fx.Storage->AcquireCalls[0].GetDeviceUUIDs(0));
            EXPECT_EQ("dev-b", fx.Storage->AcquireCalls[0].GetDeviceUUIDs(1));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ClientTest, ReleaseDevicesRoundTrip)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            NCloud::NProto::TReleaseDevicesRequest req;
            req.AddDeviceUUIDs("dev-x");
            auto resp = fx.Client->ReleaseDevices(std::move(req));

            EXPECT_EQ(0u, resp.GetError().GetCode());
            EXPECT_EQ(1u, fx.Storage->ReleaseCalls.size());
            EXPECT_EQ(1u, fx.Storage->ReleaseCalls[0].DeviceUUIDsSize());
            EXPECT_EQ("dev-x", fx.Storage->ReleaseCalls[0].GetDeviceUUIDs(0));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ClientTest, ReadPagesRoundTrip)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            //
            // Preload the canned response before the client connects so
            // the server-side dispatch returns exactly what the client
            // should see back.
            //

            auto* pg = fx.Storage->ReadResp.AddPageGroups();
            pg->SetFirstPageNo(5);
            pg->AddContent("page-content");

            NCloud::NProto::TReadPagesRequest req;
            req.SetDeviceUUID("dev-42");
            auto* ref = req.AddPageGroupRefs();
            ref->SetFirstPageNo(5);
            ref->SetPageCount(2);
            ref->SetPageSize(4096);

            auto resp = fx.Client->ReadPages(std::move(req));

            EXPECT_EQ(0u, resp.GetError().GetCode());
            EXPECT_EQ(1u, resp.PageGroupsSize());
            EXPECT_EQ(5u, resp.GetPageGroups(0).GetFirstPageNo());
            EXPECT_EQ("page-content", resp.GetPageGroups(0).GetContent(0));

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

TEST(ClientTest, WriteLogRecordRoundTrip)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            NCloud::NProto::TWriteLogRecordRequest req;
            req.SetDeviceUUID("dev-99");
            req.SetLogSequenceNumber(1234);
            auto* pg = req.AddPageGroups();
            pg->SetFirstPageNo(0);
            pg->AddContent("payload");

            auto resp = fx.Client->WriteLogRecord(std::move(req));

            EXPECT_EQ(0u, resp.GetError().GetCode());
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

TEST(ClientTest, StorageErrorPassesThrough)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            //
            // Storage returns a non-OK response. The client must relay
            // the Error field verbatim on the concrete response type.
            //

            *fx.Storage->AcquireResp.MutableError() =
                NCloud::MakeError(NCloud::E_FS_IO, "disk on fire");

            NCloud::NProto::TAcquireDevicesRequest req;
            req.AddDeviceUUIDs("dev-a");
            auto resp = fx.Client->AcquireDevices(std::move(req));

            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_FS_IO),
                resp.GetError().GetCode());
            EXPECT_EQ("disk on fire", resp.GetError().GetMessage());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ClientTest, ConnectFailureReturnsRejected)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TUnreachableFixture fx;

            NCloud::NProto::TAcquireDevicesRequest req;
            req.AddDeviceUUIDs("dev-a");
            auto resp = fx.Client->AcquireDevices(std::move(req));

            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_REJECTED),
                resp.GetError().GetCode());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ClientTest, ConcurrentCallsUseMultipleConnections)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            //
            // Storage delays every call, so all concurrent calls are in
            // flight at the same time and each needs its own connection.
            // A client that serializes calls on one connection would
            // create and use exactly one.
            //

            constexpr ui64 DelayNs = 100'000'000ULL;
            constexpr ui32 Concurrency = 10U;

            auto storage = std::make_shared<TFakeStorageNode>();
            auto slow = std::make_shared<TSlowStorageNode>(storage, DelayNs);
            NTesting::TPortHolder port = NTesting::GetFreePort();
            auto server = CreateServer(port, slow);
            server->Start();

            auto metrics = std::make_shared<TStorageNodeClientMetrics>();
            auto client =
                CreateStorageNodeClient("localhost", port, metrics);

            const ui32 errorCount = RunBurst(*client, Concurrency);

            EXPECT_EQ(0u, errorCount);
            EXPECT_EQ(Concurrency, storage->AcquireCalls.size());
            EXPECT_GT(metrics->ConnectionsCreated.load(), 1u);
            EXPECT_GT(metrics->ConnectionsUsed.load(), 1u);
            EXPECT_EQ(Concurrency, metrics->RequestsCompleted.load());

            server->Stop();
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ClientTest, ConcurrentBurstAllSucceed)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            //
            // Several waves through the same client: the first wave
            // grows the pool, the following waves must reuse the pooled
            // connections without errors.
            //

            constexpr ui32 Concurrency = 16U;
            constexpr ui32 Waves = 4U;

            for (ui32 wave = 0; wave < Waves; ++wave) {
                EXPECT_EQ(0u, RunBurst(*fx.Client, Concurrency));
            }

            EXPECT_EQ(
                Concurrency * Waves,
                fx.Storage->AcquireCalls.size());

            //
            // A connection is only opened when a request finds the pool
            // empty, and at most Concurrency requests are in flight at
            // any moment - so the total across all waves stays bounded
            // by one wave's parallelism no matter how the waves
            // interleave.
            //

            EXPECT_LE(fx.Metrics->ConnectionsCreated.load(), Concurrency);
            EXPECT_EQ(
                Concurrency * Waves,
                fx.Metrics->RequestsCompleted.load());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ClientTest, ReconnectsAfterServerRestart)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            auto storage = std::make_shared<TFakeStorageNode>();
            NTesting::TPortHolder port = NTesting::GetFreePort();
            auto server = CreateServer(port, storage);
            server->Start();

            auto client = CreateStorageNodeClient("localhost", port);

            {
                NCloud::NProto::TAcquireDevicesRequest req;
                req.AddDeviceUUIDs("dev-before");
                auto resp = client->AcquireDevices(std::move(req));
                EXPECT_EQ(0u, resp.GetError().GetCode());
            }

            server->Stop();

            //
            // The pooled connection is dead now: the call must fail with
            // E_REJECTED and the client must drop the connection.
            //

            {
                NCloud::NProto::TAcquireDevicesRequest req;
                req.AddDeviceUUIDs("dev-down");
                auto resp = client->AcquireDevices(std::move(req));
                EXPECT_EQ(
                    static_cast<ui32>(NCloud::E_REJECTED),
                    resp.GetError().GetCode());
            }

            //
            // A new server on the same port: the client must reconnect
            // transparently. The first server must be destroyed first -
            // Stop() ends the accept loop but the listening socket is
            // closed by the destructor, and a lingering listener would
            // both fail the new bind and swallow the reconnect into a
            // dead backlog.
            //

            server = nullptr;

            auto server2 = CreateServer(port, storage);
            server2->Start();

            {
                NCloud::NProto::TAcquireDevicesRequest req;
                req.AddDeviceUUIDs("dev-after");
                auto resp = client->AcquireDevices(std::move(req));
                EXPECT_EQ(0u, resp.GetError().GetCode());
            }

            server2->Stop();
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(ClientTest, ReusesConnectionAcrossSequentialCalls)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            //
            // Sequential calls never overlap, so the pool must serve
            // all of them with the single connection opened by the
            // first call.
            //

            for (ui32 i = 1; i <= 3; ++i) {
                NCloud::NProto::TAcquireDevicesRequest req;
                req.AddDeviceUUIDs(TString("dev-") + ToString(i));
                auto resp = fx.Client->AcquireDevices(std::move(req));
                EXPECT_EQ(0u, resp.GetError().GetCode());
            }

            EXPECT_EQ(3u, fx.Storage->AcquireCalls.size());
            EXPECT_EQ("dev-1", fx.Storage->AcquireCalls[0].GetDeviceUUIDs(0));
            EXPECT_EQ("dev-2", fx.Storage->AcquireCalls[1].GetDeviceUUIDs(0));
            EXPECT_EQ("dev-3", fx.Storage->AcquireCalls[2].GetDeviceUUIDs(0));

            EXPECT_EQ(1u, fx.Metrics->ConnectionsCreated.load());
            EXPECT_EQ(1u, fx.Metrics->ConnectionsUsed.load());
            EXPECT_EQ(3u, fx.Metrics->RequestsCompleted.load());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}
