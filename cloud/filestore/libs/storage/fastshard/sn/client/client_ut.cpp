#include <cloud/filestore/libs/storage/fastshard/sn/client/client.h>
#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/server/server.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/fake_storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <library/cpp/testing/common/network.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>

#include <gtest/gtest.h>

#include <util/generic/string.h>
#include <util/string/cast.h>

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
    IStorageNodePtr Client;

    TFixture()
        : Storage(std::make_shared<TFakeStorageNode>())
        , Port(NTesting::GetFreePort())
        , Server(CreateServer(Port, Storage))
        , Client(CreateStorageNodeClient("localhost", Port))
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

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(ClientTest, AcquireDevicesRoundTrip)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
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
        +[](int*) noexcept -> int {
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
        +[](int*) noexcept -> int {
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
        +[](int*) noexcept -> int {
            TFixture fx;

            NCloud::NProto::TWriteLogRecordRequest req;
            req.SetDeviceUUID("dev-99");
            req.SetLogSequenceNumber(1234);
            auto* pg = req.AddPageGroups();
            pg->SetFirstPageNo(0);
            pg->AddContent("payload");

            auto resp =
                fx.Client->WriteLogRecord(std::move(req));

            EXPECT_EQ(0u, resp.GetError().GetCode());
            EXPECT_EQ(1u, fx.Storage->WriteCalls.size());
            EXPECT_EQ("dev-99", fx.Storage->WriteCalls[0].GetDeviceUUID());
            EXPECT_EQ(
                1234u,
                fx.Storage->WriteCalls[0].GetLogSequenceNumber());
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
        +[](int*) noexcept -> int {
            TFixture fx;

            //
            // Storage returns a non-OK response. The client must relay
            // the Error field verbatim on the concrete response type.
            //

            *fx.Storage->AcquireResp.MutableError() = NCloud::MakeError(
                NCloud::E_FS_IO, "disk on fire");

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
        +[](int*) noexcept -> int {
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

TEST(ClientTest, ReusesConnectionAcrossSequentialCalls)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;

            //
            // Three sequential AcquireDevices calls on the same client
            // must all succeed. If the client tore the fd down between
            // calls (e.g. on a spurious error path) the second or third
            // call would still work — but the fake will show us the
            // full sequence.
            //

            for (ui32 i = 1; i <= 3; ++i) {
                NCloud::NProto::TAcquireDevicesRequest req;
                req.AddDeviceUUIDs(TString("dev-") + ToString(i));
                auto resp =
                    fx.Client->AcquireDevices(std::move(req));
                EXPECT_EQ(0u, resp.GetError().GetCode());
            }

            EXPECT_EQ(3u, fx.Storage->AcquireCalls.size());
            EXPECT_EQ("dev-1", fx.Storage->AcquireCalls[0].GetDeviceUUIDs(0));
            EXPECT_EQ("dev-2", fx.Storage->AcquireCalls[1].GetDeviceUUIDs(0));
            EXPECT_EQ("dev-3", fx.Storage->AcquireCalls[2].GetDeviceUUIDs(0));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}
