#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/impl/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>

#include <gtest/gtest.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/tempfile.h>

using namespace NCloud::NFileStore::NStorage::NFastShard;
using NCloud::HasError;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

////////////////////////////////////////////////////////////////////////////////

constexpr size_t PageSize = 512;
constexpr size_t PageCount = 16;
constexpr size_t FileSize = PageSize * PageCount;

////////////////////////////////////////////////////////////////////////////////
// Owns a pre-sized temp file plus a storage node built on top of it. The
// temp file's own TFile fd is closed here so the node's fd is the only
// handle; the inode is unlinked on destruction of TTempFileHandle after
// the node has released its own reference.

struct TFixture
{
    TTempFileHandle Temp;
    IStorageNodePtr Node;

    TFixture()
        : Temp(TTempFileHandle::InCurrentDir("sn-impl-ut"))
    {
        Temp.Resize(FileSize);
        Temp.Close();
        Node = CreateNaiveFileStorageNode(Temp.Name());
    }
};

////////////////////////////////////////////////////////////////////////////////

TString Pattern(char c, size_t len)
{
    return {len, c};
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveFileStorageNodeTest, AcquireDevicesReturnsSuccess)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;
            NCloud::NProto::TAcquireDevicesRequest req;
            req.AddDeviceUUIDs("dev-x");
            auto resp = fx.Node->AcquireDevices(std::move(req));
            EXPECT_FALSE(HasError(resp.GetError()));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, ReleaseDevicesReturnsSuccess)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;
            NCloud::NProto::TReleaseDevicesRequest req;
            req.AddDeviceUUIDs("dev-x");
            auto resp = fx.Node->ReleaseDevices(std::move(req));
            EXPECT_FALSE(HasError(resp.GetError()));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, RoundTripsSinglePage)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;

            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* pg = wreq.AddPageGroups();
            pg->SetFirstPageNo(3);
            pg->AddContent(Pattern('a', PageSize));
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_FALSE(HasError(wresp.GetError()));

            NCloud::NProto::TReadPagesRequest rreq;
            auto* ref = rreq.AddPageGroupRefs();
            ref->SetFirstPageNo(3);
            ref->SetPageCount(1);
            ref->SetPageSize(PageSize);
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_FALSE(HasError(rresp.GetError()));
            EXPECT_EQ(1U, rresp.PageGroupsSize());
            EXPECT_EQ(3U, rresp.GetPageGroups(0).GetFirstPageNo());
            EXPECT_EQ(1U, rresp.GetPageGroups(0).ContentSize());
            EXPECT_EQ(
                Pattern('a', PageSize),
                rresp.GetPageGroups(0).GetContent(0));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, RoundTripsMultiPageGroup)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;

            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* pg = wreq.AddPageGroups();
            pg->SetFirstPageNo(0);
            pg->AddContent(Pattern('A', PageSize));
            pg->AddContent(Pattern('B', PageSize));
            pg->AddContent(Pattern('C', PageSize));
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_FALSE(HasError(wresp.GetError()));

            NCloud::NProto::TReadPagesRequest rreq;
            auto* ref = rreq.AddPageGroupRefs();
            ref->SetFirstPageNo(0);
            ref->SetPageCount(3);
            ref->SetPageSize(PageSize);
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_FALSE(HasError(rresp.GetError()));
            EXPECT_EQ(1U, rresp.PageGroupsSize());
            const auto& out = rresp.GetPageGroups(0);
            EXPECT_EQ(0U, out.GetFirstPageNo());
            EXPECT_EQ(3U, out.ContentSize());
            EXPECT_EQ(Pattern('A', PageSize), out.GetContent(0));
            EXPECT_EQ(Pattern('B', PageSize), out.GetContent(1));
            EXPECT_EQ(Pattern('C', PageSize), out.GetContent(2));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, RoundTripsMultipleGroups)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;

            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* g0 = wreq.AddPageGroups();
            g0->SetFirstPageNo(1);
            g0->AddContent(Pattern('x', PageSize));
            g0->AddContent(Pattern('y', PageSize));
            auto* g1 = wreq.AddPageGroups();
            g1->SetFirstPageNo(10);
            g1->AddContent(Pattern('z', PageSize));
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_FALSE(HasError(wresp.GetError()));

            NCloud::NProto::TReadPagesRequest rreq;
            auto* r0 = rreq.AddPageGroupRefs();
            r0->SetFirstPageNo(1);
            r0->SetPageCount(2);
            r0->SetPageSize(PageSize);
            auto* r1 = rreq.AddPageGroupRefs();
            r1->SetFirstPageNo(10);
            r1->SetPageCount(1);
            r1->SetPageSize(PageSize);
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_FALSE(HasError(rresp.GetError()));
            EXPECT_EQ(2U, rresp.PageGroupsSize());

            EXPECT_EQ(1U, rresp.GetPageGroups(0).GetFirstPageNo());
            EXPECT_EQ(2U, rresp.GetPageGroups(0).ContentSize());
            EXPECT_EQ(
                Pattern('x', PageSize),
                rresp.GetPageGroups(0).GetContent(0));
            EXPECT_EQ(
                Pattern('y', PageSize),
                rresp.GetPageGroups(0).GetContent(1));

            EXPECT_EQ(10U, rresp.GetPageGroups(1).GetFirstPageNo());
            EXPECT_EQ(1U, rresp.GetPageGroups(1).ContentSize());
            EXPECT_EQ(
                Pattern('z', PageSize),
                rresp.GetPageGroups(1).GetContent(0));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, LaterWriteOverridesEarlier)
{
    // Second write to the same offset must be readable back.
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;

            for (char c: {'1', '2'}) {
                NCloud::NProto::TWriteLogRecordRequest wreq;
                auto* pg = wreq.AddPageGroups();
                pg->SetFirstPageNo(5);
                pg->AddContent(Pattern(c, PageSize));
                auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
                EXPECT_FALSE(HasError(wresp.GetError()));
            }

            NCloud::NProto::TReadPagesRequest rreq;
            auto* ref = rreq.AddPageGroupRefs();
            ref->SetFirstPageNo(5);
            ref->SetPageCount(1);
            ref->SetPageSize(PageSize);
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_FALSE(HasError(rresp.GetError()));
            EXPECT_EQ(1U, rresp.PageGroupsSize());
            EXPECT_EQ(
                Pattern('2', PageSize),
                rresp.GetPageGroups(0).GetContent(0));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, EmptyReadReturnsSuccessWithNoGroups)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;
            NCloud::NProto::TReadPagesRequest rreq;
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_FALSE(HasError(rresp.GetError()));
            EXPECT_EQ(0U, rresp.PageGroupsSize());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, EmptyWriteReturnsSuccess)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int {
            TFixture fx;
            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_FALSE(HasError(wresp.GetError()));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, ThrowsWhenFileMissing)
{
    EXPECT_THROW(
        CreateNaiveFileStorageNode("/nonexistent-dir/no-such-file"),
        yexception);
}
