#include <cloud/filestore/libs/storage/fastshard/sn/iface/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/sn/impl/storage_node.h>
#include <cloud/filestore/libs/storage/fastshard/testlib/silk_env.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/protos/device.pb.h>

#include <silk/fibers/fiber.h>

#include <util/generic/string.h>
#include <util/generic/yexception.h>
#include <util/system/tempfile.h>

#include <gtest/gtest.h>

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
        : TFixture(FileSize)
    {}

    explicit TFixture(size_t backingSize)
        : Temp(TTempFileHandle::InCurrentDir("sn-impl-ut"))
    {
        Temp.Resize(backingSize);
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
        +[](int*) noexcept -> int
        {
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
        +[](int*) noexcept -> int
        {
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
        +[](int*) noexcept -> int
        {
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
        +[](int*) noexcept -> int
        {
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
        +[](int*) noexcept -> int
        {
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
        +[](int*) noexcept -> int
        {
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
        +[](int*) noexcept -> int
        {
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
        +[](int*) noexcept -> int
        {
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

////////////////////////////////////////////////////////////////////////////////

TEST(NaiveFileStorageNodeTest, ReadPagesReturnsErrorOnShortRead)
{
    // Backing file holds only one page; the request asks for two.
    // The kernel returns 512 bytes for the two-page read, which io_uring
    // reports as success + bytesRead=512. ReadPages must catch the
    // mismatch and surface E_IO instead of returning a half-populated
    // response.
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx(PageSize /* backingSize */);

            NCloud::NProto::TReadPagesRequest rreq;
            auto* ref = rreq.AddPageGroupRefs();
            ref->SetFirstPageNo(0);
            ref->SetPageCount(2);
            ref->SetPageSize(PageSize);
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_TRUE(HasError(rresp.GetError()));
            EXPECT_EQ(0U, rresp.PageGroupsSize());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, ReadPagesReportsErrorOnPartialFailure)
{
    // Two page-group refs are submitted at once: the first is valid, the
    // second uses FirstPageNo=UINT64_MAX which the kernel rejects with
    // EINVAL (offset reinterpreted as off_t is negative). This exercises
    // the "one op fails while the sibling is still in flight" path: the
    // impl must wait for both completions before destroying its op
    // storage -- otherwise io_uring signals a freed IoFuture. Under asan
    // the UAF would be caught; the observable contract is (a) no crash
    // (b) error surfaced (c) no partial page data.
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            NCloud::NProto::TReadPagesRequest rreq;
            auto* valid = rreq.AddPageGroupRefs();
            valid->SetFirstPageNo(0);
            valid->SetPageCount(1);
            valid->SetPageSize(PageSize);
            auto* poison = rreq.AddPageGroupRefs();
            poison->SetFirstPageNo(1ULL << 63 /* off_t sees INT64_MIN */);
            poison->SetPageCount(1);
            poison->SetPageSize(1);
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_TRUE(HasError(rresp.GetError()));
            EXPECT_EQ(0U, rresp.PageGroupsSize());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, ReadPagesRejectsPageCountPageSizeOverflow)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            NCloud::NProto::TReadPagesRequest rreq;
            auto* ref = rreq.AddPageGroupRefs();
            // PageSize is uint32 in the proto; pick values within that
            // range whose product overflows ui64: 2^40 * 2^25 == 2^65.
            ref->SetFirstPageNo(0);
            ref->SetPageCount(1ULL << 40);
            ref->SetPageSize(1U << 25);
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_TRUE(HasError(rresp.GetError()));
            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_ARGUMENT),
                rresp.GetError().GetCode());
            EXPECT_EQ(0U, rresp.PageGroupsSize());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, ReadPagesRejectsFirstPageNoPageSizeOverflow)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            NCloud::NProto::TReadPagesRequest rreq;
            auto* ref = rreq.AddPageGroupRefs();
            // FirstPageNo * PageSize == 2^60 * 2^20 == 2^80 overflows.
            ref->SetFirstPageNo(1ULL << 60);
            ref->SetPageCount(1);
            ref->SetPageSize(1U << 20);
            auto rresp = fx.Node->ReadPages(std::move(rreq));
            EXPECT_TRUE(HasError(rresp.GetError()));
            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_ARGUMENT),
                rresp.GetError().GetCode());
            EXPECT_EQ(0U, rresp.PageGroupsSize());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, WriteLogRecordRejectsFirstPageNoPageSizeOverflow)
{
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* pg = wreq.AddPageGroups();
            // pageSize == Content[0].size() == PageSize (512).
            // FirstPageNo * pageSize needs to exceed 2^64 to overflow;
            // 2^60 * 512 == 2^69 comfortably does.
            pg->SetFirstPageNo(1ULL << 60);
            pg->AddContent(Pattern('q', PageSize));
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_TRUE(HasError(wresp.GetError()));
            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_ARGUMENT),
                wresp.GetError().GetCode());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, WriteLogRecordRejectsMixedPageSizes)
{
    // Two groups whose page sizes disagree. The impl derives per-group
    // offsets from a single page size; letting mixed sizes through would
    // silently misplace the second group's bytes on disk.
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* g0 = wreq.AddPageGroups();
            g0->SetFirstPageNo(0);
            g0->AddContent(Pattern('a', PageSize));
            auto* g1 = wreq.AddPageGroups();
            g1->SetFirstPageNo(4);
            g1->AddContent(Pattern('b', PageSize / 2));
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_TRUE(HasError(wresp.GetError()));
            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_ARGUMENT),
                wresp.GetError().GetCode());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, WriteLogRecordRejectsMixedPageSizesWithinGroup)
{
    // Two content pages of one group disagree in size.
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* pg = wreq.AddPageGroups();
            pg->SetFirstPageNo(0);
            pg->AddContent(Pattern('a', PageSize));
            pg->AddContent(Pattern('b', PageSize / 2));
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_TRUE(HasError(wresp.GetError()));
            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_ARGUMENT),
                wresp.GetError().GetCode());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, WriteLogRecordRejectsOverlappingIntervals)
{
    // Group 0 covers pages [0, 3), group 1 covers pages [2, 4).
    // They overlap on page 2 -- concurrent writes to the same range
    // have unspecified ordering, so the request must be rejected.
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* g0 = wreq.AddPageGroups();
            g0->SetFirstPageNo(0);
            for (int i = 0; i < 3; ++i) {
                g0->AddContent(Pattern('a', PageSize));
            }
            auto* g1 = wreq.AddPageGroups();
            g1->SetFirstPageNo(2);
            for (int i = 0; i < 2; ++i) {
                g1->AddContent(Pattern('b', PageSize));
            }
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_TRUE(HasError(wresp.GetError()));
            EXPECT_EQ(
                static_cast<ui32>(NCloud::E_ARGUMENT),
                wresp.GetError().GetCode());
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, WriteLogRecordAcceptsAdjacentIntervals)
{
    // Adjacent (touching but not overlapping) intervals must be allowed.
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;
            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* g0 = wreq.AddPageGroups();
            g0->SetFirstPageNo(0);
            g0->AddContent(Pattern('a', PageSize));
            g0->AddContent(Pattern('b', PageSize));
            auto* g1 = wreq.AddPageGroups();
            g1->SetFirstPageNo(2);
            g1->AddContent(Pattern('c', PageSize));
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_FALSE(HasError(wresp.GetError()));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}

TEST(NaiveFileStorageNodeTest, WriteLogRecordReportsErrorOnPartialFailure)
{
    // Same trick as the read case: one legit gather-write plus one
    // poisoned group whose FirstPageNo yields a negative off_t. Verifies
    // WriteLogRecord waits for every submitted op before returning so
    // io_uring can't touch freed op state.
    const int r = FiberScheduler::run(
        +[](int*) noexcept -> int
        {
            TFixture fx;

            NCloud::NProto::TWriteLogRecordRequest wreq;
            auto* good = wreq.AddPageGroups();
            good->SetFirstPageNo(0);
            good->AddContent(Pattern('.', PageSize));
            auto* bad = wreq.AddPageGroups();
            bad->SetFirstPageNo(1ULL << 63 /* off_t sees INT64_MIN */);
            bad->AddContent("x");
            auto wresp = fx.Node->WriteLogRecord(std::move(wreq));
            EXPECT_TRUE(HasError(wresp.GetError()));
            return 0;
        },
        0);
    EXPECT_EQ(0, r);
}
