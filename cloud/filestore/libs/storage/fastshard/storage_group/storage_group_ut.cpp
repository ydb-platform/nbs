#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group.h>
#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group_helpers.h>
#include <cloud/filestore/libs/storage/fastshard/storage_group/storage_group_quorum.h>
#include <cloud/filestore/private/api/protos/tablet.pb.h>

#include <cloud/fastshard/libs/sn/iface/storage_node.h>
#include <cloud/fastshard/libs/sn/testlib/fake_storage_node.h>
#include <cloud/fastshard/libs/sn/testlib/fiber_test.h>
#include <cloud/fastshard/libs/sn/testlib/silk_env.h>
#include <cloud/fastshard/public/api/protos/device.pb.h>

#include <cloud/storage/core/libs/common/error.h>
#include <cloud/storage/core/libs/common/timer_test.h>

#include <silk/fibers/fiber.h>
#include <silk/fibers/future.h>
#include <silk/fibers/sequencer.h>

#include <util/datetime/base.h>
#include <util/generic/deque.h>
#include <util/generic/size_literals.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/generic/utility.h>
#include <util/generic/vector.h>
#include <util/string/builder.h>
#include <util/system/guard.h>
#include <util/system/spinlock.h>

#include <gtest/gtest.h>

#include <atomic>
#include <cstring>
#include <initializer_list>
#include <memory>
#include <utility>

using namespace NCloud;
using namespace NFileStore::NStorage::NFastShard;
using namespace NCloud::NFastShard;
using silk::FiberScheduler;

namespace {

////////////////////////////////////////////////////////////////////////////////

[[maybe_unused]] auto* const gEnv =
    ::testing::AddGlobalTestEnvironment(MakeSilkTestEnv());

constexpr ui32 DeviceCount = 3;

constexpr ui64 PageNo = 111;
constexpr ui64 Lsn = 2;

const NProto::TDeviceRequestHeaders NoHeaders;

////////////////////////////////////////////////////////////////////////////////

template <typename TRequest, typename TResponse>
struct TCall
{
    TAdaptiveLock Lock;
    TVector<TRequest> Requests;
    TResponse Response;
    TDeque<TResponse> ResponseQueue;

    // While paused, a call is held until the test lets it through, one per
    // Step, or all the parked ones with Unpause; it is logged once through.
    std::atomic<bool> Paused = false;
    std::atomic<ui32> Parked = 0;
    silk::FiberSequencer Allowed;

    TResponse Call(TRequest request)
    {
        ui64 ticket = 0;
        with_lock (Lock) {
            if (Paused) {
                ticket = ++Parked;
            }
        }
        if (ticket) {
            Y_UNUSED(Allowed.wait(ticket));
        }

        with_lock (Lock) {
            Requests.push_back(std::move(request));
            if (ResponseQueue.empty()) {
                return Response;
            }

            TResponse response = std::move(ResponseQueue.front());
            ResponseQueue.pop_front();
            return response;
        }
    }

    ui32 Count()
    {
        with_lock (Lock) {
            return Requests.size();
        }
    }

    TRequest Last()
    {
        with_lock (Lock) {
            return Requests.back();
        }
    }

    void Reply(const NProto::TError& error)
    {
        with_lock (Lock) {
            Response = {};
            *Response.MutableError() = error;
        }
    }

    void Step()
    {
        Allowed.increment();
    }

    void Unpause()
    {
        with_lock (Lock) {
            Paused = false;
            Allowed.advance(Parked);
        }
    }
};

/**
 * A storage node that keeps nothing: it answers every request with its canned
 * reply, blank pages for a read, and parks a write while its gate is paused.
 * A test sets it up by scripting the replies and checks what it has been
 * asked. What the devices end up holding is the journalled node's business.
 */
struct TFakeDevice: TFakeStorageNode
{
    TCall<NProto::TWriteLogRecordRequest, NProto::TWriteLogRecordResponse>
        Writes;
    TCall<NProto::TReadPagesRequest, NProto::TReadPagesResponse> Reads;
    TCall<NProto::TAcquireDevicesRequest, NProto::TAcquireDevicesResponse>
        Acquires;

    TCall<
        NProto::TAdvanceLsnLowWatermarkRequest,
        NProto::TAdvanceLsnLowWatermarkResponse>
        Watermarks;


    NProto::TReadPagesResponse ReadPages(
        NProto::TReadPagesRequest request) override
    {
        const auto refs = request.GetPageGroupRefs();
        auto response = Reads.Call(std::move(request));
        if (HasError(response.GetError()) || response.PageGroupsSize()) {
            return response;
        }

        for (const auto& ref: refs) {
            auto* pg = response.AddPageGroups();
            pg->SetFirstPageNo(ref.GetFirstPageNo());
            for (ui64 i = 0; i < ref.GetPageCount(); ++i) {
                pg->AddContent(TString(ref.GetPageSize(), '\0'));
            }
        }

        return response;
    }

    NProto::TWriteLogRecordResponse WriteLogRecord(
        NProto::TWriteLogRecordRequest request) override
    {
        return Writes.Call(std::move(request));
    }

    NProto::TAcquireDevicesResponse AcquireDevices(
        NProto::TAcquireDevicesRequest request) override
    {
        return Acquires.Call(std::move(request));
    }

    NProto::TAdvanceLsnLowWatermarkResponse AdvanceLsnLowWatermark(
        NProto::TAdvanceLsnLowWatermarkRequest request) override
    {
        return Watermarks.Call(std::move(request));
    }
};

using TFakeDevicePtr = std::shared_ptr<TFakeDevice>;

// A reply carrying one page, for a device that is to answer with something
// other than a blank.
NProto::TReadPagesResponse PageResponse(ui64 pageNo, TString content)
{
    NProto::TReadPagesResponse response;
    auto* pg = response.AddPageGroups();
    pg->SetFirstPageNo(pageNo);
    pg->AddContent(std::move(content));
    return response;
}

////////////////////////////////////////////////////////////////////////////////

TStorageGroupConfig MakeConfig(ui32 pageSize = DefaultBlockSize)
{
    TStorageGroupConfig config;
    config.ClientId = "test-client";
    config.AcquireGeneration = 42;
    config.PageSize = pageSize;
    return config;
}

////////////////////////////////////////////////////////////////////////////////

using TGroupFactory = IStorageGroupPtr (*)(
    TStorageGroupConfig,
    TVector<TStorageDevice>,
    ITimerPtr);

////////////////////////////////////////////////////////////////////////////////
// Fixtures: a group over a set of fake devices.

struct TGroupFixture
{
    TVector<TFakeDevicePtr> Nodes;
    TVector<TString> DeviceUUIDs;
    TStorageGroupConfig Config;
    ITimerPtr Timer;
    IStorageGroupPtr Group;

    TGroupFixture(
            TGroupFactory createGroup,
            TStorageGroupConfig config,
            ITimerPtr timer,
            ui32 deviceCount)
        : Config(std::move(config))
        , Timer(std::move(timer))
    {
        for (ui32 i = 0; i < deviceCount; ++i) {
            Nodes.push_back(std::make_shared<TFakeDevice>());
            DeviceUUIDs.push_back(TStringBuilder() << "dev-" << char('a' + i));
        }

        Group = createGroup(Config, StorageDevices(), Timer);
    }

    ~TGroupFixture()
    {
        for (auto& device: Nodes) {
            device->Writes.Unpause();
            device->Acquires.Unpause();
        }

        Group->TearDown();
    }

    ui32 Size() const
    {
        return Nodes.size();
    }

    TFakeDevice& operator[](ui32 i)
    {
        return *Nodes[i];
    }

    TVector<TStorageDevice> StorageDevices() const
    {
        TVector<TStorageDevice> devices;
        for (ui32 i = 0; i < Size(); ++i) {
            devices.push_back({.Node = Nodes[i], .DeviceUUID = DeviceUUIDs[i]});
        }
        return devices;
    }

    // The sleeps the group asked for, when it runs on a TTestTimer.
    TTestTimer& TestTimer()
    {
        auto* timer = dynamic_cast<TTestTimer*>(Timer.get());
        Y_ABORT_UNLESS(timer, "the group runs on another timer");
        return *timer;
    }

    // Forgets what the devices have been asked so far, so a test that follows
    // Init starts from a clean request log.
    void ForgetRequests()
    {
        for (auto& device: Nodes) {
            device->Writes.Requests.clear();
            device->Reads.Requests.clear();
            device->ReadJournalTailCalls.clear();
        }
    }
};

struct TNaiveFixture: TGroupFixture
{
    TNaiveFixture(TStorageGroupConfig config = MakeConfig())
        : TGroupFixture(
              CreateNaiveMirroredStorageGroup,
              std::move(config),
              std::make_shared<TTestTimer>(),
              DeviceCount)
    {}
};

// init = false leaves Init to the test, so it can set the devices up first.
struct TQuorumFixture: TGroupFixture
{
    TQuorumFixture(
            bool init = true,
            TStorageGroupConfig config = MakeConfig(),
            ui32 deviceCount = DeviceCount)
        : TGroupFixture(
              CreateQuorumMirroredStorageGroup,
              std::move(config),
              std::make_shared<TTestTimer>(),
              deviceCount)
    {
        if (init) {
            auto error = Group->Init().GetError();
            Y_ABORT_UNLESS(
                !HasError(error),
                "failed to initialize: %s",
                FormatError(error).c_str());
        }
    }

    TQuorumFixture(ui32 deviceCount)
        : TQuorumFixture(true, MakeConfig(), deviceCount)
    {}
};

////////////////////////////////////////////////////////////////////////////////
// Driving the group.

// A record of one page group, linked to the record before it.
NProto::TError WritePages(
    IStorageGroup& group,
    ui64 lsn,
    ui64 firstPageNo,
    std::initializer_list<TStringBuf> pages)
{
    TPageGroup pageGroup{.FirstPageNo = firstPageNo};
    for (TStringBuf page: pages) {
        pageGroup.Content.emplace_back(page.data(), page.size());
    }

    TVector<TPageGroup> pageGroups;
    pageGroups.push_back(std::move(pageGroup));
    return group.WriteLogRecord(
        NoHeaders,
        std::move(pageGroups),
        {.Lsn = lsn, .PrevLsn = lsn ? lsn - 1 : 0});
}

NProto::TError Write(IStorageGroup& group, ui64 lsn = Lsn)
{
    return WritePages(group, lsn, PageNo, {"page1"});
}

NProto::TError ReadRange(
    IStorageGroup& group,
    ui64 firstPageNo,
    ui64 pageCount,
    TVector<TPageGroup>* pageGroups)
{
    TVector<TPageGroupRef> refs = {{
        .FirstPageNo = firstPageNo,
        .PageCount = pageCount,
    }};

    return group.ReadPages(NoHeaders, refs, pageGroups);
}

NProto::TError Read(IStorageGroup& group, TVector<TPageGroup>* pageGroups)
{
    return ReadRange(group, PageNo, 1, pageGroups);
}

// The page as a string, or what went wrong instead, so that comparing it to
// the expected page says both.
TString ReadOnePage(IStorageGroup& group)
{
    TVector<TPageGroup> pageGroups;
    auto error = Read(group, &pageGroups);
    if (HasError(error)) {
        return TStringBuilder() << "<" << FormatError(error) << ">";
    }
    if (pageGroups.size() != 1 || pageGroups[0].Content.size() != 1) {
        return TStringBuilder()
            << "<" << pageGroups.size() << " page groups>";
    }

    const auto& page = pageGroups[0].Content[0];
    return TString(page.Data(), page.Size());
}

struct TWriteParams
{
    TGroupFixture* Fixture;
    ui64 Lsn;
};

int WriteFiberMain(TWriteParams* params) noexcept
{
    return HasError(Write(*params->Fixture->Group, params->Lsn)) ? 1 : 0;
}

// A write on its own fiber, so a test can have several in flight.
void StartWrite(TGroupFixture& fx, ui64 lsn, silk::FiberFuture* future)
{
    const int r = FiberScheduler::run(
        WriteFiberMain,
        TWriteParams{.Fixture = &fx, .Lsn = lsn},
        future);
    Y_ABORT_UNLESS(
        r == 0,
        "failed to spawn a writer: %s",
        SystemErrorText(r).c_str());
}

////////////////////////////////////////////////////////////////////////////////
// Waiting and inspecting.

// Yields until the predicate holds, so a test can observe a detached fiber's
// side effect without sleeping. False if it never held: assert on it, or the
// test carries on with its precondition unmet.
template <typename TPredicate>
[[nodiscard]] bool WaitFor(TPredicate predicate)
{
    for (ui32 i = 0; i < 100000 && !predicate(); ++i) {
        FiberScheduler::yield();
    }
    return predicate();
}

// Naive check if fut is still unset after letting fibers do the active round.
bool StillRunning(silk::FiberFuture& future)
{
    FiberScheduler::yield();
    int error = 0;
    return !future.isSet(&error);
}

ui32 TotalWrites(TGroupFixture& fx)
{
    ui32 total = 0;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        total += fx[i].Writes.Count();
    }
    return total;
}

TVector<ui32> WriteCounts(TGroupFixture& fx)
{
    TVector<ui32> counts;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        counts.push_back(fx[i].Writes.Count());
    }
    return counts;
}

TVector<ui32> ReadCounts(TGroupFixture& fx)
{
    TVector<ui32> counts;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        counts.push_back(fx[i].Reads.Count());
    }
    return counts;
}

// The sleeps the group asked for, in microseconds so a mismatch reads well.
TVector<ui64> Sleeps(TGroupFixture& fx)
{
    TVector<ui64> sleeps;
    for (TDuration sleep: fx.TestTimer().GetSleepDurations()) {
        sleeps.push_back(sleep.MicroSeconds());
    }
    return sleeps;
}

// The sleeps a request that fails @p count times in a row asks for: the k-th
// backoff is k increments.
TVector<ui64> Backoffs(ui32 count)
{
    const TDuration increment = MakeConfig().RetryPolicy.BackoffIncrement;
    TVector<ui64> backoffs;
    for (ui32 k = 1; k <= count; ++k) {
        backoffs.push_back((increment * k).MicroSeconds());
    }
    return backoffs;
}

// The page the device was last written at @p pageNo.
TString PageWritten(TFakeDevice& device, ui64 pageNo)
{
    TString page;
    for (const auto& write: device.Writes.Requests) {
        for (const auto& pg: write.GetPageGroups()) {
            const ui64 first = pg.GetFirstPageNo();
            if (pageNo >= first && pageNo < first + pg.ContentSize()) {
                page = pg.GetContent(pageNo - first);
            }
        }
    }
    return page;
}

// The header pages a quorum group writes to its devices, by device number.
const TVector<TString>& HeaderPages()
{
    static const TVector<TString> pages = [] {
        TQuorumFixture fx;
        TVector<TString> pages;
        for (ui32 i = 0; i < DeviceCount; ++i) {
            pages.push_back(PageWritten(fx[i], 0));
        }
        return pages;
    }();
    return pages;
}

// Forgets the read log and reads once per device. With every replica eligible
// the rotation lands each read on a different one, so a read count of one
// everywhere is what "all of them serve" looks like.
NProto::TError ReadFromEachReplica(TGroupFixture& fx)
{
    for (auto& device: fx.Nodes) {
        device->Reads.Requests.clear();
    }

    NProto::TError first;
    for (ui32 i = 0; i < fx.Size(); ++i) {
        TVector<TPageGroup> pageGroups;
        auto error = Read(*fx.Group, &pageGroups);
        if (HasError(error) && !HasError(first)) {
            first = error;
        }
    }
    return first;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////
// MirrorGroup

FIBER_TEST(NaiveGroupTest, ShouldInitializeDevices)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].Acquires.Requests.size());
        ASSERT_EQ(1U, fx[i].Acquires.Requests[0].DeviceUUIDsSize());
        ASSERT_EQ(
            fx.DeviceUUIDs[i],
            fx[i].Acquires.Requests[0].GetDeviceUUIDs(0));
    }

    fx.Group->TearDown();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].ReleaseCalls.size());
        ASSERT_EQ(1U, fx[i].ReleaseCalls[0].DeviceUUIDsSize());
        ASSERT_EQ(fx.DeviceUUIDs[i], fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
    }
}

FIBER_TEST(NaiveGroupTest, ShouldMirrorWrites)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    auto error = WritePages(*fx.Group, 1, PageNo, {"page1", "page2"});
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].Writes.Count());
        const auto& write = fx[i].Writes.Requests[0];
        ASSERT_EQ(1U, write.GetLogSequenceNumber());
        // The naive group keeps nothing of its own on the device, so the
        // caller's page numbers are the device's page numbers.
        ASSERT_EQ(PageNo, write.GetPageGroups(0).GetFirstPageNo());
        ASSERT_EQ("page1", write.GetPageGroups(0).GetContent(0));
        ASSERT_EQ("page2", write.GetPageGroups(0).GetContent(1));
        ASSERT_EQ(fx.DeviceUUIDs[i], write.GetDeviceUUID());
        ASSERT_EQ("test-client", write.GetHeaders().GetClientId());
    }
}

FIBER_TEST(NaiveGroupTest, ShouldInitFromTheHighestAckedLsn)
{
    TNaiveFixture fx;

    // The devices come back from a crash at different positions.
    const ui64 acked[] = {3, 7, 5};
    for (ui32 i = 0; i < DeviceCount; ++i) {
        fx[i].ReadJournalTailResp.SetLsnLowWatermark(acked[i]);
    }

    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();
    ASSERT_EQ(7U, init.GetResult());

    // The caller continues from the furthest one, and the link it builds is
    // what reaches the devices.
    auto error = Write(*fx.Group, 8);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    for (ui32 i = 0; i < DeviceCount; ++i) {
        const auto& write = fx[i].Writes.Requests.back();
        ASSERT_EQ(8U, write.GetLogSequenceNumber());
        ASSERT_EQ(7U, write.GetPrevLogSequenceNumber());
    }
}

FIBER_TEST(NaiveGroupTest, ShouldRoundRobinReads)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    // Devices that disagree, so every answer names the one that gave it.
    for (ui32 i = 0; i < DeviceCount; ++i) {
        fx[i].Reads.Response =
            PageResponse(PageNo, TStringBuilder() << "dev" << i);
    }

    for (ui32 round = 0; round < 2; ++round) {
        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_EQ(
                TString(TStringBuilder() << "dev" << i),
                ReadOnePage(*fx.Group))
                << "round " << round;
        }
    }

    for (ui32 i = 0; i < DeviceCount; ++i) {
        ASSERT_EQ(2U, fx[i].Reads.Requests.size());
    }
}

FIBER_TEST(NaiveGroupTest, ShouldRetryErrors)
{
    TNaiveFixture fx;
    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();

    auto& flaky = fx[1];
    flaky.Writes.ResponseQueue.emplace_back();
    *flaky.Writes.ResponseQueue.back().MutableError() =
        MakeError(E_REJECTED, "busy");

    flaky.Writes.ResponseQueue.emplace_back();
    *flaky.Writes.ResponseQueue.back().MutableError() =
        MakeError(E_TIMEOUT, "slow");

    auto error = Write(*fx.Group, 1);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    ASSERT_EQ((TVector<ui32>{1, 3, 1}), WriteCounts(fx));
    ASSERT_EQ(Backoffs(2), Sleeps(fx));

    fx[1].Reads.Response = PageResponse(PageNo, "payload");
    fx[0].Reads.ResponseQueue.emplace_back();
    *fx[0].Reads.ResponseQueue.back().MutableError() =
        MakeError(E_REJECTED, "busy");

    ASSERT_EQ("payload", ReadOnePage(*fx.Group));
    ASSERT_EQ((TVector<ui32>{1, 1, 0}), ReadCounts(fx));

    // The read starts a backoff sequence of its own; it does not continue
    // the write's.
    auto sleeps = Backoffs(2);
    sleeps.push_back(Backoffs(1)[0]);
    ASSERT_EQ(sleeps, Sleeps(fx));
}

FIBER_TEST(NaiveGroupTest, ShouldNotRetryErrors)
{
    {
        TNaiveFixture fx;
        const auto init = fx.Group->Init();
        ASSERT_EQ(S_OK, init.GetError().GetCode())
            << init.GetError().GetMessage();

        *fx[1].Writes.Response.MutableError() =
            MakeError(E_ARGUMENT, "bad record");
        auto error = Write(*fx.Group, 1);
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        ASSERT_EQ(TVector<ui32>(DeviceCount, 1), WriteCounts(fx));

        *fx[0].Reads.Response.MutableError() =
            MakeError(E_ARGUMENT, "bad range");
        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(pageGroups.empty());

        // One attempt, and no failover either: a device that rejects the
        // request outright means the request is wrong, not the device.
        ASSERT_EQ((TVector<ui32>{1, 0, 0}), ReadCounts(fx));

        ASSERT_TRUE(fx.TestTimer().GetSleepDurations().empty());
    }

    // The retries stop at the deadline.
    // A one second budget over the default half second increment. The test
    // timer advances by every sleep, so a dead device fails at 0s, 0.5s and
    // 1.5s: the third error is past the budget and ends the attempts.
    auto config = MakeConfig();
    config.RetryPolicy.TotalTimeout = TDuration::Seconds(1);
    constexpr ui32 attempts = 3;

    {
        TNaiveFixture fx(config);
        const auto init = fx.Group->Init();
        ASSERT_EQ(S_OK, init.GetError().GetCode())
            << init.GetError().GetMessage();
        *fx[1].Writes.Response.MutableError() =
            MakeError(E_REJECTED, "busy");

        auto error = Write(*fx.Group, 1);
        ASSERT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();

        ASSERT_EQ((TVector<ui32>{1, attempts, 1}), WriteCounts(fx));
        ASSERT_EQ(Backoffs(attempts - 1), Sleeps(fx));
    }

    {
        TNaiveFixture fx(config);
        const auto init = fx.Group->Init();
        ASSERT_EQ(S_OK, init.GetError().GetCode())
            << init.GetError().GetMessage();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            *fx[i].Reads.Response.MutableError() =
                MakeError(E_UNAVAILABLE, "down");
        }

        TVector<TPageGroup> pageGroups;
        auto error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(E_UNAVAILABLE, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(pageGroups.empty());

        // The rotation keeps moving while retrying, so each device gets
        // one.
        ASSERT_EQ(TVector<ui32>(DeviceCount, 1), ReadCounts(fx));
        ASSERT_EQ(Backoffs(attempts - 1), Sleeps(fx));
    }
}

////////////////////////////////////////////////////////////////////////////////
// QuorumGroup

FIBER_TEST(QuorumGroupTest, ShouldInit)
{
    // No errors: every device is acquired, a blank one gets the header page as
    // record 1, and TearDown releases every device.
    {
        TQuorumFixture fx(false /* init */);
        const auto init = fx.Group->Init();
        ASSERT_EQ(S_OK, init.GetError().GetCode())
            << init.GetError().GetMessage();

        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_EQ(1U, fx[i].Acquires.Requests.size());

            const auto& acquire = fx[i].Acquires.Requests[0];
            ASSERT_EQ(1U, acquire.DeviceUUIDsSize());
            ASSERT_EQ(42U, acquire.GetGeneration());
            ASSERT_EQ("test-client", acquire.GetHeaders().GetClientId());
            ASSERT_EQ(fx.DeviceUUIDs[i], acquire.GetDeviceUUIDs(0));

            ASSERT_EQ(1U, fx[i].Writes.Count());

            const auto& headerWrite = fx[i].Writes.Requests[0];
            ASSERT_EQ(1U, headerWrite.GetLogSequenceNumber());
            ASSERT_EQ(0U, headerWrite.GetPageGroups(0).GetFirstPageNo());
            ASSERT_EQ(DefaultBlockSize, PageWritten(fx[i], 0).size());
        }

        fx.Group->TearDown();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_EQ(1U, fx[i].ReleaseCalls.size());
            ASSERT_EQ(
                fx.DeviceUUIDs[i],
                fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
        }
    }
}

FIBER_TEST(QuorumGroupTest, ShouldNotRewriteExistingHeaderOnInit)
{
    TQuorumFixture fx(false /* init */);
    for (ui32 i = 0; i < DeviceCount; ++i) {
        fx[i].Reads.Response = PageResponse(0, HeaderPages()[i]);
        fx[i].Reads.Response.SetLastAckedLogSequenceNumber(Lsn);
    }

    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();
    ASSERT_EQ(Lsn, init.GetResult());
    ASSERT_EQ(TVector<ui32>(fx.Size(), 0), WriteCounts(fx));
}

FIBER_TEST(QuorumGroupTest, ShouldFailInitOnErrors)
{
    // The acquire is refused.
    {
        TQuorumFixture fx(false /* init */);
        *fx[1].Acquires.Response.MutableError() =
            MakeError(E_ARGUMENT, "no session");

        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    }

    // The header page cannot be read.
    {
        TQuorumFixture fx(false /* init */);
        *fx[0].Reads.Response.MutableError() = MakeError(E_IO, "media error");

        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(E_IO, error.GetCode()) << error.GetMessage();
        ASSERT_EQ(0U, fx[0].Writes.Count())
            << "header written despite the error";
    }

    // The header page cannot be written.
    {
        TQuorumFixture fx(false /* init */);
        fx[1].Writes.ResponseQueue.emplace_back();
        *fx[1].Writes.ResponseQueue.back().MutableError() =
            MakeError(E_ARGUMENT, "read only");

        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
        ASSERT_EQ(1U, fx[1].Writes.Count());
    }

    // The journal tail cannot be read.
    {
        TQuorumFixture fx(false /* init */);
        auto response = PageResponse(0, HeaderPages()[0]);
        response.SetLastAckedLogSequenceNumber(4);
        fx[0].Reads.ResponseQueue.push_back(response);
        *fx[0].ReadJournalTailResp.MutableError() =
            MakeError(E_ARGUMENT, "no journal");

        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

        // An uninitialised group serves nobody.
        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(E_REJECTED, error.GetCode()) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, ShouldAllowTearDownBeforeInit)
{
    // TearDown before Init releases the devices and returns.
    {
        TQuorumFixture fx(false /* init */);
        fx.Group->TearDown();

        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_EQ(1U, fx[i].ReleaseCalls.size());
            ASSERT_EQ(
                fx.DeviceUUIDs[i],
                fx[i].ReleaseCalls[0].GetDeviceUUIDs(0));
        }
    }
}

FIBER_TEST(QuorumGroupTest, ShouldValidateGroupHeader)
{
    // The header page is short.
    {
        TQuorumFixture fx(false /* init */);
        fx[2].Reads.Response = PageResponse(0, TString(1, 'x'));

        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_EQ(0U, fx[2].Writes.Count())
            << "header written despite the error";
    }

    const TString headerPage = HeaderPages()[0];

    // A device written up as dev-a, offered to a group that calls it dev-b.
    {
        TQuorumFixture other(false /* init */);
        other[1].Reads.Response = PageResponse(0, headerPage);

        auto error = other.Group->Init().GetError();
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_EQ(0U, other[1].Writes.Count());
    }

    // A header page this build cannot understand.
    {
        const TString good = HeaderPages()[2];
        ASSERT_GE(good.size(), sizeof(TStorageGroupHeader));

        for (TStringBuf field:
             {"magic", "version", "group type", "page size", "device"})
        {
            TStorageGroupHeader header;
            memcpy(&header, good.data(), sizeof(header));
            if (field == "magic") {
                header.MagicNumber = header.MagicNumber + 1;
            } else if (field == "version") {
                header.Version = TStorageGroupHeader::CurrentVersion + 1;
            } else if (field == "group type") {
                header.GroupType = header.GroupType + 1;
            } else if (field == "page size") {
                header.PageSize = header.PageSize * 2;
            } else {
                header.DeviceUUIDHash = header.DeviceUUIDHash + 1;
            }

            TString headerPage = good;
            memcpy(headerPage.begin(), &header, sizeof(header));

            TQuorumFixture fx(false /* init */);
            fx[2].Reads.Response = PageResponse(0, headerPage);

            auto error = fx.Group->Init().GetError();
            ASSERT_EQ(E_INVALID_STATE, error.GetCode())
                << field << ": " << error.GetMessage();
            ASSERT_EQ(0U, fx[2].Writes.Count())
                << field << ": header written anyway";
        }
    }
}

FIBER_TEST(QuorumGroupTest, ShouldReplayTheJournalTailOnInit)
{
    TQuorumFixture fx(false /* init */);
    auto response = PageResponse(0, HeaderPages()[0]);
    response.SetLastAckedLogSequenceNumber(4);
    fx[0].Reads.ResponseQueue.push_back(response);

    for (ui64 lsn = 2; lsn <= 4; ++lsn) {
        auto* record = fx[0].ReadJournalTailResp.AddRecords();
        record->SetLogSequenceNumber(lsn);
        record->SetPrevLogSequenceNumber(lsn - 1);
        auto* pg = record->AddPageGroups();
        pg->SetFirstPageNo(100 + lsn);
        pg->AddContent(TStringBuilder() << "record" << lsn);
    }

    const auto init = fx.Group->Init();
    ASSERT_EQ(S_OK, init.GetError().GetCode())
        << init.GetError().GetMessage();
    ASSERT_EQ(4U, init.GetResult());

    // The devices behind take the header page and then the three records,
    // each as the device that kept it holds it, and every device serves once
    // it is level.
    ASSERT_EQ((TVector<ui32>{0, 4, 4}), WriteCounts(fx));
    for (ui32 i = 1; i < DeviceCount; ++i) {
        ASSERT_EQ(1U, fx[i].Writes.Requests.at(0).GetLogSequenceNumber());
        for (ui64 lsn = 2; lsn <= 4; ++lsn) {
            const auto& write = fx[i].Writes.Requests.at(lsn - 1);
            ASSERT_EQ(lsn, write.GetLogSequenceNumber());
            ASSERT_EQ(lsn - 1, write.GetPrevLogSequenceNumber());
            ASSERT_EQ(100 + lsn, write.GetPageGroups(0).GetFirstPageNo());
            ASSERT_EQ(
                TString(TStringBuilder() << "record" << lsn),
                write.GetPageGroups(0).GetContent(0));
        }
    }

    ASSERT_EQ(S_OK, ReadFromEachReplica(fx).GetCode());
    ASSERT_EQ(TVector<ui32>(fx.Size(), 1), ReadCounts(fx));
}

FIBER_TEST(QuorumGroupTest, ShouldFailIfReplayImpossible)
{
    // TODO(#6957): a tail that does not continue from where a device is, e.g.
    // devices at 10, 15, 15 and a tail after 10 that starts at 13, is replayed
    // as it comes; ReadRecordsAbove should check the chain and fail Init.

    const TString headerPage = HeaderPages()[0];

    // empty read tail response
    {
        TQuorumFixture fx(false /* init */);
        auto response = PageResponse(0, headerPage);
        response.SetLastAckedLogSequenceNumber(Lsn);
        fx[0].Reads.ResponseQueue.push_back(response);

        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
    }

    // A device behind takes the header page but fails write.
    {
        TQuorumFixture fx(false /* init */);
        auto response = PageResponse(0, headerPage);
        response.SetLastAckedLogSequenceNumber(4);
        fx[0].Reads.ResponseQueue.push_back(response);
        for (ui64 lsn = 2; lsn <= 4; ++lsn) {
            auto* record = fx[0].ReadJournalTailResp.AddRecords();
            record->SetLogSequenceNumber(lsn);
            record->SetPrevLogSequenceNumber(lsn - 1);
        }

        fx[2].Writes.ResponseQueue.emplace_back();
        *fx[2].Writes.Response.MutableError() =
            MakeError(E_ARGUMENT, "read only");

        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, ShouldAckWritesOnMajority)
{
    // Two of three ack the write; the laggard stays out of reads until it
    // catches up.
    {
        TQuorumFixture fx;
        fx.ForgetRequests();

        fx[2].Writes.Paused = true;
        TPageGroup pageGroup{.FirstPageNo = PageNo};
        pageGroup.Content.emplace_back("fresh", 5U /* len */);
        TVector<TPageGroup> record;
        record.push_back(std::move(pageGroup));
        auto error = fx.Group->WriteLogRecord(
            NoHeaders,
            std::move(record),
            {.Lsn = Lsn + 1, .PrevLsn = Lsn - 1});
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

        // Two acks are enough to return; the third record is still in flight.
        ASSERT_EQ((TVector<ui32>{1, 1, 0}), WriteCounts(fx));

        // The record goes out with the link the caller built, even one that
        // skips a number.
        for (ui32 i = 0; i < 2; ++i) {
            const auto& write = fx[i].Writes.Requests.back();
            ASSERT_EQ(Lsn + 1, write.GetLogSequenceNumber());
            ASSERT_EQ(Lsn - 1, write.GetPrevLogSequenceNumber());
        }

        // Once the laggard catches up, every replica serves.
        fx[2].Writes.Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[2].Writes.Count() == 1; }));
        ASSERT_EQ(S_OK, ReadFromEachReplica(fx).GetCode());
        ASSERT_EQ(TVector<ui32>(fx.Size(), 1), ReadCounts(fx));
    }

    // A majority is more than half of the devices.
    // Two devices leave no room for a straggler: the majority is both.
    {
        TQuorumFixture fx(2U /* devices */);
        fx[1].Writes.Paused = true;

        silk::FiberFuture write;
        StartWrite(fx, Lsn, &write);
        ASSERT_TRUE(StillRunning(write))
            << "acked without the second device";

        fx[1].Writes.Unpause();
        ASSERT_EQ(0, write.wait());
    }

    // Four tolerate one straggler.
    {
        TQuorumFixture fx(4U /* devices */);
        fx[3].Writes.Paused = true;

        auto error = Write(*fx.Group);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

        fx[3].Writes.Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[3].Writes.Count() == 2; }));
    }

    // But not two: half the devices is not a majority.
    {
        TQuorumFixture fx(4U /* devices */);
        fx[2].Writes.Paused = true;
        fx[3].Writes.Paused = true;

        silk::FiberFuture write;
        StartWrite(fx, Lsn, &write);
        ASSERT_TRUE(StillRunning(write)) << "acked on half the devices";

        fx[2].Writes.Unpause();
        ASSERT_EQ(0, write.wait());

        fx[3].Writes.Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[3].Writes.Count() == 2; }));
    }
}

FIBER_TEST(QuorumGroupTest, ShouldReadFromReplicasAtTheAckedLsn)
{
    TQuorumFixture fx;
    fx.ForgetRequests();

    // A retriable error is the same replica's problem to fix: it is retried
    // there rather than handed to the next one.
    fx[0].Reads.ResponseQueue.emplace_back();
    *fx[0].Reads.ResponseQueue.back().MutableError() =
        MakeError(E_REJECTED, "busy");

    TVector<TPageGroup> pageGroups;
    auto error = Read(*fx.Group, &pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ((TVector<ui32>{2, 0, 0}), ReadCounts(fx));
    ASSERT_EQ(Backoffs(1), Sleeps(fx));

    for (auto& device: fx.Nodes) {
        device->Reads.Requests.clear();
    }

    // A replica that is behind is not a fallback: answering with the page as
    // it was before the last record is worse than failing the read.
    fx[2].Writes.Paused = true;
    error = Write(*fx.Group);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    *fx[0].Reads.Response.MutableError() = MakeError(E_ARGUMENT, "bad range");
    *fx[1].Reads.Response.MutableError() = MakeError(E_ARGUMENT, "bad range");

    error = Read(*fx.Group, &pageGroups);
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(0U, fx[2].Reads.Requests.size());
    ASSERT_GT(fx[0].Reads.Requests.size(), 0U);
    ASSERT_GT(fx[1].Reads.Requests.size(), 0U);

    // A failed read is not a failed device, so the group stays usable.
    error = Write(*fx.Group, Lsn + 1);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    fx[2].Writes.Unpause();
    ASSERT_TRUE(WaitFor([&] { return fx[2].Writes.Count() == 2; }));
}

FIBER_TEST(QuorumGroupTest, ShouldBreakOnAnyDeviceWriteFailure)
{
    // A failure that arrives before the majority fails the write itself.
    {
        TQuorumFixture fx;
        fx.ForgetRequests();
        fx[1].Writes.Paused = true;
        *fx[2].Writes.Response.MutableError() =
            MakeError(E_ARGUMENT, "read only");

        auto error = Write(*fx.Group);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

        // And the group stays broken for everything that follows.
        error = Write(*fx.Group, Lsn + 1);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

        TVector<TPageGroup> pageGroups;
        error = Read(*fx.Group, &pageGroups);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();

        fx[1].Writes.Unpause();
        ASSERT_TRUE(WaitFor([&] { return fx[1].Writes.Count() == 1; }));
    }

    // A failure that arrives after it does not un-ack the caller, but still
    // breaks the group: the devices have diverged.
    {
        TQuorumFixture fx;
        fx[2].Writes.Paused = true;
        *fx[2].Writes.Response.MutableError() =
            MakeError(E_ARGUMENT, "read only");

        auto error = Write(*fx.Group);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

        fx[2].Writes.Unpause();
        ASSERT_TRUE(WaitFor(
            [&]
            {
                TVector<TPageGroup> pageGroups;
                return HasError(Read(*fx.Group, &pageGroups));
            }));

        error = Write(*fx.Group, Lsn + 1);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
    }
}

FIBER_TEST(QuorumGroupTest, ShouldHandleInvalidPageRanges)
{
    TQuorumFixture fx;
    fx.ForgetRequests();

    constexpr ui64 reserved = TStorageGroupHeader::StorageGroupReservedPages;

    // cannot technically write all the pages in range
    auto error = WritePages(*fx.Group, Lsn, Max<ui64>(), {"page1"});
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

    TVector<TPageGroup> pageGroups;
    error = ReadRange(*fx.Group, Max<ui64>(), 1, &pageGroups);
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    error = ReadRange(*fx.Group, Max<ui64>() - reserved, 1, &pageGroups);
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();
    error = ReadRange(*fx.Group, Max<ui64>() - reserved - 1, 2, &pageGroups);
    ASSERT_EQ(E_ARGUMENT, error.GetCode()) << error.GetMessage();

    // The last page that fits comes back under the caller's number.
    error = ReadRange(*fx.Group, Max<ui64>() - reserved - 1, 1, &pageGroups);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
    ASSERT_EQ(1U, pageGroups.size());
    ASSERT_EQ(Max<ui64>() - reserved - 1, pageGroups[0].FirstPageNo);

    // A device answering with a page from below the caller's would come back
    // as a number that does not exist, so it is dropped.
    for (auto& device: fx.Nodes) {
        device->Reads.Response = PageResponse(0, "the header");
    }

    error = ReadRange(*fx.Group, 0, 1, &pageGroups);
    ASSERT_EQ(E_FAIL, error.GetCode()) << error.GetMessage();
    ASSERT_TRUE(pageGroups.empty());
}

FIBER_TEST(QuorumGroupTest, ShouldPushLowWatermark)
{
    // The watermark follows the slowest device, one push per move.
    {
        auto config = MakeConfig();
        config.LowWatermarkPeriod = TDuration::MilliSeconds(1);
        TQuorumFixture fx(true, config);

        // Everything every device already holds may be trimmed from the start.
        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_TRUE(WaitFor([&] { return fx[i].Watermarks.Count() == 1; }));
            ASSERT_EQ(1U, fx[i].Watermarks.Last().GetLsnLowWatermark());
        }

        fx[2].Writes.Paused = true;
        auto error = Write(*fx.Group);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
        FiberScheduler::yield();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_EQ(1U, fx[i].Watermarks.Count());
        }

        fx[2].Writes.Unpause();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_TRUE(WaitFor([&] { return fx[i].Watermarks.Count() == 2; }));
            ASSERT_EQ(Lsn, fx[i].Watermarks.Last().GetLsnLowWatermark());
        }

        // One push per move: the next record brings the third and no other.
        error = Write(*fx.Group, Lsn + 5);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
        for (ui32 i = 0; i < DeviceCount; ++i) {
            ASSERT_TRUE(WaitFor([&] { return fx[i].Watermarks.Count() == 3; }));
            ASSERT_EQ(Lsn + 5, fx[i].Watermarks.Last().GetLsnLowWatermark());
        }
    }

    // A watermark a device missed is superseded by the next one, not retried.
    {
        auto config = MakeConfig();
        config.LowWatermarkPeriod = TDuration::MilliSeconds(1);
        config.RetryPolicy.TotalTimeout = TDuration::Zero();   // one request

        TQuorumFixture fx(false /* init */, config);
        *fx[2].Watermarks.Response.MutableError() =
            MakeError(E_REJECTED, "busy");

        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(WaitFor([&] { return fx[2].Watermarks.Count() == 1; }));

        // The refusal was retriable, so the group does not break, and the
        // missed watermark is not resent on its own.
        fx[2].Watermarks.Reply({});
        FiberScheduler::yield();
        ASSERT_EQ(1U, fx[2].Watermarks.Count());

        // The next one carries the device forward anyway.
        error = Write(*fx.Group);
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();
        ASSERT_TRUE(WaitFor([&] { return fx[2].Watermarks.Count() == 2; }));
        ASSERT_EQ(Lsn, fx[2].Watermarks.Last().GetLsnLowWatermark());
    }

    // A watermark non-retriable error stops the group so far.
    {
        auto config = MakeConfig();
        config.LowWatermarkPeriod = TDuration::MicroSeconds(1);
        TQuorumFixture fx(false /* init */, config);
        *fx[2].Watermarks.Response.MutableError() =
            MakeError(E_ARGUMENT, "unknown device");
        auto error = fx.Group->Init().GetError();
        ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

        TVector<TPageGroup> pageGroups;
        ASSERT_TRUE(
            WaitFor([&] { return HasError(Read(*fx.Group, &pageGroups)); }));
        const ui32 writes = TotalWrites(fx);
        error = Write(*fx.Group);
        ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
        ASSERT_EQ(writes, TotalWrites(fx));
    }
}

FIBER_TEST(QuorumGroupTest, ShouldRenewSessions)
{
    auto config = MakeConfig();
    config.ReacquirePeriod = TDuration::MicroSeconds(1);
    config.RetryPolicy.TotalTimeout = TDuration::Zero();   // one request
    TQuorumFixture fx(false /* init */, config);

    // Every acquire is held at the devices and let through by the test: the
    // one of Init at once, then a renewal round at a time.
    for (ui32 i = 0; i < DeviceCount; ++i) {
        fx[i].Acquires.Paused = true;
        fx[i].Acquires.Step();
    }
    auto error = fx.Group->Init().GetError();
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    // Init's acquire is the one before step 1.
    auto reacquireStep = [&](ui32 round)
    {
        for (ui32 i = 0; i < DeviceCount; ++i) {
            if (!WaitFor([&] { return fx[i].Acquires.Parked == round + 1; })) {
                return false;
            }
            fx[i].Acquires.Step();
        }
        for (ui32 i = 0; i < DeviceCount; ++i) {
            if (!WaitFor([&] { return fx[i].Acquires.Count() == round + 1; })) {
                return false;
            }
        }
        return true;
    };

    // A storage node lets go of a session that stops being renewed, so Init's
    // acquire is repeated on every device for as long as the group lives.
    ASSERT_TRUE(reacquireStep(1));
    for (ui32 i = 0; i < DeviceCount; ++i) {
        const auto renewal = fx[i].Acquires.Last();
        ASSERT_EQ(1U, renewal.DeviceUUIDsSize());
        ASSERT_EQ(fx.DeviceUUIDs[i], renewal.GetDeviceUUIDs(0));
        ASSERT_EQ(42U, renewal.GetGeneration());
        ASSERT_EQ("test-client", renewal.GetHeaders().GetClientId());
    }

    // A retriable or aborted refusal is no verdict on the session: the group
    // carries on, and the next round is the retry.
    fx[2].Acquires.Reply(MakeError(E_REJECTED, "busy"));
    ASSERT_TRUE(reacquireStep(2));
    error = Write(*fx.Group);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    fx[2].Acquires.Reply(MakeError(E_ABORTED, "shutting down"));
    ASSERT_TRUE(reacquireStep(3));
    error = Write(*fx.Group, Lsn + 1);
    ASSERT_EQ(S_OK, error.GetCode()) << error.GetMessage();

    // The device belongs to someone else now, which is not a device this
    // group can go on writing to.
    fx[2].Acquires.Reply(MakeError(E_BS_MOUNT_CONFLICT, "another writer"));
    ASSERT_TRUE(reacquireStep(4));
    TVector<TPageGroup> pageGroups;
    ASSERT_TRUE(
        WaitFor([&] { return HasError(Read(*fx.Group, &pageGroups)); }));
    error = Write(*fx.Group, Lsn + 2);
    ASSERT_EQ(E_INVALID_STATE, error.GetCode()) << error.GetMessage();
}
